// ruggine_server/src/main.rs
mod http;
mod state;

use anyhow::anyhow;
use chrono::Utc;
// NEW: Aggiunto per i timestamp
use futures_util::stream::{SplitSink, SplitStream, StreamExt};
use futures_util::SinkExt;
use ruggine_common::{ClientMessage, ServerMessage};
use rusqlite::{params, Connection, Result as DbResult};
// rust
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use sysinfo::System;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tracing::{error, info, warn};

use state::{AppState, ClientTx, SharedState};

/// Internal server state wrapper (uses SharedState internally)
#[derive(Clone)]
struct ServerState {
    inner: SharedState,
}

impl ServerState {
    fn new(state: SharedState) -> Self {
        Self { inner: state }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let file_appender = tracing_appender::rolling::daily("logs", "ruggine_server.log");
    let (non_blocking_appender, _guard) = tracing_appender::non_blocking(file_appender);
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_writer(non_blocking_appender)
        .init();

    info!("Avvio server Ruggine...");

    let conn = Connection::open("ruggine.db")?;
    init_db(&conn)?;

    // Create shared state
    let shared_state: SharedState = Arc::new(AppState::new(conn));

    // Wrap for TCP/WebSocket handlers
    let state = ServerState::new(shared_state.clone());

    tokio::spawn(async move {
        cpu_logging_task().await;
    });

    // Start HTTP API server on port 3000
    let http_state = shared_state.clone();
    tokio::spawn(async move {
        let app = http::build_router(http_state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await.unwrap();
        info!("Server HTTP Axum in ascolto su http://127.0.0.1:3000");
        axum::serve(listener, app).await.unwrap();
    });

    // Start WebSocket server on port 4000
    let addr = "127.0.0.1:4000";
    let listener = TcpListener::bind(addr).await?;
    info!("Server WebSocket in ascolto su {}", addr);

    while let Ok((stream, addr)) = listener.accept().await {
        let state = state.clone();
        tokio::spawn(handle_connection(stream, addr, state));
    }

    Ok(())
}

fn init_db(conn: &Connection) -> DbResult<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            username TEXT UNIQUE NOT NULL
        );
        CREATE TABLE IF NOT EXISTS groups (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS group_members (
            user_id INTEGER NOT NULL,
            group_id INTEGER NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users(id),
            FOREIGN KEY(group_id) REFERENCES groups(id),
            PRIMARY KEY(user_id, group_id)
        );
        CREATE TABLE IF NOT EXISTS group_invites (
            invited_user_id INTEGER NOT NULL,
            group_id INTEGER NOT NULL,
            inviter_user_id INTEGER NOT NULL,
            FOREIGN KEY(invited_user_id) REFERENCES users(id),
            FOREIGN KEY(group_id) REFERENCES groups(id),
            FOREIGN KEY(inviter_user_id) REFERENCES users(id),
            PRIMARY KEY(invited_user_id, group_id)
        );
        ",
    )?;
    info!("Database inizializzato.");
    Ok(())
}

async fn handle_connection(stream: tokio::net::TcpStream, addr: SocketAddr, state: ServerState) {
    info!("Nuova connessione da: {}", addr);
    let ws_stream = match accept_async(stream).await {
        Ok(ws) => ws,
        Err(e) => {
            error!("Errore handshake WebSocket: {}", e);
            return;
        }
    };

    let (ws_tx, ws_rx) = ws_stream.split();

    let (client_tx, client_rx) = mpsc::unbounded_channel::<ServerMessage>();

    tokio::spawn(forward_to_websocket(ws_tx, client_rx));

    let mut current_user: Option<String> = None;

    // Passiamo client_tx al processore di messaggi per la registrazione
    process_incoming_messages(ws_rx, addr, state.clone(), &mut current_user, client_tx).await;

    info!("Connessione chiusa: {}", addr);
    if let Some(username) = current_user {
        info!("Utente {} disconnesso.", username);
        if let Ok(mut active_users) = state.inner.active_users.lock() {
            active_users.remove(&username);
        }
    }
}

async fn process_incoming_messages(
    mut ws_rx: SplitStream<tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>>,
    addr: SocketAddr,
    state: ServerState,
    current_user: &mut Option<String>,
    client_tx: ClientTx,
) {
    while let Some(Ok(msg)) = ws_rx.next().await {
        match msg {
            WsMessage::Text(text) => {
                match serde_json::from_str::<ClientMessage>(&text) {
                    Ok(client_msg) => {
                        let response = handle_client_message(
                            client_msg,
                            state.clone(),
                            current_user,
                            client_tx.clone(),
                        )
                            .await;

                        // Se handle_client_message fallisce, invia un errore generico
                        if let Err(e) = response {
                            warn!("Errore nell'elaborare messaggio: {}", e);
                            let _ = client_tx
                                .send(ServerMessage::Error { message: e.to_string() });
                        }
                    }
                    Err(e) => {
                        warn!("Ricevuto JSON non valido da {}: {}", addr, e);
                        let _ = client_tx
                            .send(ServerMessage::Error { message: "JSON non valido".into() });
                    }
                }
            }
            WsMessage::Binary(_) => {
                warn!("Ricevuto messaggio binario (non supportato) da {}", addr);
            }
            WsMessage::Close(_) => {
                break;
            }
            WsMessage::Ping(data) => {
                info!("Ricevuto Ping: {:?}", data);
            }
            _ => {}
        }
    }
}

async fn forward_to_websocket(
    mut ws_tx: SplitSink<tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>, WsMessage>,
    mut client_rx: mpsc::UnboundedReceiver<ServerMessage>,
) {
    while let Some(msg) = client_rx.recv().await {
        match serde_json::to_string(&msg) {
            Ok(text) => {
                if let Err(e) = ws_tx.send(WsMessage::Text(text)).await {
                    warn!("Errore invio messaggio WebSocket: {}. Chiudo canale.", e);
                    break;
                }
            }
            Err(e) => {
                error!("Errore serializzazione ServerMessage: {}", e);
            }
        }
    }
}

// NEW: Funzione helper per trasmettere un messaggio a un gruppo
async fn broadcast_to_group(
    state: ServerState,
    group_id: i64,
    message: ServerMessage,
    exclude_username: Option<&str>,
) -> anyhow::Result<()> {
    let db = state.inner.db.lock().map_err(|e| anyhow!("Lock DB error: {}", e))?;
    let active_users = state.inner.active_users.lock().map_err(|e| anyhow!("Lock error: {}", e))?;

    let mut stmt = db.prepare(
        "SELECT u.username FROM users u
         JOIN group_members gm ON u.id = gm.user_id
         WHERE gm.group_id = ?1",
    )?;

    let member_usernames: Vec<String> = stmt
        .query_map(params![group_id], |row| row.get(0))?
        .collect::<Result<Vec<_>, _>>()?;

    for username in member_usernames {
        // Salta l'utente da escludere (se specificato)
        if let Some(exclude) = exclude_username {
            if username == exclude {
                continue;
            }
        }

        // Invia il messaggio solo se l'utente è attualmente connesso
        if let Some(tx) = active_users.get(&username) {
            if let Err(e) = tx.send(message.clone()) {
                warn!("Impossibile inviare messaggio a {}: {}", username, e);
            }
        }
    }

    Ok(())
}

async fn handle_client_message(
    msg: ClientMessage,
    state: ServerState,
    current_user: &mut Option<String>,
    client_tx: ClientTx,
) -> anyhow::Result<()> {
    match msg {
        ClientMessage::Register { username } => {
            if current_user.is_some() {
                client_tx.send(ServerMessage::RegisterResponse {
                    success: false,
                    reason: Some("Sei già registrato.".into()),
                })?;
                return Ok(());
            }

            let db = state.inner.db.lock().map_err(|e| anyhow!("Lock DB error: {}", e))?;

            // FASE 2: User persistence - controlla se l'utente esiste già
            let existing_user: DbResult<i64> = db.query_row(
                "SELECT id FROM users WHERE username = ?1",
                params![&username],
                |row| row.get(0),
            );

            match existing_user {
                Ok(user_id) => {
                    // Utente esistente: login automatico
                    info!("Utente esistente loggato: {} (ID: {})", username, user_id);

                    state
                        .inner.active_users
                        .lock()
                        .map_err(|e| anyhow!("Lock error: {}", e))?
                        .insert(username.clone(), client_tx.clone());
                    *current_user = Some(username);

                    client_tx.send(ServerMessage::RegisterResponse {
                        success: true,
                        reason: Some("Bentornato!".into()),
                    })?;
                }
                Err(_) => {
                    // Nuovo utente: inserisci nel database
                    match db.execute("INSERT INTO users (username) VALUES (?1)", params![&username]) {
                        Ok(_) => {
                            let user_id = db.last_insert_rowid();
                            info!("Nuovo utente registrato: {} (ID: {})", username, user_id);

                            state
                                .inner.active_users
                                .lock()
                                .map_err(|e| anyhow!("Lock error: {}", e))?
                                .insert(username.clone(), client_tx.clone());
                            *current_user = Some(username);

                            client_tx.send(ServerMessage::RegisterResponse {
                                success: true,
                                reason: None,
                            })?;
                        }
                        Err(e) => {
                            warn!("Errore inserimento DB: {}", e);
                            client_tx.send(ServerMessage::RegisterResponse {
                                success: false,
                                reason: Some(format!("Errore DB: {}", e)),
                            })?;
                        }
                    }
                }
            }
        }

        ClientMessage::CreateGroup { name } => {
            let username = current_user
                .as_ref()
                .ok_or_else(|| anyhow!("Registrazione richiesta"))?;

            let mut db = state.inner.db.lock().map_err(|e| anyhow!("Lock DB error: {}", e))?;

            let tx = db.transaction()?;

            let user_id: i64 = tx.query_row(
                "SELECT id FROM users WHERE username = ?1",
                params![username],
                |row| row.get(0),
            )?;

            tx.execute("INSERT INTO groups (name) VALUES (?1)", params![&name])?;
            let group_id = tx.last_insert_rowid();

            tx.execute(
                "INSERT INTO group_members (user_id, group_id) VALUES (?1, ?2)",
                params![user_id, group_id],
            )?;

            tx.commit()?;

            info!(
                "Utente '{}' ha creato il gruppo '{}' (ID: {})",
                username, name, group_id
            );

            client_tx.send(ServerMessage::GroupCreated {
                id: group_id,
                name: name.clone(),
            })?;
        }

        ClientMessage::InviteToGroup {
            username_to_invite,
            group_id,
        } => {
            let inviter_username = current_user
                .as_ref()
                .ok_or_else(|| anyhow!("Registrazione richiesta"))?;

            let db = state.inner.db.lock().map_err(|e| anyhow!("Lock DB error: {}", e))?;

            let (inviter_user_id, inviter_username_cloned): (i64, String) = db.query_row(
                "SELECT id, username FROM users WHERE username = ?1",
                params![inviter_username],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;

            let invited_user_id: DbResult<i64> = db.query_row(
                "SELECT id FROM users WHERE username = ?1",
                params![&username_to_invite],
                |row| row.get(0),
            );

            let invited_user_id = match invited_user_id {
                Ok(id) => id,
                Err(_) => {
                    client_tx.send(ServerMessage::Error {
                        message: format!("Utente '{}' non trovato.", username_to_invite),
                    })?;
                    return Ok(());
                }
            };

            let is_member: DbResult<i64> = db.query_row(
                "SELECT 1 FROM group_members WHERE user_id = ?1 AND group_id = ?2",
                params![inviter_user_id, group_id],
                |row| row.get(0),
            );

            if is_member.is_err() {
                warn!("Utente '{}' ha tentato di invitare senza essere membro del gruppo {}", inviter_username, group_id);
                client_tx.send(ServerMessage::Error {
                    message: "Solo i membri del gruppo possono invitare altri utenti.".into(),
                })?;
                return Ok(());
            }

            let group_name: String = db.query_row(
                "SELECT name FROM groups WHERE id = ?1",
                params![group_id],
                |row| row.get(0),
            )?;

            match db.execute(
                "INSERT INTO group_invites (invited_user_id, group_id, inviter_user_id) VALUES (?1, ?2, ?3)",
                params![invited_user_id, group_id, inviter_user_id],
            ) {
                Ok(_) => {
                    info!(
                        "Invito inviato a '{}' per il gruppo '{}'",
                        username_to_invite, group_name
                    );

                    let active_users = state.inner.active_users.lock().map_err(|e| anyhow!("Lock error: {}", e))?;
                    if let Some(target_tx) = active_users.get(&username_to_invite) {
                        let _ = target_tx.send(ServerMessage::InviteReceived {
                            group_id,
                            group_name,
                            inviter_username: inviter_username_cloned,
                        });
                    }

                    client_tx.send(ServerMessage::Error {
                        message: "Invito inviato.".into(),
                    })?;
                }
                Err(e) => {
                    warn!("Errore inserimento invito: {}", e);
                    client_tx.send(ServerMessage::Error {
                        message: "Invito già inviato o utente già nel gruppo.".into(),
                    })?;
                }
            }
        }

        ClientMessage::JoinGroup { group_id } => {
            let username = current_user
                .as_ref()
                .ok_or_else(|| anyhow!("Registrazione richiesta"))?;

            {
                let mut db = state.inner.db.lock().map_err(|e| anyhow!("Lock DB error: {}", e))?;

                let user_id: i64 = db.query_row(
                    "SELECT id FROM users WHERE username = ?1",
                    params![username],
                    |row| row.get(0),
                )?;

                let invite_exists: DbResult<i64> = db.query_row(
                    "SELECT 1 FROM group_invites WHERE invited_user_id = ?1 AND group_id = ?2",
                    params![user_id, group_id],
                    |row| row.get(0),
                );

                if invite_exists.is_err() {
                    warn!("Utente '{}' ha tentato di unirsi al gruppo {} senza invito", username, group_id);
                    client_tx.send(ServerMessage::Error {
                        message: "Non puoi unirti a questo gruppo senza un invito valido.".into(),
                    })?;
                    return Ok(());
                }

                let tx = db.transaction()?;

                tx.execute(
                    "DELETE FROM group_invites WHERE invited_user_id = ?1 AND group_id = ?2",
                    params![user_id, group_id],
                )?;

                tx.execute(
                    "INSERT INTO group_members (user_id, group_id) VALUES (?1, ?2)",
                    params![user_id, group_id],
                )?;

                tx.commit()?;

                info!("Utente '{}' si è unito al gruppo {}", username, group_id);
            }
            let join_msg = ServerMessage::UserJoinedGroup {
                group_id,
                username: username.to_string(),
            };
            broadcast_to_group(state.clone(), group_id, join_msg, None).await?;
        }

        ClientMessage::SendMessage { group_id, content } => {
            let sender_username = current_user
                .as_ref()
                .ok_or_else(|| anyhow!("Registrazione richiesta"))?;

            {
                let db = state.inner.db.lock().map_err(|e| anyhow!("Lock DB error: {}", e))?;

                let user_id: i64 = db.query_row(
                    "SELECT id FROM users WHERE username = ?1",
                    params![sender_username],
                    |row| row.get(0),
                )?;

                let is_member: DbResult<i64> = db.query_row(
                    "SELECT 1 FROM group_members WHERE user_id = ?1 AND group_id = ?2",
                    params![user_id, group_id],
                    |row| row.get(0),
                );

                if is_member.is_err() {
                    client_tx.send(ServerMessage::Error {
                        message: "Non sei membro di questo gruppo.".into(),
                    })?;
                    return Ok(());
                }
            }
            let timestamp = Utc::now().timestamp();
            let msg = ServerMessage::NewMessage {
                group_id,
                sender_username: sender_username.to_string(),
                content,
                timestamp,
            };
            broadcast_to_group(state.clone(), group_id, msg, None).await?;
        }

        // === FASE 1: Nuovi handler per inviti basati su nome gruppo ===

        ClientMessage::InviteUser { group, username } => {
            let inviter_username = current_user
                .as_ref()
                .ok_or_else(|| anyhow!("Registrazione richiesta"))?;

            let db = state.inner.db.lock().map_err(|e| anyhow!("Lock DB error: {}", e))?;

            // Trova il gruppo per nome
            let group_result: DbResult<(i64, String)> = db.query_row(
                "SELECT id, name FROM groups WHERE name = ?1",
                params![&group],
                |row| Ok((row.get(0)?, row.get(1)?)),
            );

            let (group_id, group_name) = match group_result {
                Ok(g) => g,
                Err(_) => {
                    warn!("Tentativo di invito a gruppo inesistente '{}' da '{}'", group, inviter_username);
                    client_tx.send(ServerMessage::Error {
                        message: format!("Gruppo '{}' non trovato.", group),
                    })?;
                    return Ok(());
                }
            };

            // Verifica che l'invitante sia un membro
            let inviter_id_result: DbResult<i64> = db.query_row(
                "SELECT id FROM users WHERE username = ?1",
                params![inviter_username],
                |row| row.get(0),
            );

            let inviter_id = match inviter_id_result {
                Ok(id) => id,
                Err(_) => {
                    client_tx.send(ServerMessage::Error {
                        message: "Utente non trovato.".into(),
                    })?;
                    return Ok(());
                }
            };

            let is_member: DbResult<i64> = db.query_row(
                "SELECT 1 FROM group_members WHERE user_id = ?1 AND group_id = ?2",
                params![inviter_id, group_id],
                |row| row.get(0),
            );

            if is_member.is_err() {
                warn!("Utente '{}' ha tentato di invitare senza essere membro del gruppo '{}'", inviter_username, group);
                client_tx.send(ServerMessage::Error {
                    message: "Solo i membri del gruppo possono invitare altri utenti.".into(),
                })?;
                return Ok(());
            }

            // Verifica che l'utente da invitare esista
            let invited_result: DbResult<i64> = db.query_row(
                "SELECT id FROM users WHERE username = ?1",
                params![&username],
                |row| row.get(0),
            );

            let invited_id = match invited_result {
                Ok(id) => id,
                Err(_) => {
                    client_tx.send(ServerMessage::Error {
                        message: format!("Utente '{}' non trovato.", username),
                    })?;
                    return Ok(());
                }
            };

            // Verifica che non sia già membro
            let already_member: DbResult<i64> = db.query_row(
                "SELECT 1 FROM group_members WHERE user_id = ?1 AND group_id = ?2",
                params![invited_id, group_id],
                |row| row.get(0),
            );

            if already_member.is_ok() {
                client_tx.send(ServerMessage::Error {
                    message: format!("'{}' è già membro del gruppo.", username),
                })?;
                return Ok(());
            }

            // Inserisci l'invito
            match db.execute(
                "INSERT OR IGNORE INTO group_invites (invited_user_id, group_id, inviter_user_id) VALUES (?1, ?2, ?3)",
                params![invited_id, group_id, inviter_id],
            ) {
                Ok(_) => {
                    info!("Invito inviato da '{}' a '{}' per il gruppo '{}'", inviter_username, username, group);

                    // Notifica l'utente invitato se online
                    let active_users = state.inner.active_users.lock().map_err(|e| anyhow!("Lock error: {}", e))?;
                    if let Some(target_tx) = active_users.get(&username) {
                        let _ = target_tx.send(ServerMessage::InviteReceived {
                            group_id,
                            group_name: group_name.clone(),
                            inviter_username: inviter_username.clone(),
                        });
                    }

                    client_tx.send(ServerMessage::InviteSent {
                        group: group_name,
                        username,
                    })?;
                }
                Err(e) => {
                    warn!("Errore inserimento invito: {}", e);
                    client_tx.send(ServerMessage::Error {
                        message: "Invito già pendente o errore interno.".into(),
                    })?;
                }
            }
        }

        ClientMessage::AcceptInvite { group } => {
            let username = current_user
                .as_ref()
                .ok_or_else(|| anyhow!("Registrazione richiesta"))?;

            let (group_id, group_name) = {
                let mut db = state.inner.db.lock().map_err(|e| anyhow!("Lock DB error: {}", e))?;

                // Trova il gruppo
                let group_result: DbResult<(i64, String)> = db.query_row(
                    "SELECT id, name FROM groups WHERE name = ?1",
                    params![&group],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                );

                let (group_id, group_name) = match group_result {
                    Ok(g) => g,
                    Err(_) => {
                        client_tx.send(ServerMessage::Error {
                            message: format!("Gruppo '{}' non trovato.", group),
                        })?;
                        return Ok(());
                    }
                };

                // Trova l'utente
                let user_id: i64 = db.query_row(
                    "SELECT id FROM users WHERE username = ?1",
                    params![username],
                    |row| row.get(0),
                )?;

                // Verifica che esista un invito pendente
                let invite_exists: DbResult<i64> = db.query_row(
                    "SELECT 1 FROM group_invites WHERE invited_user_id = ?1 AND group_id = ?2",
                    params![user_id, group_id],
                    |row| row.get(0),
                );

                if invite_exists.is_err() {
                    warn!("Utente '{}' ha tentato di accettare un invito inesistente per '{}'", username, group);
                    client_tx.send(ServerMessage::Error {
                        message: "Nessun invito valido trovato per questo gruppo.".into(),
                    })?;
                    return Ok(());
                }

                // Accetta l'invito: rimuovi dall'invito e aggiungi ai membri
                let tx = db.transaction()?;

                tx.execute(
                    "DELETE FROM group_invites WHERE invited_user_id = ?1 AND group_id = ?2",
                    params![user_id, group_id],
                )?;

                tx.execute(
                    "INSERT INTO group_members (user_id, group_id) VALUES (?1, ?2)",
                    params![user_id, group_id],
                )?;

                tx.commit()?;

                info!("Utente '{}' ha accettato l'invito e si è unito al gruppo '{}'", username, group);

                (group_id, group_name)
            };

            // Invia conferma all'utente
            client_tx.send(ServerMessage::JoinedGroup {
                group_id,
                group_name: group_name.clone(),
            })?;

            // Notifica gli altri membri del gruppo
            let join_msg = ServerMessage::UserJoinedGroup {
                group_id,
                username: username.to_string(),
            };
            broadcast_to_group(state.clone(), group_id, join_msg, Some(username)).await?;
        }

        ClientMessage::RejectInvite { group } => {
            let username = current_user
                .as_ref()
                .ok_or_else(|| anyhow!("Registrazione richiesta"))?;

            let db = state.inner.db.lock().map_err(|e| anyhow!("Lock DB error: {}", e))?;

            // Trova il gruppo
            let group_result: DbResult<i64> = db.query_row(
                "SELECT id FROM groups WHERE name = ?1",
                params![&group],
                |row| row.get(0),
            );

            let group_id = match group_result {
                Ok(id) => id,
                Err(_) => {
                    client_tx.send(ServerMessage::Error {
                        message: format!("Gruppo '{}' non trovato.", group),
                    })?;
                    return Ok(());
                }
            };

            // Trova l'utente
            let user_id: i64 = db.query_row(
                "SELECT id FROM users WHERE username = ?1",
                params![username],
                |row| row.get(0),
            )?;

            // Verifica e rimuovi l'invito
            let deleted = db.execute(
                "DELETE FROM group_invites WHERE invited_user_id = ?1 AND group_id = ?2",
                params![user_id, group_id],
            )?;

            if deleted == 0 {
                client_tx.send(ServerMessage::Error {
                    message: "Nessun invito da rifiutare per questo gruppo.".into(),
                })?;
                return Ok(());
            }

            info!("Utente '{}' ha rifiutato l'invito per il gruppo '{}'", username, group);

            client_tx.send(ServerMessage::InviteRejected {
                group: group.clone(),
            })?;
        }
    }
    Ok(())
}

async fn cpu_logging_task() {
    use std::fs::OpenOptions;
    use std::io::Write;

    let mut sys = System::new_all();
    let pid = std::process::id();
    let mut interval = tokio::time::interval(Duration::from_secs(120));

    loop {
        interval.tick().await;
        sys.refresh_cpu();
        let cpu = sys.global_cpu_info().cpu_usage();
        let timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();

        let log_line = format!("[{}] PID: {} - CPU usage: {:.2}%\n", timestamp, pid, cpu);

        // Log to console/file via tracing
        info!("CPU usage: {:.2}%", cpu);

        // Write to cpu.log file
        match OpenOptions::new()
            .create(true)
            .append(true)
            .open("cpu.log")
        {
            Ok(mut file) => {
                if let Err(e) = file.write_all(log_line.as_bytes()) {
                    warn!("Errore scrittura cpu.log: {}", e);
                }
            }
            Err(e) => {
                warn!("Errore apertura cpu.log: {}", e);
            }
        }
    }
}