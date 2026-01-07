use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use log::warn;
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::Span,
    widgets::{Block, Borders, List, ListItem, ListState, Paragraph},
    Frame, Terminal,
};
use ruggine_common::{ClientMessage, ServerMessage};
use serde_json;
use std::{
    io::{self, Stdout},
    time::Duration,
};
use tokio::sync::mpsc;
// Import necessario per la funzione helper
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
// Esplicito per chiarezza
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message as WsMessage;

/// Tipo alias per il terminale
type Term = Terminal<CrosstermBackend<Stdout>>;

/// Stato globale dell'applicazione client
struct AppState {
    input_buffer: String,
    username: String,
    messages: Vec<String>,
    list_state: ListState,
    is_registered: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // --- Canali di Comunicazione ---
    // UI -> WS (Invia messaggi al server)
    let (ui_to_ws_tx, ui_to_ws_rx) = mpsc::unbounded_channel::<ClientMessage>();
    // WS -> UI (Riceve messaggi dal server)
    let (ws_to_ui_tx, ws_to_ui_rx) = mpsc::unbounded_channel::<ServerMessage>();

    // --- Setup WebSocket in background ---
    // spawn un task che mantiene la WS; non lo awaitiamo qui
    tokio::spawn(async move {
        if let Err(e) = websocket_task(ui_to_ws_rx, ws_to_ui_tx).await {
            warn!("websocket_task terminato con errore: {:?}", e);
        }
    });

    // --- Setup TUI ---
    let mut terminal = init_terminal()?;

    let mut app = AppState {
        input_buffer: String::new(),
        username: String::new(),
        messages: vec![
            "Benvenuto in Ruggine! Inserisci il tuo username per registrarti.".into(),
            "Digita /help per vedere tutti i comandi disponibili.".into(),
        ],
        list_state: ListState::default(),
        is_registered: false,
    };
    app.list_state.select(Some(0)); // Seleziona l'ultimo messaggio

    // Esegui il loop principale della TUI
    let res = run_app(&mut terminal, &mut app, ui_to_ws_tx, ws_to_ui_rx).await;

    // Ripristina il terminale (anche se run_app fallisce)
    if let Err(e) = restore_terminal(&mut terminal) {
        // in genere vogliamo mostrare l'errore ma non panicare
        eprintln!("Errore durante il ripristino del terminale: {:?}", e);
    }

    // Propaga eventuale errore di run_app
    res.map_err(|e| e.into())
}

/// Task che gestisce la connessione WebSocket
async fn websocket_task(
    mut ui_to_ws_rx: mpsc::UnboundedReceiver<ClientMessage>,
    ws_to_ui_tx: mpsc::UnboundedSender<ServerMessage>,
) -> anyhow::Result<()> {
    let url = "ws://127.0.0.1:4000";
    let ws_to_ui_tx_clone = ws_to_ui_tx.clone();
    let (ws_stream, _) = connect_async(url).await.map_err(|e| {
        let _ = ws_to_ui_tx_clone.send(ServerMessage::Error {
            message: format!("Connessione fallita: {}", e),
        });
        e
    })?;

    // Dividi il WebSocket in sink e stream
    let (mut ws_tx, mut ws_rx) = ws_stream.split();

    ws_to_ui_tx.send(ServerMessage::Error {
        message: "Connesso al server!".into(),
    })?;

    // Task di invio (da UI -> WS)
    let send_task = tokio::spawn({
        // let mut ws_tx = ws_tx; // ✅ Spostiamo la ownership nel task
        async move {
            while let Some(msg) = ui_to_ws_rx.recv().await {
                if let Ok(text) = serde_json::to_string(&msg) {
                    if ws_tx.send(WsMessage::Text(text)).await.is_err() {
                        break; // errore -> esci
                    }
                }
            }
        }
    });

    // Task di ricezione (da WS -> UI)
    let recv_task = tokio::spawn({
        let ws_to_ui_tx = ws_to_ui_tx.clone();
        async move {
            while let Some(Ok(msg)) = ws_rx.next().await {
                if let WsMessage::Text(text) = msg {
                    if let Ok(server_msg) = serde_json::from_str::<ServerMessage>(&text) {
                        if ws_to_ui_tx.send(server_msg).is_err() {
                            break;
                        }
                    }
                }
            }
        }
    });

    // Attendi che uno dei due termini
    tokio::select! {
        _ = send_task => {},
        _ = recv_task => {},
    }

    let _ = ws_to_ui_tx.send(ServerMessage::Error {
        message: "Disconnesso.".into(),
    });

    Ok(())
}

/// Loop principale della TUI
async fn run_app(
    term: &mut Term,
    app: &mut AppState,
    ui_to_ws_tx: mpsc::UnboundedSender<ClientMessage>,
    mut ws_to_ui_rx: mpsc::UnboundedReceiver<ServerMessage>,
) -> io::Result<()> {
    loop {
        // Disegna la UI
        term.draw(|f| ui(f, app))?;

        // Gestisci input (eventi)
        // Controlla per max 100ms, poi ridisegna
        let timeout = Duration::from_millis(100);

        // Controlla prima i messaggi dal server (non bloccante)
        while let Ok(msg) = ws_to_ui_rx.try_recv() {
            handle_server_message(msg, app);
        }

        // Controlla input utente (bloccante per 'timeout')
        if event::poll(timeout)? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    match key.code {
                        KeyCode::Enter => {
                            let input: String = app.input_buffer.drain(..).collect();
                            let input = input.trim().to_string();
                            if input.is_empty() {
                                continue;
                            }

                            if !app.is_registered {
                                // Logica di Registrazione
                                app.username = input.clone();
                                // Invia Register al server
                                let _ =
                                    ui_to_ws_tx.send(ClientMessage::Register { username: input });
                            } else {
                                // Logica Comandi e Messaggi
                                if input.starts_with('/') {
                                    // È un comando
                                    handle_command(&input, app, &ui_to_ws_tx);
                                } else {
                                    // È un messaggio
                                    app.messages.push(format!("[Tu]: {}", input));
                                    // NOTA: Invia al gruppo 1 come da codice originale.
                                    // Una UI migliore permetterebbe di selezionare il gruppo.
                                    let _ = ui_to_ws_tx.send(ClientMessage::SendMessage {
                                        group_id: 1, // Hardcoded
                                        content: input,
                                    });
                                }
                                // Scroll alla fine
                                app.list_state
                                    .select(Some(app.messages.len().saturating_sub(1)));
                            }
                        }
                        KeyCode::Char(c) => {
                            app.input_buffer.push(c);
                        }
                        KeyCode::Backspace => {
                            app.input_buffer.pop();
                        }
                        KeyCode::Esc => {
                            // Esci
                            return Ok(());
                        }
                        KeyCode::Up => {
                            let sel = app.list_state.selected().unwrap_or(0);
                            let new = sel.saturating_sub(1);
                            app.list_state.select(Some(new));
                        }
                        KeyCode::Down => {
                            let sel = app.list_state.selected().unwrap_or(0);
                            let new = sel.saturating_add(1);
                            if new < app.messages.len() {
                                app.list_state.select(Some(new));
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    }
}

// --- NUOVA FUNZIONE: handle_command ---
/// Analizza ed esegue i comandi locali (che iniziano con /)
fn handle_command(
    input: &str,
    app: &mut AppState,
    ui_to_ws_tx: &mpsc::UnboundedSender<ClientMessage>,
) {
    let parts: Vec<&str> = input.split_whitespace().collect();
    if parts.is_empty() {
        return;
    }

    match parts[0] {
        "/create" => {
            if parts.len() >= 2 {
                let name = parts[1..].join(" ");
                app.messages
                    .push(format!("[Comando inviato]: Crea gruppo '{}'", name));
                let _ = ui_to_ws_tx.send(ClientMessage::CreateGroup { name });
            } else {
                app.messages
                    .push("[Errore]: Uso: /create <nome_gruppo>".into());
            }
        }
        "/invite" => {
            if parts.len() >= 3 {
                let username_to_invite = parts[1].to_string();
                let group_arg = parts[2..].join(" ");

                // Prova prima come ID numerico (legacy), altrimenti come nome gruppo
                if let Ok(group_id) = group_arg.parse::<i64>() {
                    app.messages.push(format!(
                        "[Comando inviato]: Invita {} al gruppo {}",
                        username_to_invite, group_id
                    ));
                    let _ = ui_to_ws_tx
                        .send(ClientMessage::InviteToGroup { username_to_invite, group_id });
                } else {
                    // Usa il nome del gruppo
                    app.messages.push(format!(
                        "[Comando inviato]: Invita {} al gruppo '{}'",
                        username_to_invite, group_arg
                    ));
                    let _ = ui_to_ws_tx.send(ClientMessage::InviteUser {
                        group: group_arg,
                        username: username_to_invite,
                    });
                }
            } else {
                app.messages
                    .push("[Errore]: Uso: /invite <username> <group_id o nome_gruppo>".into());
            }
        }
        "/join" => {
            if parts.len() >= 2 {
                let group_arg = parts[1..].join(" ");

                if let Ok(group_id) = group_arg.parse::<i64>() {
                    app.messages.push(format!(
                        "[Comando inviato]: Unisciti al gruppo {}",
                        group_id
                    ));
                    let _ = ui_to_ws_tx.send(ClientMessage::JoinGroup { group_id });
                } else {
                    app.messages.push(format!(
                        "[Comando inviato]: Accetta invito per il gruppo '{}'",
                        group_arg
                    ));
                    let _ = ui_to_ws_tx.send(ClientMessage::AcceptInvite { group: group_arg });
                }
            } else {
                app.messages.push("[Errore]: Uso: /join <group_id o nome_gruppo>".into());
            }
        }
        "/accept" => {
            if parts.len() >= 2 {
                let group = parts[1..].join(" ");
                app.messages.push(format!(
                    "[Comando inviato]: Accetta invito per il gruppo '{}'",
                    group
                ));
                let _ = ui_to_ws_tx.send(ClientMessage::AcceptInvite { group });
            } else {
                app.messages.push("[Errore]: Uso: /accept <nome_gruppo>".into());
            }
        }
        "/reject" => {
            if parts.len() >= 2 {
                let group = parts[1..].join(" ");
                app.messages.push(format!(
                    "[Comando inviato]: Rifiuta invito per il gruppo '{}'",
                    group
                ));
                let _ = ui_to_ws_tx.send(ClientMessage::RejectInvite { group });
            } else {
                app.messages.push("[Errore]: Uso: /reject <nome_gruppo>".into());
            }
        }
        "/help" => {
            app.messages.push("Comandi disponibili:".into());
            app.messages.push("  /create <nome>       - Crea un nuovo gruppo".into());
            app.messages.push("  /invite <user> <grp> - Invita un utente al gruppo".into());
            app.messages.push("  /join <gruppo>       - Accetta invito (ID o nome)".into());
            app.messages.push("  /accept <nome>       - Accetta invito per gruppo".into());
            app.messages.push("  /reject <nome>       - Rifiuta invito per gruppo".into());
            app.messages.push("  /help                - Mostra questo aiuto".into());
        }
        _ => {
            app.messages
                .push(format!("[Errore]: Comando '{}' sconosciuto. Usa /help.", parts[0]));
        }
    }
}
// --- FINE NUOVA FUNZIONE ---

/// Gestisce i messaggi ricevuti dal server e aggiorna lo stato della UI
fn handle_server_message(msg: ServerMessage, app: &mut AppState) {
    match msg {
        ServerMessage::RegisterResponse { success, reason } => {
            if success {
                app.is_registered = true;
                let welcome_msg = match reason {
                    Some(ref r) if r.contains("Bentornato") => {
                        format!("👋 {} Loggato come '{}'. Digita /help per i comandi.", r, app.username)
                    }
                    _ => {
                        format!("✅ Registrazione completata come '{}'. Digita /help per i comandi.", app.username)
                    }
                };
                app.messages.push(welcome_msg);
            } else {
                app.messages.push(format!(
                    "❌ Registrazione fallita: {}",
                    reason.unwrap_or_else(|| "Errore sconosciuto".into())
                ));
                app.username.clear();
            }
        }
        ServerMessage::Error { message } => {
            app.messages.push(format!("[SERVER]: {}", message));
        }
        ServerMessage::NewMessage { group_id, sender_username, content, .. } => {
            app.messages.push(format!(
                "[Gruppo {}] {}: {}",
                group_id, sender_username, content
            ));
        }
        ServerMessage::InviteReceived { group_id, group_name, inviter_username } => {
            app.messages.push(format!(
                "📩 Ricevuto invito da '{}' per il gruppo '{}' (ID: {})",
                inviter_username, group_name, group_id
            ));
            app.messages.push(format!(
                "   → Accetta con: /accept {} oppure /join {}",
                group_name, group_id
            ));
            app.messages.push(format!(
                "   → Rifiuta con: /reject {}",
                group_name
            ));
        }
        // --- NUOVI MESSAGGI GESTITI ---
        ServerMessage::UserJoinedGroup { group_id, username } => {
            app.messages.push(format!(
                "[Gruppo {}] L'utente '{}' si è unito.",
                group_id, username
            ));
        }
        ServerMessage::GroupCreated { id, name } => {
            app.messages.push(format!(
                "✅ Creato gruppo '{}' (ID: {}). Invita qualcuno! /invite <utente> {}",
                name, id, name
            ));
        }
        ServerMessage::InviteSent { group, username } => {
            app.messages.push(format!(
                "✅ Invito inviato a '{}' per il gruppo '{}'",
                username, group
            ));
        }
        ServerMessage::JoinedGroup { group_id, group_name } => {
            app.messages.push(format!(
                "✅ Ti sei unito al gruppo '{}' (ID: {}). Puoi ora inviare messaggi!",
                group_name, group_id
            ));
        }
        ServerMessage::InviteRejected { group } => {
            app.messages.push(format!(
                "❌ Hai rifiutato l'invito per il gruppo '{}'",
                group
            ));
        }
    }
    // Auto-scroll alla fine
    app.list_state
        .select(Some(app.messages.len().saturating_sub(1)));
}

/// Disegna i widget della TUI
fn ui(f: &mut Frame, app: &AppState) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(1), Constraint::Length(3)].as_ref())
        .split(f.size());

    // Area Messaggi
    let items: Vec<ListItem> = app.messages.iter().map(|m| ListItem::new(Span::raw(m))).collect();

    let list = List::new(items)
        .block(Block::default().borders(Borders::ALL).title("Chat"))
        .highlight_style(
            Style::default()
                .add_modifier(Modifier::BOLD)
                .bg(Color::DarkGray),
        );

    // Per mantenere e aggiornare correttamente lo stato della lista,
    // cloniamo lo stato, lo passiamo al render e poi lo riscriviamo.
    let mut state = app.list_state.clone();
    f.render_stateful_widget(list, chunks[0], &mut state);
    // Non possiamo aggiornare app.list_state qui perché abbiamo solo &AppState
    // (questa funzione riceve app immutabile). Lo stato è aggiornato nel loop principale.

    // Area Input
    let title = if app.is_registered {
        format!("Input ({}):", app.username)
    } else {
        "Input (Registrazione):".into()
    };

    let input = Paragraph::new(app.input_buffer.as_str())
        .style(Style::default())
        .block(Block::default().borders(Borders::ALL).title(title));
    f.render_widget(input, chunks[1]);

    // Cursore
    f.set_cursor(
        // posizione x: inizio area input + 1 (bordo) + lunghezza buffer
        chunks[1].x + app.input_buffer.len() as u16 + 1,
        // posizione y: inizio area input + 1 (bordo)
        chunks[1].y + 1,
    );
}

// --- Funzioni Helper TUI ---

fn init_terminal() -> io::Result<Term> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    Terminal::new(backend)
}

fn restore_terminal(term: &mut Term) -> io::Result<()> {
    disable_raw_mode()?;
    // riporta lo schermo alternativo e mostra il cursore
    execute!(term.backend_mut(), LeaveAlternateScreen)?;
    term.show_cursor()
}