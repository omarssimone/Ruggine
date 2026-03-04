// ruggine_client/src/main.rs
use eframe::egui;
use ruggine_common::{ClientMessage, GroupInfo, ServerMessage};
use tokio::sync::mpsc;

fn main() -> eframe::Result<()> {
    // Canali di comunicazione UI <-> WebSocket
    let (ui_to_ws_tx, ui_to_ws_rx) = mpsc::unbounded_channel::<ClientMessage>();
    let (ws_to_ui_tx, ws_to_ui_rx) = mpsc::unbounded_channel::<ServerMessage>();

    // Avvia tokio runtime in un thread separato
    std::thread::spawn(move || {
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(websocket_task(ui_to_ws_rx, ws_to_ui_tx));
    });

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_title("Ruggine Chat")
            .with_inner_size([900.0, 600.0])
            .with_min_inner_size([600.0, 400.0]),
        ..Default::default()
    };

    eframe::run_native(
        "Ruggine Chat",
        options,
        Box::new(|_cc| Box::new(RuggineApp::new(ui_to_ws_tx, ws_to_ui_rx))),
    )
}

// --- Stato dell'app ---

struct RuggineApp {
    // Canali
    ui_to_ws_tx: mpsc::UnboundedSender<ClientMessage>,
    ws_to_ui_rx: mpsc::UnboundedReceiver<ServerMessage>,

    // Stato utente
    username_input: String,
    username: String,
    is_registered: bool,

    // Chat
    messages: Vec<ChatMessage>,
    input_buffer: String,

    // Gruppi
    groups: Vec<GroupInfo>,
    active_group: Option<GroupInfo>,

    // UI
    connection_status: String,
}

struct ChatMessage {
    sender: String,
    content: String,
    group_name: Option<String>,
    group_id: Option<i64>,
    is_system: bool,
}

impl RuggineApp {
    fn new(
        ui_to_ws_tx: mpsc::UnboundedSender<ClientMessage>,
        ws_to_ui_rx: mpsc::UnboundedReceiver<ServerMessage>,
    ) -> Self {
        Self {
            ui_to_ws_tx,
            ws_to_ui_rx,
            username_input: String::new(),
            username: String::new(),
            is_registered: false,
            messages: vec![ChatMessage {
                sender: "Sistema".into(),
                content: "Benvenuto in Ruggine! Inserisci il tuo username per connetterti.".into(),
                group_name: None,
                group_id: None,
                is_system: true,
            }],
            input_buffer: String::new(),
            groups: Vec::new(),
            active_group: None,
            connection_status: "Connessione in corso...".into(),
        }
    }

    fn send_message(&mut self) {
        let content = self.input_buffer.trim().to_string();
        if content.is_empty() { return; }
        self.input_buffer.clear();

        if content.starts_with('/') {
            self.handle_command(&content);
            return;
        }

        if let Some(ref group) = self.active_group.clone() {
            self.messages.push(ChatMessage {
                sender: format!("Tu"),
                content: content.clone(),
                group_name: Some(group.name.clone()),
                group_id: Some(group.id),
                is_system: false,
            });
            let _ = self.ui_to_ws_tx.send(ClientMessage::SendMessage {
                group_id: group.id,
                content,
            });
        } else {
            self.push_system("❌ Nessun gruppo attivo. Crea uno con /create <nome>");
        }
    }

    fn handle_command(&mut self, input: &str) {
        let parts: Vec<&str> = input.split_whitespace().collect();
        match parts[0] {
            "/create" if parts.len() >= 2 => {
                let name = parts[1..].join(" ");
                let _ = self.ui_to_ws_tx.send(ClientMessage::CreateGroup { name });
            }
            "/invite" if parts.len() >= 3 => {
                let username = parts[1].to_string();
                let group = parts[2..].join(" ");
                let _ = self.ui_to_ws_tx.send(ClientMessage::InviteUser { group, username });
            }
            "/accept" if parts.len() >= 2 => {
                let group = parts[1..].join(" ");
                let _ = self.ui_to_ws_tx.send(ClientMessage::AcceptInvite { group });
            }
            "/reject" if parts.len() >= 2 => {
                let group = parts[1..].join(" ");
                let _ = self.ui_to_ws_tx.send(ClientMessage::RejectInvite { group });
            }
            "/help" => {
                self.push_system("Comandi:\n /create <nome> per creare un nuovo gruppo\n| /invite <user> <gruppo> per invitare l'utente <user> al gruppo <gruppo>\n | /accept <gruppo> per accettare l'invito al gruppo <gruppo>\n | /reject <gruppo> per rifiutare l'invito al gruppo <gruppo>");
            }
            _ => self.push_system(&format!("Comando sconosciuto: {}. Usa /help", parts[0])),
        }
    }

    fn push_system(&mut self, msg: &str) {
        self.messages.push(ChatMessage {
            sender: "Sistema".into(),
            content: msg.to_string(),
            group_name: None,
            group_id: None,
            is_system: true,
        });
    }

    fn process_server_messages(&mut self) {
        while let Ok(msg) = self.ws_to_ui_rx.try_recv() {
            match msg {
                ServerMessage::Error { message } => {
                    let status = message.clone();
                    self.connection_status = status;
                    self.push_system(&message);
                }
                ServerMessage::RegisterResponse { success, reason } => {
                    if success {
                        self.is_registered = true;
                        self.push_system(&format!("✅ Connesso come '{}'", self.username));
                        let _ = self.ui_to_ws_tx.send(ClientMessage::ListGroups);
                    } else {
                        self.push_system(&format!("❌ {}", reason.unwrap_or("Errore".into())));
                        self.username.clear();
                    }
                }
                ServerMessage::GroupList { groups } => {
                    if groups.is_empty() {
                        self.push_system("Nessun gruppo ancora. Usa /create <nome> per crearne uno.");
                    }
                    if self.active_group.is_none() {
                        self.active_group = groups.first().cloned();
                    }
                    self.groups = groups;
                }
                ServerMessage::NewMessage { group_id, sender_username, content, .. } => {
                    let group_name = self.groups.iter()
                        .find(|g| g.id == group_id)
                        .map(|g| g.name.clone())
                        .unwrap_or_else(|| format!("Gruppo {}", group_id));
                    self.messages.push(ChatMessage {
                        sender: sender_username,
                        content,
                        group_name: Some(group_name),
                        group_id: Some(group_id),
                        is_system: false,
                    });
                }
                ServerMessage::GroupCreated { id, name } => {
                    self.push_system(&format!("✅ Gruppo '{}' creato (ID: {})", name, id));
                    let group = GroupInfo { id, name };
                    self.active_group = Some(group.clone());
                    self.groups.push(group);
                }
                ServerMessage::InviteReceived { group_name, inviter_username, .. } => {
                    self.push_system(&format!(
                        "📩 Invito da '{}' per il gruppo '{}' → /accept {} oppure /reject {}",
                        inviter_username, group_name, group_name, group_name
                    ));
                }
                ServerMessage::JoinedGroup { group_id, group_name } => {
                    self.push_system(&format!("✅ Unito al gruppo '{}'", group_name));
                    let group = GroupInfo { id: group_id, name: group_name };
                    self.active_group = Some(group.clone());
                    self.groups.push(group);
                }
                ServerMessage::UserJoinedGroup { group_id, username } => {
                    let group_name = self.groups.iter()
                        .find(|g| g.id == group_id)
                        .map(|g| g.name.clone())
                        .unwrap_or_else(|| format!("{}", group_id));
                    self.push_system(&format!("👤 '{}' si è unito a '{}'", username, group_name));
                }
                ServerMessage::InviteSent { group, username } => {
                    self.push_system(&format!("✅ Invito inviato a '{}' per '{}'", username, group));
                }
                ServerMessage::InviteRejected { group } => {
                    self.push_system(&format!("❌ Invito rifiutato per '{}'", group));
                }
            }
        }
    }
}

impl eframe::App for RuggineApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        // Processa messaggi dal server ad ogni frame
        self.process_server_messages();

        // Richiedi ridisegno continuo (necessario per ricevere messaggi in real-time)
        ctx.request_repaint_after(std::time::Duration::from_millis(50));

        // Schermata di login
        if !self.is_registered {
            egui::CentralPanel::default().show(ctx, |ui| {
                ui.vertical_centered(|ui| {
                    ui.add_space(180.0);
                    ui.heading("🦀 Ruggine Chat");
                    ui.add_space(20.0);
                    ui.label(&self.connection_status);
                    ui.add_space(20.0);

                    let response = ui.add(
                        egui::TextEdit::singleline(&mut self.username_input)
                            .hint_text("Il tuo username...")
                            .desired_width(250.0),
                    );

                    if response.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter)) {
                        let username = self.username_input.trim().to_string();
                        if !username.is_empty() {
                            self.username = username.clone();
                            let _ = self.ui_to_ws_tx.send(ClientMessage::Register { username });
                        }
                    }

                    ui.add_space(10.0);
                    if ui.button("Connetti").clicked() {
                        let username = self.username_input.trim().to_string();
                        if !username.is_empty() {
                            self.username = username.clone();
                            let _ = self.ui_to_ws_tx.send(ClientMessage::Register { username });
                        }
                    }
                });
            });
            return;
        }

        // --- UI principale dopo login ---

        // Pannello sinistro: lista gruppi
        egui::SidePanel::left("groups_panel")
            .resizable(false)
            .exact_width(200.0)
            .show(ctx, |ui| {
                ui.add_space(8.0);
                ui.heading("💬 Gruppi");
                ui.separator();

                for group in &self.groups.clone() {
                    let is_active = self.active_group.as_ref().map(|g| g.id) == Some(group.id);
                    let label = if is_active {
                        format!("▶ {}", group.name)
                    } else {
                        format!("  {}", group.name)
                    };

                    if ui.selectable_label(is_active, &label).clicked() {
                        self.active_group = Some(group.clone());
                    }
                }

                ui.add_space(8.0);
                ui.separator();
                ui.small(format!("👤 {}", self.username));
            });

        // Pannello principale: messaggi + input
        egui::CentralPanel::default().show(ctx, |ui| {
            let active_name = self.active_group.as_ref()
                .map(|g| g.name.clone())
                .unwrap_or_else(|| "Nessun gruppo".into());

            ui.horizontal(|ui| {
                ui.heading(format!("# {}", active_name));
            });
            ui.separator();

            // Area messaggi con scroll
            let available = ui.available_height() - 50.0;
            egui::ScrollArea::vertical()
                .max_height(available)
                .stick_to_bottom(true)
                .show(ui, |ui| {
                    let active_id = self.active_group.as_ref().map(|g| g.id);
                    for msg in self.messages.iter().filter(|m| {
                        m.is_system || m.group_id == active_id
                    }) {
                        if msg.is_system {
                            ui.colored_label(egui::Color32::from_rgb(150, 150, 150), &msg.content);
                        } else {
                            ui.horizontal(|ui| {
                                if let Some(ref gname) = msg.group_name {
                                    ui.colored_label(egui::Color32::from_rgb(80, 80, 80), format!("[{}]", gname));
                                }
                                ui.colored_label(egui::Color32::from_rgb(100, 180, 255),
                                                 format!("{}:", msg.sender));
                                ui.label(&msg.content);
                            });
                        }
                    }
                });

            ui.separator();

            // Barra di input
            ui.horizontal(|ui| {
                let input_response = ui.add(
                    egui::TextEdit::singleline(&mut self.input_buffer)
                        .hint_text("Scrivi un messaggio o /help per i comandi...")
                        .desired_width(ui.available_width() - 70.0),
                );

                let send_clicked = ui.button("Invia").clicked();
                let enter_pressed = input_response.lost_focus()
                    && ui.input(|i| i.key_pressed(egui::Key::Enter));

                if send_clicked || enter_pressed {
                    self.send_message();
                    input_response.request_focus();
                }
            });
        });
    }
}

// --- WebSocket task (invariato) ---
async fn websocket_task(
    mut ui_to_ws_rx: mpsc::UnboundedReceiver<ClientMessage>,
    ws_to_ui_tx: mpsc::UnboundedSender<ServerMessage>,
) {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::{connect_async, tungstenite::Message as WsMessage};

    let url = "ws://127.0.0.1:4000";

    let (ws_stream, _) = match connect_async(url).await {
        Ok(s) => s,
        Err(e) => {
            let _ = ws_to_ui_tx.send(ServerMessage::Error {
                message: format!("Connessione fallita: {}", e),
            });
            return;
        }
    };

    let _ = ws_to_ui_tx.send(ServerMessage::Error {
        message: "Connesso al server!".into(),
    });

    let (mut ws_tx, mut ws_rx) = ws_stream.split();

    let send_task = tokio::spawn({
        async move {
            while let Some(msg) = ui_to_ws_rx.recv().await {
                if let Ok(text) = serde_json::to_string(&msg) {
                    if ws_tx.send(WsMessage::Text(text)).await.is_err() { break; }
                }
            }
        }
    });

    let recv_task = tokio::spawn({
        let ws_to_ui_tx = ws_to_ui_tx.clone();
        async move {
            while let Some(Ok(msg)) = ws_rx.next().await {
                if let WsMessage::Text(text) = msg {
                    if let Ok(server_msg) = serde_json::from_str::<ServerMessage>(&text) {
                        if ws_to_ui_tx.send(server_msg).is_err() { break; }
                    }
                }
            }
        }
    });

    tokio::select! {
        _ = send_task => {},
        _ = recv_task => {},
    }

    let _ = ws_to_ui_tx.send(ServerMessage::Error { message: "Disconnesso dal server.".into() });
}