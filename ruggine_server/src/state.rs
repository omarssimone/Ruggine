// ruggine_server/src/state.rs
use ruggine_common::ServerMessage;
use rusqlite::Connection;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

pub type ClientTx = mpsc::UnboundedSender<ServerMessage>;

/// Application state shared between TCP/WebSocket server and HTTP API
pub struct AppState {
    pub db: Mutex<Connection>,
    pub active_users: Mutex<HashMap<String, ClientTx>>,
}

impl AppState {
    pub fn new(conn: Connection) -> Self {
        Self {
            db: Mutex::new(conn),
            active_users: Mutex::new(HashMap::new()),
        }
    }
}

/// Thread-safe shared state type alias
pub type SharedState = Arc<AppState>;

