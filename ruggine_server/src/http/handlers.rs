use crate::state::SharedState;
// ruggine_server/src/http/handlers.rs
use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use ruggine_common::UserDTO;
use serde::Deserialize;
use tracing::{info, warn};

/// GET /health - Health check endpoint
pub async fn health_check() -> &'static str {
    "OK"
}

/// GET /users - Get all users
pub async fn get_users(
    State(state): State<SharedState>,
) -> Result<Json<Vec<UserDTO>>, (StatusCode, String)> {
    let db = state.db.lock().map_err(|e| {
        (StatusCode::INTERNAL_SERVER_ERROR, format!("DB lock error: {}", e))
    })?;

    let mut stmt = db
        .prepare("SELECT id, username FROM users")
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("DB error: {}", e)))?;

    let users: Vec<UserDTO> = stmt
        .query_map([], |row| {
            Ok(UserDTO {
                id: row.get(0)?,
                username: row.get(1)?,
            })
        })
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Query error: {}", e)))?
        .filter_map(|r| r.ok())
        .collect();

    info!("GET /users - returned {} users", users.len());
    Ok(Json(users))
}

/// Login request body
#[derive(Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

/// POST /login - Simple login (no JWT/sessions yet)
pub async fn login(
    State(state): State<SharedState>,
    Json(payload): Json<LoginRequest>,
) -> impl IntoResponse {
    let db = match state.db.lock() {
        Ok(db) => db,
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, format!("DB lock error: {}", e));
        }
    };

    // Check if user exists in database
    let user_exists: Result<i64, _> = db.query_row(
        "SELECT id FROM users WHERE username = ?1",
        rusqlite::params![&payload.username],
        |row| row.get(0),
    );

    match user_exists {
        Ok(user_id) => {
            // For now, we just check if user exists (no password verification)
            // Password field is accepted but not validated - this is a placeholder
            info!("Login successful for user: {} (ID: {})", payload.username, user_id);
            (StatusCode::OK, format!("Login successful for {}", payload.username))
        }
        Err(_) => {
            warn!("Login failed: user '{}' not found", payload.username);
            (StatusCode::UNAUTHORIZED, "Invalid credentials".to_string())
        }
    }
}

