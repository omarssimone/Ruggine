use super::handlers;
use crate::state::SharedState;
// ruggine_server/src/http/routes.rs
use axum::{routing::get, routing::post, Router};

/// Create all HTTP routes
pub fn create_routes(state: SharedState) -> Router {
    Router::new()
        .route("/health", get(handlers::health_check))
        .route("/users", get(handlers::get_users))
        .route("/login", post(handlers::login))
        .with_state(state)
}

