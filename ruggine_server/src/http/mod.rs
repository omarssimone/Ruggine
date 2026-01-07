// ruggine_server/src/http/mod.rs
pub mod handlers;
pub mod routes;

use crate::state::SharedState;
use axum::Router;
use tower_http::cors::{Any, CorsLayer};

/// Build the Axum router with all routes and middleware
pub fn build_router(state: SharedState) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    routes::create_routes(state).layer(cors)
}

