use anyhow::{Context, Result};
use axum::{
    extract::{Request, State},
    http::{HeaderMap, StatusCode},
    middleware::{self, Next},
    response::{Html, Response},
    routing::get,
    Router,
};
use rmcp::transport::{
    streamable_http_server::{session::local::LocalSessionManager, tower::StreamableHttpService},
    StreamableHttpServerConfig,
};
use rmcp::ServiceExt;
use hive_memory::{server::SharedMemoryServer, storage::Storage};
use std::{net::SocketAddr, path::PathBuf, sync::Arc};
use tracing_subscriber::{fmt, EnvFilter};

struct TokenStore(Vec<String>);

impl TokenStore {
    fn is_valid(&self, token: &str) -> bool {
        self.0.contains(&token.to_string())
    }
}

async fn auth_middleware(
    State(store): State<Arc<Option<TokenStore>>>,
    headers: HeaderMap,
    request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    if let Some(store) = store.as_ref() {
        let auth = headers.get("Authorization").and_then(|h| h.to_str().ok());
        if let Some(auth) = auth {
            if let Some(token) = auth.strip_prefix("Bearer ") {
                if store.is_valid(token.trim()) {
                    let token_str = token.trim().to_string();
                    return Ok(hive_memory::storage::AUTH_BEARER
                        .scope(token_str, async { next.run(request).await })
                        .await);
                }
            }
        }
        tracing::warn!("Rejecting unauthorized request");
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(next.run(request).await)
}

async fn index() -> Html<&'static str> {
    Html(
        r#"<!DOCTYPE html>
<html>
<head><title>Shared Agents Memory (MCP)</title></head>
<body>
    <h1>Shared Agents Memory Server</h1>
    <p>This is an MCP server running over HTTP transport.</p>
    <p>Connection endpoint: <code>/mcp</code></p>
</body>
</html>"#,
    )
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut project_root = std::env::current_dir().ok();
    dotenvy::dotenv_override().ok();
    
    if std::env::var("MEMORY_DB_PATH").is_err() {
        if let Ok(exe_path) = std::env::current_exe() {
            let mut curr = exe_path.parent();
            while let Some(p) = curr {
                let env_path = p.join(".env");
                if env_path.exists() {
                    dotenvy::from_path_override(env_path).ok();
                    project_root = Some(p.to_path_buf());
                    break;
                }
                if p.join("Cargo.toml").exists() {
                    project_root = Some(p.to_path_buf());
                    break;
                }
                curr = p.parent();
            }
        }
    }

    fmt()
        .with_env_filter(
            EnvFilter::try_from_env("RUST_LOG").unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();

    let db_path_raw = std::env::var("MEMORY_DB_PATH").ok();
    let db_path = if let Some(path_str) = db_path_raw {
        let p = PathBuf::from(path_str);
        if p.is_relative() {
            if let Some(root) = project_root {
                root.join(p)
            } else {
                p
            }
        } else {
            p
        }
    } else {
        dirs_sys::home_dir()
            .unwrap_or_else(|| PathBuf::from("/tmp"))
            .join(".hive-memory")
            .join("db")
    };

    std::fs::create_dir_all(&db_path)?;
    tracing::info!("Database path: {:?}", db_path);

    let storage = Storage::open(&db_path)?;
    let handler = SharedMemoryServer::new(storage);

    let transport_mode = std::env::var("MEMORY_TRANSPORT").unwrap_or_else(|_| "stdio".to_string());

    if transport_mode == "http" {
        let port = std::env::var("MEMORY_HTTP_PORT")
            .unwrap_or_else(|_| "8080".to_string())
            .parse::<u16>()
            .context("Invalid MEMORY_HTTP_PORT")?;

        let api_keys_raw = std::env::var("MEMORY_API_KEYS").unwrap_or_default();
        let token_store = if !api_keys_raw.is_empty() {
            let tokens: Vec<String> = api_keys_raw
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            tracing::info!("Auth enabled. Loaded {} valid API keys.", tokens.len());
            Some(TokenStore(tokens))
        } else {
            tracing::warn!("Auth DISABLED. The /mcp endpoint is publicly accessible!");
            None
        };
        let token_store = Arc::new(token_store);

        let addr = SocketAddr::from(([0, 0, 0, 0], port));
        tracing::info!(
            "hive-memory MCP server v{} starting on http://{}",
            env!("CARGO_PKG_VERSION"),
            addr
        );

        let mcp_service: StreamableHttpService<SharedMemoryServer, LocalSessionManager> =
            StreamableHttpService::new(
                move || Ok(handler.clone()),
                LocalSessionManager::default().into(),
                StreamableHttpServerConfig::default(),
            );

        let protected_mcp_router =
            Router::new()
                .nest_service("/mcp", mcp_service)
                .layer(middleware::from_fn_with_state(
                    token_store.clone(),
                    auth_middleware,
                ));

        let app = Router::new()
            .route("/", get(index))
            .merge(protected_mcp_router);

        let listener = tokio::net::TcpListener::bind(addr).await?;
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                tokio::signal::ctrl_c().await.ok();
                tracing::info!("Shutting down server...");
            })
            .await?;
    } else {
        tracing::info!(
            "hive-memory MCP server v{} starting on stdio",
            env!("CARGO_PKG_VERSION")
        );

        let transport = rmcp::transport::io::stdio();
        let running = handler.serve(transport).await?;

        tracing::info!("Server ready");
        running.waiting().await?;
    }

    Ok(())
}
