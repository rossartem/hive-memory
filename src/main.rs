use anyhow::{Context, Result};
use axum::{Router, response::Html, routing::get};
use hive_memory::{server::SharedMemoryServer, storage::Storage};
use rmcp::ServiceExt;
use rmcp::transport::{
    StreamableHttpServerConfig,
    streamable_http_server::{session::local::LocalSessionManager, tower::StreamableHttpService},
};
use std::{net::SocketAddr, path::PathBuf};
use tracing_subscriber::{EnvFilter, fmt};

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

    if std::env::var("MEMORY_DB_PATH").is_err()
        && let Ok(exe_path) = std::env::current_exe() {
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

        let app = Router::new()
            .route("/", get(index))
            .nest_service("/mcp", mcp_service);

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
