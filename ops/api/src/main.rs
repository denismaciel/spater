mod bridge;
mod model;
mod routes;
#[cfg(test)]
mod tests;

use std::io;
use std::net::SocketAddr;
use utoipa::OpenApi;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    match std::env::args().nth(1).as_deref() {
        Some("openapi") => {
            println!("{}", routes::Api::openapi().to_pretty_json()?);
            return Ok(());
        }
        None | Some("serve") => {}
        _ => return Err("usage: spater-ops-api [serve|openapi]".into()),
    }
    let address: SocketAddr = std::env::var("SPATER_OPS_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:8410".into())
        .parse()?;
    if !address.ip().is_loopback() {
        return Err(io::Error::other("Spater Ops requires a loopback listen address").into());
    }
    let listener = tokio::net::TcpListener::bind(address).await?;
    let bridge = bridge::Bridge::start()?;
    eprintln!(
        "Spater Ops API listening at http://{}",
        listener.local_addr()?
    );
    axum::serve(listener, routes::router(bridge))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
