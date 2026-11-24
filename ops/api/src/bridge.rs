use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::ffi::OsString;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::{mpsc, oneshot};

use crate::model::ErrorBody;

#[derive(Debug)]
pub struct ApiError(pub StatusCode, pub String);

impl ApiError {
    fn unavailable() -> Self {
        Self(
            StatusCode::SERVICE_UNAVAILABLE,
            "Workflow adapter is unavailable".into(),
        )
    }

    fn invalid_response() -> Self {
        Self(
            StatusCode::BAD_GATEWAY,
            "Invalid response from workflow adapter".into(),
        )
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(ErrorBody { detail: self.1 })).into_response()
    }
}

type Reply = Result<Value, ApiError>;

struct Call {
    op: &'static str,
    args: Value,
    reply: oneshot::Sender<Reply>,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum AdapterReply {
    Ok(Value),
    Error { status: u16, detail: String },
}

#[derive(Clone)]
pub struct Bridge {
    requests: mpsc::Sender<Call>,
}

impl Bridge {
    pub fn start() -> io::Result<Self> {
        let default_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let root = std::env::var_os("SPATER_OPS_ROOT")
            .map(PathBuf::from)
            .unwrap_or(default_root)
            .canonicalize()?;
        let mut paths = vec![root.join("examples")];
        if let Some(existing) = std::env::var_os("PYTHONPATH") {
            paths.extend(std::env::split_paths(&existing));
        }
        let pythonpath: OsString = std::env::join_paths(paths).map_err(io::Error::other)?;
        let mut command = Command::new("uv");
        command
            .current_dir(root)
            .env("PYTHONPATH", pythonpath)
            .args(["run", "--no-sync", "python", "-m", "spater.flow._ops"]);
        Self::spawn(command)
    }

    pub fn spawn(mut command: Command) -> io::Result<Self> {
        let mut child = command
            .kill_on_drop(true)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        let mut input = child.stdin.take().expect("piped adapter stdin");
        let mut output = BufReader::new(child.stdout.take().expect("piped adapter stdout"));
        let (requests, mut pending) = mpsc::channel::<Call>(64);
        // The actor owns both halves of the stream. A disconnected HTTP client
        // can drop its reply receiver without cancelling an in-flight read.
        tokio::spawn(async move {
            while let Some(call) = pending.recv().await {
                if call.reply.is_closed() {
                    continue;
                }
                let mut frame = serde_json::to_vec(&json!({ "op": call.op, "args": call.args }))
                    .expect("serializing a JSON value");
                frame.push(b'\n');
                let mut line = String::new();
                let exchange = async {
                    input.write_all(&frame).await?;
                    input.flush().await?;
                    output.read_line(&mut line).await
                }
                .await;
                let reply = match exchange {
                    Ok(0) | Err(_) => Err(ApiError::unavailable()),
                    Ok(_) => serde_json::from_str::<AdapterReply>(&line)
                        .map_err(|_| ApiError::invalid_response()),
                };
                match reply {
                    Ok(AdapterReply::Ok(value)) => {
                        let _ = call.reply.send(Ok(value));
                    }
                    Ok(AdapterReply::Error { status, detail }) => {
                        let error = match StatusCode::from_u16(status) {
                            Ok(code) if code.is_client_error() => ApiError(code, detail),
                            _ => ApiError(
                                StatusCode::INTERNAL_SERVER_ERROR,
                                "Workflow operation failed".into(),
                            ),
                        };
                        let _ = call.reply.send(Err(error));
                    }
                    Err(error) => {
                        eprintln!("Workflow adapter connection closed during {}", call.op);
                        let _ = call.reply.send(Err(error));
                        break;
                    }
                }
            }
            drop(input);
            let _ = child.kill().await;
        });
        Ok(Self { requests })
    }

    pub async fn call<T: DeserializeOwned>(
        &self,
        op: &'static str,
        args: Value,
    ) -> Result<T, ApiError> {
        let (reply, response) = oneshot::channel();
        self.requests
            .try_send(Call { op, args, reply })
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => ApiError(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "Workflow adapter is busy".into(),
                ),
                mpsc::error::TrySendError::Closed(_) => ApiError::unavailable(),
            })?;
        let payload = response.await.map_err(|_| ApiError::unavailable())??;
        serde_json::from_value(payload).map_err(|e| {
            eprintln!("Unexpected {op} response: {:?}", e.classify());
            ApiError::invalid_response()
        })
    }
}
