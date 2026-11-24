use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Method, Request, StatusCode};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::process::Command;
use tokio::time::timeout;
use tower::ServiceExt;
use utoipa::OpenApi;

use crate::bridge::Bridge;
use crate::routes::{Api, router};

fn adapter(script: &str, args: &[&str]) -> Bridge {
    let mut command = Command::new("uv");
    command
        .args(["run", "--no-sync", "python", "-u", "-c", script])
        .args(args);
    Bridge::spawn(command).unwrap()
}

async fn send(
    app: &Router,
    method: Method,
    path: &str,
    headers: &[(&str, &str)],
    data: &str,
) -> (StatusCode, Value) {
    let mut request = Request::builder().method(method).uri(path);
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    let response = app
        .clone()
        .oneshot(request.body(Body::from(data.to_owned())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 2 * 1024 * 1024)
        .await
        .unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
async fn routes_forward_typed_arguments_and_preserve_http_errors() {
    const SCRIPT: &str = r#"
import json, sys
for line in sys.stdin:
    request = json.loads(line)
    op, args = request['op'], request['args']
    if op == 'list_runs' and args == {'q': 'hello world', 'status': 'running', 'offset': 4}:
        reply = {'ok': {'items': [], 'total': 6}}
    elif op == 'launch' and args == {'name': 'demo', 'input': [False, 123456789012345678901234567890]}:
        reply = {'ok': {'run_id': 'new-id'}}
    elif op == 'rerun' and args == {'id': 'missing'}:
        reply = {'error': {'status': 404, 'detail': 'Run missing'}}
    elif op == 'run' and args == {'id': 'broken'}:
        reply = {'error': {'status': 500, 'detail': 'internal service credentials'}}
    else:
        reply = {'error': {'status': 400, 'detail': 'Unexpected operation'}}
    print(json.dumps(reply), flush=True)
"#;
    let app = router(adapter(SCRIPT, &[]));
    let local = [("host", "127.0.0.1:8410")];
    let same_origin = [
        local[0],
        ("content-type", "application/json"),
        ("origin", "http://127.0.0.1:8410"),
    ];
    let (status, spec) = send(&app, Method::GET, "/openapi.json", &local, "").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(spec, serde_json::to_value(Api::openapi()).unwrap());
    assert_eq!(spec["paths"].as_object().unwrap().len(), 7);
    assert_eq!(
        send(
            &app,
            Method::GET,
            "/api/v1/runs?q=hello+world&status=running&offset=4",
            &local,
            ""
        )
        .await,
        (StatusCode::OK, json!({"items": [], "total": 6})),
    );
    assert_eq!(
        send(
            &app,
            Method::POST,
            "/api/v1/workflows/demo/launch",
            &same_origin,
            r#"{"input":[false,123456789012345678901234567890]}"#
        )
        .await,
        (StatusCode::ACCEPTED, json!({"run_id": "new-id"})),
    );
    assert_eq!(
        send(
            &app,
            Method::POST,
            "/api/v1/runs/missing/rerun",
            &same_origin,
            "{}"
        )
        .await,
        (StatusCode::NOT_FOUND, json!({"detail": "Run missing"})),
    );
    assert_eq!(
        send(&app, Method::GET, "/api/v1/runs/broken", &local, "").await,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"detail": "Workflow operation failed"})
        ),
    );
    for (path, expected) in [
        ("/api/v1/runs?status=waiting", StatusCode::BAD_REQUEST),
        ("/api/v1/runs?since=NaN", StatusCode::BAD_REQUEST),
        ("/missing", StatusCode::NOT_FOUND),
    ] {
        let (status, error) = send(&app, Method::GET, path, &local, "").await;
        assert_eq!(status, expected);
        assert!(error["detail"].is_string());
    }
    let path = "/api/v1/workflows/demo/launch";
    assert_eq!(
        send(&app, Method::POST, path, &same_origin, "{}").await.0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(
        send(&app, Method::POST, path, &local, "{}").await.0,
        StatusCode::UNSUPPORTED_MEDIA_TYPE
    );
    for headers in [
        vec![local[0], ("origin", "https://example.org")],
        vec![local[0], ("origin", "https://127.0.0.1:8410")],
        vec![local[0], ("sec-fetch-site", "cross-site")],
        vec![("host", "rebind.example")],
    ] {
        assert_eq!(
            send(&app, Method::POST, path, &headers, "{}").await.0,
            StatusCode::FORBIDDEN
        );
    }
}

#[tokio::test]
async fn abandoned_inflight_request_cannot_consume_the_next_reply() {
    const SCRIPT: &str = r#"
import json, socket, sys
for line in sys.stdin:
    request = json.loads(line)
    if request['op'] == 'blocked':
        with socket.create_connection(('127.0.0.1', int(sys.argv[1]))) as gate:
            gate.recv(1)
    print(json.dumps({'ok': request['args']['index']}), flush=True)
"#;
    timeout(Duration::from_secs(10), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bridge = adapter(
            SCRIPT,
            &[&listener.local_addr().unwrap().port().to_string()],
        );
        let blocked = bridge.clone();
        let pending =
            tokio::spawn(async move { blocked.call::<u64>("blocked", json!({"index":0})).await });
        let (mut gate, _) = listener.accept().await.unwrap();
        pending.abort();
        assert!(pending.await.unwrap_err().is_cancelled());
        let mut calls = Vec::new();
        for index in 1..=8 {
            let bridge = bridge.clone();
            calls.push(tokio::spawn(async move {
                assert_eq!(
                    bridge
                        .call::<u64>("echo", json!({"index":index}))
                        .await
                        .unwrap(),
                    index
                );
            }));
        }
        gate.write_all(b"1").await.unwrap();
        for call in calls {
            call.await.unwrap();
        }
    })
    .await
    .expect("workflow adapter exchanges completed");
}

#[tokio::test]
async fn malformed_protocol_closes_the_connection_and_fails_pending_work() {
    let bridge = adapter("input(); print('malformed frame')", &[]);
    let (first, second) = tokio::join!(
        bridge.call::<Value>("one", json!({})),
        bridge.call::<Value>("two", json!({}))
    );
    assert_eq!(first.unwrap_err().0, StatusCode::BAD_GATEWAY);
    assert_eq!(second.unwrap_err().0, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        bridge
            .call::<Value>("three", json!({}))
            .await
            .unwrap_err()
            .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
}
