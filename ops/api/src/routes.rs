use axum::extract::{Path, Query, Request, State};
use axum::http::{HeaderValue, Method, StatusCode, header, uri::Authority};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Serialize;
use serde_json::{Value, json};
use std::net::IpAddr;
use utoipa::OpenApi;

use crate::bridge::{ApiError, Bridge};
use crate::model::*;

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Spater Ops",
        description = "Local workflow inspection and execution API.",
        version = "1.0.0"
    ),
    paths(
        list_runs,
        get_run,
        list_children,
        list_workflows,
        get_workflow,
        launch_workflow,
        rerun
    ),
    components(schemas(
        Status,
        RunSummary,
        RunPage,
        Graph,
        GraphNode,
        GraphEdge,
        Attempt,
        Preview,
        RunDetail,
        WorkflowSummary,
        WorkflowList,
        WorkflowDetail,
        NodeConfig,
        LaunchRequest,
        RerunRequest,
        SavedInput,
        InputField,
        Comparison,
        StepChange,
        InputChange,
        LaunchResponse,
        ErrorBody
    ))
)]
pub struct Api;

type Read<T> = Result<Json<T>, ApiError>;
type Accepted = Result<(StatusCode, Json<LaunchResponse>), ApiError>;

pub fn router(bridge: Bridge) -> Router {
    Router::new()
        .route("/openapi.json", get(async || Json(Api::openapi())))
        .route("/api/v1/runs", get(list_runs))
        .route("/api/v1/runs/{id}", get(get_run))
        .route("/api/v1/runs/{id}/children", get(list_children))
        .route("/api/v1/runs/{id}/rerun", post(rerun))
        .route("/api/v1/workflows", get(list_workflows))
        .route("/api/v1/workflows/{name}", get(get_workflow))
        .route("/api/v1/workflows/{name}/launch", post(launch_workflow))
        .layer(middleware::from_fn(local_request))
        .with_state(bridge)
}

fn query_args(query: impl Serialize) -> Value {
    let mut args = serde_json::to_value(query).expect("serializable query");
    args.as_object_mut()
        .expect("query object")
        .retain(|_, value| !value.is_null());
    args
}

#[utoipa::path(get, path="/api/v1/runs", operation_id="listRuns", tag="runs",
    params(RunQuery), responses((status=200, body=RunPage), (status=400, body=ErrorBody), (status=503, body=ErrorBody)))]
async fn list_runs(State(bridge): State<Bridge>, Query(query): Query<RunQuery>) -> Read<RunPage> {
    if [query.since, query.until]
        .into_iter()
        .flatten()
        .any(|v| !v.is_finite())
    {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "Time filters must be finite Unix timestamps".into(),
        ));
    }
    Ok(Json(bridge.call("list_runs", query_args(query)).await?))
}

#[utoipa::path(get, path="/api/v1/runs/{id}", operation_id="getRun", tag="runs",
    params(("id"=String, Path)), responses((status=200, body=RunDetail), (status=404, body=ErrorBody)))]
async fn get_run(State(bridge): State<Bridge>, Path(id): Path<String>) -> Read<RunDetail> {
    Ok(Json(bridge.call("run", json!({ "id": id })).await?))
}

#[utoipa::path(get, path="/api/v1/runs/{id}/children", operation_id="listChildren", tag="runs",
    params(("id"=String, Path), ChildrenQuery),
    responses((status=200, body=RunPage), (status=400, body=ErrorBody), (status=404, body=ErrorBody)))]
async fn list_children(
    State(bridge): State<Bridge>,
    Path(id): Path<String>,
    Query(query): Query<ChildrenQuery>,
) -> Read<RunPage> {
    let mut args = query_args(query);
    args["id"] = Value::String(id);
    Ok(Json(bridge.call("children", args).await?))
}

#[utoipa::path(get, path="/api/v1/workflows", operation_id="listWorkflows", tag="workflows",
    responses((status=200, body=WorkflowList), (status=503, body=ErrorBody)))]
async fn list_workflows(State(bridge): State<Bridge>) -> Read<WorkflowList> {
    Ok(Json(bridge.call("workflows", json!({})).await?))
}

#[utoipa::path(get, path="/api/v1/workflows/{name}", operation_id="getWorkflow", tag="workflows",
    params(("name"=String, Path)), responses((status=200, body=WorkflowDetail), (status=404, body=ErrorBody)))]
async fn get_workflow(
    State(bridge): State<Bridge>,
    Path(name): Path<String>,
) -> Read<WorkflowDetail> {
    Ok(Json(
        bridge.call("workflow", json!({ "name": name })).await?,
    ))
}

#[utoipa::path(post, path="/api/v1/workflows/{name}/launch", operation_id="launchWorkflow", tag="workflows",
    params(("name"=String, Path)), request_body=LaunchRequest,
    responses((status=202, body=LaunchResponse), (status=400, body=ErrorBody), (status=403, body=ErrorBody),
        (status=404, body=ErrorBody), (status=409, body=ErrorBody), (status=422, body=ErrorBody)))]
async fn launch_workflow(
    State(bridge): State<Bridge>,
    Path(name): Path<String>,
    Json(req): Json<LaunchRequest>,
) -> Accepted {
    let result = bridge
        .call("launch", json!({ "name": name, "input": req.input }))
        .await?;
    Ok((StatusCode::ACCEPTED, Json(result)))
}

#[utoipa::path(post, path="/api/v1/runs/{id}/rerun", operation_id="rerun", tag="runs",
    params(("id"=String, Path)), request_body=RerunRequest,
    responses((status=202, body=LaunchResponse), (status=400, body=ErrorBody), (status=403, body=ErrorBody),
        (status=404, body=ErrorBody), (status=409, body=ErrorBody), (status=422, body=ErrorBody)))]
async fn rerun(
    State(bridge): State<Bridge>,
    Path(id): Path<String>,
    Json(req): Json<RerunRequest>,
) -> Accepted {
    let args = match req {
        RerunRequest::Edited(edited) => json!({"id": id, "input": edited.input}),
        RerunRequest::Saved(_) => json!({"id": id}),
    };
    let result = bridge.call("rerun", args).await?;
    Ok((StatusCode::ACCEPTED, Json(result)))
}

fn local_host(authority: &Authority) -> bool {
    let host = authority
        .host()
        .trim_start_matches('[')
        .trim_end_matches(']');
    !authority.as_str().contains('@')
        && (host.eq_ignore_ascii_case("localhost")
            || host.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback()))
}

async fn local_request(request: Request, next: Next) -> Response {
    let authority = request
        .headers()
        .get(header::HOST)
        .and_then(|v| v.to_str().ok()?.parse::<Authority>().ok())
        .or_else(|| request.uri().authority().cloned());
    let Some(authority) = authority.filter(local_host) else {
        return ApiError(StatusCode::FORBIDDEN, "A loopback Host is required".into())
            .into_response();
    };
    if request.method() == Method::POST {
        let bad_origin = request.headers().get(header::ORIGIN).is_some_and(|value| {
            value
                .to_str()
                .ok()
                .and_then(|origin| origin.strip_prefix("http://"))
                .is_none_or(|host| !host.eq_ignore_ascii_case(authority.as_str()))
        });
        let cross_site = request
            .headers()
            .get("sec-fetch-site")
            .is_some_and(|site| site == "cross-site");
        if bad_origin || cross_site {
            return ApiError(
                StatusCode::FORBIDDEN,
                "Cross-origin workflow changes are forbidden".into(),
            )
            .into_response();
        }
    }
    let response = next.run(request).await;
    let status = response.status();
    let is_json = response
        .headers()
        .get(header::CONTENT_TYPE)
        .is_some_and(|v| v == "application/json");
    let mut response = if status.is_client_error() && !is_json {
        ApiError(
            status,
            status
                .canonical_reason()
                .unwrap_or("Invalid request")
                .into(),
        )
        .into_response()
    } else {
        response
    };
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}
