use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::{IntoParams, ToSchema};

#[derive(Clone, Copy, Debug, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Running,
    Cancelling,
    Cancelled,
    Failed,
    Succeeded,
}

#[derive(Deserialize, Serialize, ToSchema)]
pub struct RunSummary {
    pub id: String,
    pub workflow: String,
    pub version: String,
    pub status: Status,
    /// Unix timestamp in seconds.
    pub started_at: f64,
    pub finished_at: Option<f64>,
    pub parent_id: Option<String>,
    pub parent_node: Option<String>,
    pub item_index: Option<u64>,
    pub item_summary: Option<String>,
    pub rerun_of: Option<String>,
    pub node_id: Option<String>,
    pub error: Option<String>,
    pub wake_at: Option<f64>,
}

#[derive(Deserialize, Serialize, ToSchema)]
pub struct RunPage {
    pub items: Vec<RunSummary>,
    pub total: u64,
}

#[derive(Deserialize, Serialize, ToSchema)]
pub struct GraphNode {
    pub id: String,
    pub kind: String,
    pub label: String,
}

#[derive(Deserialize, Serialize, ToSchema)]
pub struct GraphEdge {
    pub src: String,
    pub dst: String,
    pub label: Option<String>,
}

#[derive(Deserialize, Serialize, ToSchema)]
pub struct Graph {
    pub nodes: Vec<GraphNode>,
    pub edges: Vec<GraphEdge>,
}

#[derive(Deserialize, Serialize, ToSchema)]
pub struct Preview {
    pub value: Value,
    pub truncated: bool,
    pub original_bytes: u64,
    pub omitted_root: u64,
}

#[derive(Deserialize, Serialize, ToSchema)]
pub struct Attempt {
    pub node_id: String,
    pub attempt: u64,
    pub status: String,
    pub started_at: Option<f64>,
    pub finished_at: Option<f64>,
    pub due: Option<f64>,
    pub deadline: Option<f64>,
    pub error: Option<String>,
    pub arm: Option<String>,
    pub preview: Option<Preview>,
    pub input_preview: Option<Preview>,
}

#[derive(Deserialize, Serialize, ToSchema)]
pub struct RunDetail {
    pub run: RunSummary,
    pub graph: Graph,
    pub attempts: Vec<Attempt>,
    pub input: Value,
    pub result: Option<Value>,
    pub input_json: Option<String>,
    pub comparison: Option<Comparison>,
}

#[derive(Deserialize, Serialize, ToSchema)]
pub struct Comparison {
    pub source_status: Status,
    pub source_failure: Option<String>,
    pub source_node: Option<String>,
    pub input_changed: bool,
    pub input_changes: Vec<InputChange>,
    pub more_input_changes: bool,
    pub definition_changed: bool,
    pub steps: Vec<StepChange>,
}

#[derive(Deserialize, Serialize, ToSchema)]
pub struct InputChange {
    pub path: String,
    /// JSON text, or absent when the field did not exist.
    pub before: Option<String>,
    pub after: Option<String>,
}

#[derive(Deserialize, Serialize, ToSchema)]
pub struct StepChange {
    pub node: String,
    pub before: Option<Attempt>,
    pub after: Option<Attempt>,
}

#[derive(Deserialize, Serialize, ToSchema)]
pub struct WorkflowSummary {
    pub name: String,
    pub version: String,
    pub input_type: String,
    pub output_type: String,
    pub launchable: bool,
    pub launch_reason: Option<String>,
    pub latest: Option<RunSummary>,
}

#[derive(Deserialize, Serialize, ToSchema)]
pub struct WorkflowList {
    pub items: Vec<WorkflowSummary>,
}

#[derive(Deserialize, Serialize, ToSchema)]
pub struct NodeConfig {
    pub id: String,
    pub callable: Option<String>,
    pub timeout: Option<String>,
    pub retry: Option<String>,
}

#[derive(Deserialize, Serialize, ToSchema)]
pub struct WorkflowDetail {
    pub workflow: WorkflowSummary,
    pub graph: Graph,
    pub definition: Value,
    pub example_input: Value,
    pub example_note: Option<String>,
    #[serde(default)]
    pub input_fields: Vec<InputField>,
    pub nodes: Vec<NodeConfig>,
}

#[derive(Deserialize, Serialize, IntoParams)]
#[into_params(parameter_in = Query)]
#[serde(deny_unknown_fields)]
pub struct RunQuery {
    pub rerun_of: Option<String>,
    pub q: Option<String>,
    pub status: Option<Status>,
    pub workflow: Option<String>,
    /// Earliest start time, in Unix seconds.
    pub since: Option<f64>,
    /// Latest start time, in Unix seconds.
    pub until: Option<f64>,
    pub offset: Option<u64>,
    pub limit: Option<u32>,
}

#[derive(Deserialize, Serialize, IntoParams)]
#[into_params(parameter_in = Query)]
#[serde(deny_unknown_fields)]
pub struct ChildrenQuery {
    pub node: Option<String>,
    pub status: Option<Status>,
    pub offset: Option<u64>,
    pub limit: Option<u32>,
}

#[derive(Deserialize, Serialize, ToSchema)]
pub struct InputField {
    pub name: String,
    pub kind: String,
    pub required: bool,
}
#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct LaunchRequest {
    pub input: Value,
}

#[derive(Deserialize, ToSchema)]
#[serde(untagged)]
pub enum RerunRequest {
    Edited(LaunchRequest),
    Saved(SavedInput),
}
#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SavedInput {}

#[derive(Deserialize, Serialize, ToSchema)]
pub struct LaunchResponse {
    pub run_id: String,
}

#[derive(Deserialize, Serialize, ToSchema)]
pub struct ErrorBody {
    pub detail: String,
}
