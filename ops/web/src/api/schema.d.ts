// Generated from the Rust operations API. Run npm run gen:openapi.
export interface paths {
    "/api/v1/runs": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get: operations["listRuns"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/runs/{id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get: operations["getRun"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/runs/{id}/children": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get: operations["listChildren"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/runs/{id}/rerun": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        post: operations["rerun"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/workflows": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get: operations["listWorkflows"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/workflows/{name}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get: operations["getWorkflow"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/workflows/{name}/launch": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        post: operations["launchWorkflow"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
}
export type webhooks = Record<string, never>;
export interface components {
    schemas: {
        Attempt: {
            arm?: string | null;
            /** Format: int64 */
            attempt: number;
            /** Format: double */
            deadline?: number | null;
            /** Format: double */
            due?: number | null;
            error?: string | null;
            /** Format: double */
            finished_at?: number | null;
            input_preview?: null | components["schemas"]["Preview"];
            node_id: string;
            preview?: null | components["schemas"]["Preview"];
            /** Format: double */
            started_at?: number | null;
            status: string;
        };
        Comparison: {
            definition_changed: boolean;
            input_changed: boolean;
            input_changes: components["schemas"]["InputChange"][];
            more_input_changes: boolean;
            source_failure?: string | null;
            source_node?: string | null;
            source_status: components["schemas"]["Status"];
            steps: components["schemas"]["StepChange"][];
        };
        ErrorBody: {
            detail: string;
        };
        Graph: {
            edges: components["schemas"]["GraphEdge"][];
            nodes: components["schemas"]["GraphNode"][];
        };
        GraphEdge: {
            dst: string;
            label?: string | null;
            src: string;
        };
        GraphNode: {
            id: string;
            kind: string;
            label: string;
        };
        InputChange: {
            after?: string | null;
            /** @description JSON text, or absent when the field did not exist. */
            before?: string | null;
            path: string;
        };
        InputField: {
            kind: string;
            name: string;
            required: boolean;
        };
        LaunchRequest: {
            input: unknown;
        };
        LaunchResponse: {
            run_id: string;
        };
        NodeConfig: {
            callable?: string | null;
            id: string;
            retry?: string | null;
            timeout?: string | null;
        };
        Preview: {
            /** Format: int64 */
            omitted_root: number;
            /** Format: int64 */
            original_bytes: number;
            truncated: boolean;
            value: unknown;
        };
        RerunRequest: components["schemas"]["LaunchRequest"] | components["schemas"]["SavedInput"];
        RunDetail: {
            attempts: components["schemas"]["Attempt"][];
            comparison?: null | components["schemas"]["Comparison"];
            graph: components["schemas"]["Graph"];
            input: unknown;
            input_json?: string | null;
            result?: unknown;
            run: components["schemas"]["RunSummary"];
        };
        RunPage: {
            items: components["schemas"]["RunSummary"][];
            /** Format: int64 */
            total: number;
        };
        RunSummary: {
            error?: string | null;
            /** Format: double */
            finished_at?: number | null;
            id: string;
            /** Format: int64 */
            item_index?: number | null;
            item_summary?: string | null;
            node_id?: string | null;
            parent_id?: string | null;
            parent_node?: string | null;
            rerun_of?: string | null;
            /**
             * Format: double
             * @description Unix timestamp in seconds.
             */
            started_at: number;
            status: components["schemas"]["Status"];
            version: string;
            /** Format: double */
            wake_at?: number | null;
            workflow: string;
        };
        SavedInput: Record<string, never>;
        /** @enum {string} */
        Status: "running" | "cancelling" | "cancelled" | "failed" | "succeeded";
        StepChange: {
            after?: null | components["schemas"]["Attempt"];
            before?: null | components["schemas"]["Attempt"];
            node: string;
        };
        WorkflowDetail: {
            definition: unknown;
            example_input: unknown;
            example_note?: string | null;
            graph: components["schemas"]["Graph"];
            input_fields?: components["schemas"]["InputField"][];
            nodes: components["schemas"]["NodeConfig"][];
            workflow: components["schemas"]["WorkflowSummary"];
        };
        WorkflowList: {
            items: components["schemas"]["WorkflowSummary"][];
        };
        WorkflowSummary: {
            input_type: string;
            latest?: null | components["schemas"]["RunSummary"];
            launch_reason?: string | null;
            launchable: boolean;
            name: string;
            output_type: string;
            version: string;
        };
    };
    responses: never;
    parameters: never;
    requestBodies: never;
    headers: never;
    pathItems: never;
}
export type $defs = Record<string, never>;
export interface operations {
    listRuns: {
        parameters: {
            query?: {
                rerun_of?: string;
                q?: string;
                status?: components["schemas"]["Status"];
                workflow?: string;
                /** @description Earliest start time, in Unix seconds. */
                since?: number;
                /** @description Latest start time, in Unix seconds. */
                until?: number;
                offset?: number;
                limit?: number;
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["RunPage"];
                };
            };
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    getRun: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["RunDetail"];
                };
            };
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    listChildren: {
        parameters: {
            query?: {
                node?: string;
                status?: components["schemas"]["Status"];
                offset?: number;
                limit?: number;
            };
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["RunPage"];
                };
            };
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    rerun: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["RerunRequest"];
            };
        };
        responses: {
            202: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["LaunchResponse"];
                };
            };
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    listWorkflows: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["WorkflowList"];
                };
            };
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    getWorkflow: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                name: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["WorkflowDetail"];
                };
            };
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    launchWorkflow: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                name: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["LaunchRequest"];
            };
        };
        responses: {
            202: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["LaunchResponse"];
                };
            };
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
}
