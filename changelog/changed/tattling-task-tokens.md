Workflow task completions sent to the server identify their workflow in `resource_id` as `workflow:<workflow_id>`, so proxies in front of Temporal can tell which workflow a completion is for.
