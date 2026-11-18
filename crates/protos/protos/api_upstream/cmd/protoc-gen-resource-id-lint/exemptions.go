package main

// exemptions lists RPCs that intentionally have no temporal-resource-id annotation, keyed by
// "<Service>.<Method>". Every other RPC must have the annotation. Only exempt an RPC that has no
// single resource to route by.
var exemptions = map[string]struct{}{
	"WorkflowService.RegisterNamespace":  {},
	"WorkflowService.DescribeNamespace":  {},
	"WorkflowService.ListNamespaces":     {},
	"WorkflowService.UpdateNamespace":    {},
	"WorkflowService.DeprecateNamespace": {},

	"WorkflowService.GetClusterInfo":      {},
	"WorkflowService.GetSystemInfo":       {},
	"WorkflowService.GetSearchAttributes": {},

	"WorkflowService.ListOpenWorkflowExecutions":     {},
	"WorkflowService.ListClosedWorkflowExecutions":   {},
	"WorkflowService.ListWorkflowExecutions":         {},
	"WorkflowService.ListArchivedWorkflowExecutions": {},
	"WorkflowService.ScanWorkflowExecutions":         {},
	"WorkflowService.CountWorkflowExecutions":        {},
	"WorkflowService.ListSchedules":                  {},
	"WorkflowService.CountSchedules":                 {},
	"WorkflowService.ListBatchOperations":            {},
	"WorkflowService.ListWorkers":                    {},
	"WorkflowService.CountWorkers":                   {},
	"WorkflowService.ListActivityExecutions":         {},
	"WorkflowService.CountActivityExecutions":        {},
	"WorkflowService.ListNexusOperationExecutions":   {},
	"WorkflowService.CountNexusOperationExecutions":  {},
	"WorkflowService.ListWorkerDeployments":          {},
	"WorkflowService.ListDeployments":                {},

	"WorkflowService.CreateWorkflowRule":   {},
	"WorkflowService.DescribeWorkflowRule": {},
	"WorkflowService.DeleteWorkflowRule":   {},
	"WorkflowService.ListWorkflowRules":    {},

	"WorkflowService.ShutdownWorker": {},

	"WorkflowService.GetWorkerTaskReachability": {},

	"WorkflowService.DescribeDeployment":        {},
	"WorkflowService.GetDeploymentReachability": {},
	"WorkflowService.GetCurrentDeployment":      {},
	"WorkflowService.SetCurrentDeployment":      {},

	"OperatorService.AddSearchAttributes":      {},
	"OperatorService.RemoveSearchAttributes":   {},
	"OperatorService.ListSearchAttributes":     {},
	"OperatorService.DeleteNamespace":          {},
	"OperatorService.SoftDeleteNamespace":      {},
	"OperatorService.RestoreNamespace":         {},
	"OperatorService.DescribeNamespaceRestore": {},

	"OperatorService.AddOrUpdateRemoteCluster": {},
	"OperatorService.RemoveRemoteCluster":      {},
	"OperatorService.ListClusters":             {},

	"OperatorService.GetNexusEndpoint":    {},
	"OperatorService.CreateNexusEndpoint": {},
	"OperatorService.UpdateNexusEndpoint": {},
	"OperatorService.DeleteNexusEndpoint": {},
	"OperatorService.ListNexusEndpoints":  {},
}
