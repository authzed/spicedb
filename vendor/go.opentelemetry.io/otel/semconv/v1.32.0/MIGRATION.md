# Semantic Convention Changes

The `go.opentelemetry.io/otel/semconv/v1.32.0` package should be a drop-in replacement for `go.opentelemetry.io/otel/semconv/v1.31.0` with the following exceptions.

## Metric packaging

All metric instrument names, descriptions, and units have been moved into dedicated packages scoped by a namespace.
Each package contains metric instrument types designed for convenience.
Existing uses of `go.opentelemetry.io/otel/semconv/v1.31.0` can migrate to using these new purpose built instruments.
Additionally, the static `Name`, `Description`, and `Unit` methods of instruments can be used to retrieve the name, description, and unit of the instrument similar to what was provided before.

Below are the new packages and the respective declarations they replace.

- `go.opentelemetry.io/otel/semconv/v1.32.0/azureconv`
  - `AzureCosmosDBClientActiveInstanceCountDescription`
  - `AzureCosmosDBClientActiveInstanceCountName`
  - `AzureCosmosDBClientActiveInstanceCountUnit`
  - `AzureCosmosDBClientOperationRequestChargeDescription`
  - `AzureCosmosDBClientOperationRequestChargeName`
  - `AzureCosmosDBClientOperationRequestChargeUnit`
- `go.opentelemetry.io/otel/semconv/v1.32.0/cicdconv`
  - `CICDPipelineRunActiveDescription`
  - `CICDPipelineRunActiveName`
  - `CICDPipelineRunActiveUnit`
  - `CICDPipelineRunDurationDescription`
  - `CICDPipelineRunDurationName`
  - `CICDPipelineRunDurationUnit`
  - `CICDPipelineRunErrorsDescription`
  - `CICDPipelineRunErrorsName`
  - `CICDPipelineRunErrorsUnit`
  - `CICDSystemErrorsDescription`
  - `CICDSystemErrorsName`
  - `CICDSystemErrorsUnit`
  - `CICDWorkerCountDescription`
  - `CICDWorkerCountName`
  - `CICDWorkerCountUnit`
- `go.opentelemetry.io/otel/semconv/v1.32.0/containerconv`
  - `ContainerCPUTimeDescription`
  - `ContainerCPUTimeName`
  - `ContainerCPUTimeUnit`
  - `ContainerCPUUsageDescription`
  - `ContainerCPUUsageName`
  - `ContainerCPUUsageUnit`
  - `ContainerDiskIoDescription`
  - `ContainerDiskIoName`
  - `ContainerDiskIoUnit`
  - `ContainerMemoryUsageDescription`
  - `ContainerMemoryUsageName`
  - `ContainerMemoryUsageUnit`
  - `ContainerNetworkIoDescription`
  - `ContainerNetworkIoName`
  - `ContainerNetworkIoUnit`
  - `ContainerUptimeDescription`
  - `ContainerUptimeName`
  - `ContainerUptimeUnit`
- `go.opentelemetry.io/otel/semconv/v1.32.0/cpuconv`
  - `CPUFrequencyDescription`
  - `CPUFrequencyName`
  - `CPUFrequencyUnit`
  - `CPUTimeDescription`
  - `CPUTimeName`
  - `CPUTimeUnit`
  - `CPUUtilizationDescription`
  - `CPUUtilizationName`
  - `CPUUtilizationUnit`
- `go.opentelemetry.io/otel/semconv/v1.32.0/dbconv`
  - `DBClientConnectionCountDescription`
  - `DBClientConnectionCountName`
  - `DBClientConnectionCountUnit`
  - `DBClientConnectionCreateTimeDescription`
  - `DBClientConnectionCreateTimeName`
  - `DBClientConnectionCreateTimeUnit`
  - `DBClientConnectionIdleMaxDescription`
  - `DBClientConnectionIdleMaxName`
  - `DBClientConnectionIdleMaxUnit`
  - `DBClientConnectionIdleMinDescription`
  - `DBClientConnectionIdleMinName`
  - `DBClientConnectionIdleMinUnit`
  - `DBClientConnectionMaxDescription`
  - `DBClientConnectionMaxName`
  - `DBClientConnectionMaxUnit`
  - `DBClientConnectionPendingRequestsDescription`
  - `DBClientConnectionPendingRequestsName`
  - `DBClientConnectionPendingRequestsUnit`
  - `DBClientConnectionTimeoutsDescription`
  - `DBClientConnectionTimeoutsName`
  - `DBClientConnectionTimeoutsUnit`
  - `DBClientConnectionUseTimeDescription`
  - `DBClientConnectionUseTimeName`
  - `DBClientConnectionUseTimeUnit`
  - `DBClientConnectionWaitTimeDescription`
  - `DBClientConnectionWaitTimeName`
  - `DBClientConnectionWaitTimeUnit`
  - `DBClientOperationDurationDescription`
  - `DBClientOperationDurationName`
  - `DBClientOperationDurationUnit`
  - `DBClientResponseReturnedRowsDescription`
  - `DBClientResponseReturnedRowsName`
  - `DBClientResponseReturnedRowsUnit`
- `go.opentelemetry.io/otel/semconv/v1.32.0/dnsconv`
  - `DNSLookupDurationDescription`
  - `DNSLookupDurationName`
  - `DNSLookupDurationUnit`
- `go.opentelemetry.io/otel/semconv/v1.32.0/faasconv`
  - `FaaSCPUUsageDescription`
  - `FaaSCPUUsageName`
  - `FaaSCPUUsageUnit`
  - `FaaSColdstartsDescription`
  - `FaaSColdstartsName`
  - `FaaSColdstartsUnit`
  - `FaaSErrorsDescription`
  - `FaaSErrorsName`
  - `FaaSErrorsUnit`
  - `FaaSInitDurationDescription`
  - `FaaSInitDurationName`
  - `FaaSInitDurationUnit`
  - `FaaSInvocationsDescription`
  - `FaaSInvocationsName`
  - `FaaSInvocationsUnit`
  - `FaaSInvokeDurationDescription`
  - `FaaSInvokeDurationName`
  - `FaaSInvokeDurationUnit`
  - `FaaSMemUsageDescription`
  - `FaaSMemUsageName`
  - `FaaSMemUsageUnit`
  - `FaaSNetIoDescription`
  - `FaaSNetIoName`
  - `FaaSNetIoUnit`
  - `FaaSTimeoutsDescription`
  - `FaaSTimeoutsName`
  - `FaaSTimeoutsUnit`
- `go.opentelemetry.io/otel/semconv/v1.32.0/genaiconv`
  - `GenAIClientOperationDurationDescription`
  - `GenAIClientOperationDurationName`
  - `GenAIClientOperationDurationUnit`
  - `GenAIClientTokenUsageDescription`
  - `GenAIClientTokenUsageName`
  - `GenAIClientTokenUsageUnit`
  - `GenAIServerRequestDurationDescription`
  - `GenAIServerRequestDurationName`
  - `GenAIServerRequestDurationUnit`
  - `GenAIServerTimePerOutputTokenDescription`
  - `GenAIServerTimePerOutputTokenName`
  - `GenAIServerTimePerOutputTokenUnit`
  - `GenAIServerTimeToFirstTokenDescription`
  - `GenAIServerTimeToFirstTokenName`
  - `GenAIServerTimeToFirstTokenUnit`
- `go.opentelemetry.io/otel/semconv/v1.32.0/goconv`
  - `GoConfigGogcDescription`
  - `GoConfigGogcName`
  - `GoConfigGogcUnit`
  - `GoGoroutineCountDescription`
  - `GoGoroutineCountName`
  - `GoGoroutineCountUnit`
  - `GoMemoryAllocatedDescription`
  - `GoMemoryAllocatedName`
  - `GoMemoryAllocatedUnit`
  - `GoMemoryAllocationsDescription`
  - `GoMemoryAllocationsName`
  - `GoMemoryAllocationsUnit`
  - `GoMemoryGCGoalDescription`
  - `GoMemoryGCGoalName`
  - `GoMemoryGCGoalUnit`
  - `GoMemoryLimitDescription`
  - `GoMemoryLimitName`
  - `GoMemoryLimitUnit`
  - `GoMemoryUsedDescription`
  - `GoMemoryUsedName`
  - `GoMemoryUsedUnit`
  - `GoProcessorLimitDescription`
  - `GoProcessorLimitName`
  - `GoProcessorLimitUnit`
  - `GoScheduleDurationDescription`
  - `GoScheduleDurationName`
  - `GoScheduleDurationUnit`
- `go.opentelemetry.io/otel/semconv/v1.32.0/httpconv`
  - `HTTPClientActiveRequestsDescription`
  - `HTTPClientActiveRequestsName`
  - `HTTPClientActiveRequestsUnit`
  - `HTTPClientConnectionDurationDescription`
  - `HTTPClientConnectionDurationName`
  - `HTTPClientConnectionDurationUnit`
  - `HTTPClientOpenConnectionsDescription`
  - `HTTPClientOpenConnectionsName`
  - `HTTPClientOpenConnectionsUnit`
  - `HTTPClientRequestBodySizeDescription`
  - `HTTPClientRequestBodySizeName`
  - `HTTPClientRequestBodySizeUnit`
  - `HTTPClientRequestDurationDescription`
  - `HTTPClientRequestDurationName`
  - `HTTPClientRequestDurationUnit`
  - `HTTPClientResponseBodySizeDescription`
  - `HTTPClientResponseBodySizeName`
  - `HTTPClientResponseBodySizeUnit`
  - `HTTPServerActiveRequestsDescription`
  - `HTTPServerActiveRequestsName`
  - `HTTPServerActiveRequestsUnit`
  - `HTTPServerRequestBodySizeDescription`
  - `HTTPServerRequestBodySizeName`
  - `HTTPServerRequestBodySizeUnit`
  - `HTTPServerRequestDurationDescription`
  - `HTTPServerRequestDurationName`
  - `HTTPServerRequestDurationUnit`
  - `HTTPServerResponseBodySizeDescription`
  - `HTTPServerResponseBodySizeName`
  - `HTTPServerResponseBodySizeUnit`
- `go.opentelemetry.io/otel/semconv/v1.32.0/hwconv`
  - `HwEnergyDescription`
  - `HwEnergyName`
  - `HwEnergyUnit`
  - `HwErrorsDescription`
  - `HwErrorsName`
  - `HwErrorsUnit`
  - `HwHostAmbientTemperatureDescription`
  - `HwHostAmbientTemperatureName`
  - `HwHostAmbientTemperatureUnit`
  - `HwHostEnergyDescription`
  - `HwHostEnergyName`
  - `HwHostEnergyUnit`
  - `HwHostHeatingMarginDescription`
  - `HwHostHeatingMarginName`
  - `HwHostHeatingMarginUnit`
  - `HwHostPowerDescription`
  - `HwHostPowerName`
  - `HwHostPowerUnit`
  - `HwPowerDescription`
  - `HwPowerName`
  - `HwPowerUnit`
  - `HwStatusDescription`
  - `HwStatusName`
  - `HwStatusUnit`
- `go.opentelemetry.io/otel/semconv/v1.32.0/k8sconv`
  - `K8SCronJobActiveJobsDescription`
  - `K8SCronJobActiveJobsName`
  - `K8SCronJobActiveJobsUnit`
  - `K8SDaemonSetCurrentScheduledNodesDescription`
  - `K8SDaemonSetCurrentScheduledNodesName`
  - `K8SDaemonSetCurrentScheduledNodesUnit`
  - `K8SDaemonSetDesiredScheduledNodesDescription`
  - `K8SDaemonSetDesiredScheduledNodesName`
  - `K8SDaemonSetDesiredScheduledNodesUnit`
  - `K8SDaemonSetMisscheduledNodesDescription`
  - `K8SDaemonSetMisscheduledNodesName`
  - `K8SDaemonSetMisscheduledNodesUnit`
  - `K8SDaemonSetReadyNodesDescription`
  - `K8SDaemonSetReadyNodesName`
  - `K8SDaemonSetReadyNodesUnit`
  - `K8SDeploymentAvailablePodsDescription`
  - `K8SDeploymentAvailablePodsName`
  - `K8SDeploymentAvailablePodsUnit`
  - `K8SDeploymentDesiredPodsDescription`
  - `K8SDeploymentDesiredPodsName`
  - `K8SDeploymentDesiredPodsUnit`
  - `K8SHpaCurrentPodsDescription`
  - `K8SHpaCurrentPodsName`
  - `K8SHpaCurrentPodsUnit`
  - `K8SHpaDesiredPodsDescription`
  - `K8SHpaDesiredPodsName`
  - `K8SHpaDesiredPodsUnit`
  - `K8SHpaMaxPodsDescription`
  - `K8SHpaMaxPodsName`
  - `K8SHpaMaxPodsUnit`
  - `K8SHpaMinPodsDescription`
  - `K8SHpaMinPodsName`
  - `K8SHpaMinPodsUnit`
  - `K8SJobActivePodsDescription`
  - `K8SJobActivePodsName`
  - `K8SJobActivePodsUnit`
  - `K8SJobDesiredSuccessfulPodsDescription`
  - `K8SJobDesiredSuccessfulPodsName`
  - `K8SJobDesiredSuccessfulPodsUnit`
  - `K8SJobFailedPodsDescription`
  - `K8SJobFailedPodsName`
  - `K8SJobFailedPodsUnit`
  - `K8SJobMaxParallelPodsDescription`
  - `K8SJobMaxParallelPodsName`
  - `K8SJobMaxParallelPodsUnit`
  - `K8SJobSuccessfulPodsDescription`
  - `K8SJobSuccessfulPodsName`
  - `K8SJobSuccessfulPodsUnit`
  - `K8SNamespacePhaseDescription`
  - `K8SNamespacePhaseName`
  - `K8SNamespacePhaseUnit`
  - `K8SNodeCPUTimeDescription`
  - `K8SNodeCPUTimeName`
  - `K8SNodeCPUTimeUnit`
  - `K8SNodeCPUUsageDescription`
  - `K8SNodeCPUUsageName`
  - `K8SNodeCPUUsageUnit`
  - `K8SNodeMemoryUsageDescription`
  - `K8SNodeMemoryUsageName`
  - `K8SNodeMemoryUsageUnit`
  - `K8SNodeNetworkErrorsDescription`
  - `K8SNodeNetworkErrorsName`
  - `K8SNodeNetworkErrorsUnit`
  - `K8SNodeNetworkIoDescription`
  - `K8SNodeNetworkIoName`
  - `K8SNodeNetworkIoUnit`
  - `K8SNodeUptimeDescription`
  - `K8SNodeUptimeName`
  - `K8SNodeUptimeUnit`
  - `K8SPodCPUTimeDescription`
  - `K8SPodCPUTimeName`
  - `K8SPodCPUTimeUnit`
  - `K8SPodCPUUsageDescription`
  - `K8SPodCPUUsageName`
  - `K8SPodCPUUsageUnit`
  - `K8SPodMemoryUsageDescription`
  - `K8SPodMemoryUsageName`
  - `K8SPodMemoryUsageUnit`
  - `K8SPodNetworkErrorsDescription`
  - `K8SPodNetworkErrorsName`
  - `K8SPodNetworkErrorsUnit`
  - `K8SPodNetworkIoDescription`
  - `K8SPodNetworkIoName`
  - `K8SPodNetworkIoUnit`
  - `K8SPodUptimeDescription`
  - `K8SPodUptimeName`
  - `K8SPodUptimeUnit`
  - `K8SReplicaSetAvailablePodsDescription`
  - `K8SReplicaSetAvailablePodsName`
  - `K8SReplicaSetAvailablePodsUnit`
  - `K8SReplicaSetDesiredPodsDescription`
  - `K8SReplicaSetDesiredPodsName`
  - `K8SReplicaSetDesiredPodsUnit`
  - `K8SReplicationControllerAvailablePodsDescription`
  - `K8SReplicationControllerAvailablePodsName`
  - `K8SReplicationControllerAvailablePodsUnit`
  - `K8SReplicationControllerDesiredPodsDescription`
  - `K8SReplicationControllerDesiredPodsName`
  - `K8SReplicationControllerDesiredPodsUnit`
  - `K8SStatefulSetCurrentPodsDescription`
  - `K8SStatefulSetCurrentPodsName`
  - `K8SStatefulSetCurrentPodsUnit`
  - `K8SStatefulSetDesiredPodsDescription`
  - `K8SStatefulSetDesiredPodsName`
  - `K8SStatefulSetDesiredPodsUnit`
  - `K8SStatefulSetReadyPodsDescription`
  - `K8SStatefulSetReadyPodsName`
  - `K8SStatefulSetReadyPodsUnit`
  - `K8SStatefulSetUpdatedPodsDescription`
  - `K8SStatefulSetUpdatedPodsName`
  - `K8SStatefulSetUpdatedPodsUnit`
- `go.opentelemetry.io/otel/semconv/v1.32.0/messagingconv`
  - `MessagingClientConsumedMessagesDescription`
  - `MessagingClientConsumedMessagesName`
  - `MessagingClientConsumedMessagesUnit`
  - `MessagingClientOperationDurationDescription`
  - `MessagingClientOperationDurationName`
  - `MessagingClientOperationDurationUnit`
  - `MessagingClientSentMessagesDescription`
  - `MessagingClientSentMessagesName`
  - `MessagingClientSentMessagesUnit`
  - `MessagingProcessDurationDescription`
  - `MessagingProcessDurationName`
  - `MessagingProcessDurationUnit`
- `go.opentelemetry.io/otel/semconv/v1.32.0/otelconv`
  - `OTelSDKExporterSpanExportedCountDescription`
  - `OTelSDKExporterSpanExportedCountName`
  - `OTelSDKExporterSpanExportedCountUnit`
  - `OTelSDKExporterSpanInflightCountDescription`
  - `OTelSDKExporterSpanInflightCountName`
  - `OTelSDKExporterSpanInflightCountUnit`
  - `OTelSDKProcessorSpanProcessedCountDescription`
  - `OTelSDKProcessorSpanProcessedCountName`
  - `OTelSDKProcessorSpanProcessedCountUnit`
  - `OTelSDKProcessorSpanQueueCapacityDescription`
  - `OTelSDKProcessorSpanQueueCapacityName`
  - `OTelSDKProcessorSpanQueueCapacityUnit`
  - `OTelSDKProcessorSpanQueueSizeDescription`
  - `OTelSDKProcessorSpanQueueSizeName`
  - `OTelSDKProcessorSpanQueueSizeUnit`
  - `OTelSDKSpanEndedCountDescription`
  - `OTelSDKSpanEndedCountName`
  - `OTelSDKSpanEndedCountUnit`
  - `OTelSDKSpanLiveCountDescription`
  - `OTelSDKSpanLiveCountName`
  - `OTelSDKSpanLiveCountUnit`
- `go.opentelemetry.io/otel/semconv/v1.32.0/processconv`
  - `ProcessCPUTimeDescription`
  - `ProcessCPUTimeName`
  - `ProcessCPUTimeUnit`
  - `ProcessCPUUtilizationDescription`
  - `ProcessCPUUtilizationName`
  - `ProcessCPUUtilizationUnit`
  - `ProcessContextSwitchesDescription`
  - `ProcessContextSwitchesName`
  - `ProcessContextSwitchesUnit`
  - `ProcessDiskIoDescription`
  - `ProcessDiskIoName`
  - `ProcessDiskIoUnit`
  - `ProcessMemoryUsageDescription`
  - `ProcessMemoryUsageName`
  - `ProcessMemoryUsageUnit`
  - `ProcessMemoryVirtualDescription`
  - `ProcessMemoryVirtualName`
  - `ProcessMemoryVirtualUnit`
  - `ProcessNetworkIoDescription`
  - `ProcessNetworkIoName`
  - `ProcessNetworkIoUnit`
  - `ProcessOpenFileDescriptorCountDescription`
  - `ProcessOpenFileDescriptorCountName`
  - `ProcessOpenFileDescriptorCountUnit`
  - `ProcessPagingFaultsDescription`
  - `ProcessPagingFaultsName`
  - `ProcessPagingFaultsUnit`
  - `ProcessThreadCountDescription`
  - `ProcessThreadCountName`
  - `ProcessThreadCountUnit`
  - `ProcessUptimeDescription`
  - `ProcessUptimeName`
  - `ProcessUptimeUnit`
- `go.opentelemetry.io/otel/semconv/v1.32.0/rpcconv`
  - `RPCClientDurationDescription`
  - `RPCClientDurationName`
  - `RPCClientDurationUnit`
  - `RPCClientRequestSizeDescription`
  - `RPCClientRequestSizeName`
  - `RPCClientRequestSizeUnit`
  - `RPCClientRequestsPerRPCDescription`
  - `RPCClientRequestsPerRPCName`
  - `RPCClientRequestsPerRPCUnit`
  - `RPCClientResponseSizeDescription`
  - `RPCClientResponseSizeName`
  - `RPCClientResponseSizeUnit`
  - `RPCClientResponsesPerRPCDescription`
  - `RPCClientResponsesPerRPCName`
  - `RPCClientResponsesPerRPCUnit`
  - `RPCServerDurationDescription`
  - `RPCServerDurationName`
  - `RPCServerDurationUnit`
  - `RPCServerRequestSizeDescription`
  - `RPCServerRequestSizeName`
  - `RPCServerRequestSizeUnit`
  - `RPCServerRequestsPerRPCDescription`
  - `RPCServerRequestsPerRPCName`
  - `RPCServerRequestsPerRPCUnit`
  - `RPCServerResponseSizeDescription`
  - `RPCServerResponseSizeName`
  - `RPCServerResponseSizeUnit`
  - `RPCServerResponsesPerRPCDescription`
  - `RPCServerResponsesPerRPCName`
  - `RPCServerResponsesPerRPCUnit`
- `go.opentelemetry.io/otel/semconv/v1.32.0/signalrconv`
  - `SignalrServerActiveConnectionsDescription`
  - `SignalrServerActiveConnectionsName`
  - `SignalrServerActiveConnectionsUnit`
  - `SignalrServerConnectionDurationDescription`
  - `SignalrServerConnectionDurationName`
  - `SignalrServerConnectionDurationUnit`
- `go.opentelemetry.io/otel/semconv/v1.32.0/systemconv`
  - `SystemCPULogicalCountDescription`
  - `SystemCPULogicalCountName`
  - `SystemCPULogicalCountUnit`
  - `SystemCPUPhysicalCountDescription`
  - `SystemCPUPhysicalCountName`
  - `SystemCPUPhysicalCountUnit`
  - `SystemDiskIoName`
  - `SystemDiskIoTimeDescription`
  - `SystemDiskIoTimeName`
  - `SystemDiskIoTimeUnit`
  - `SystemDiskIoUnit`
  - `SystemDiskLimitDescription`
  - `SystemDiskLimitName`
  - `SystemDiskLimitUnit`
  - `SystemDiskMergedName`
  - `SystemDiskMergedUnit`
  - `SystemDiskOperationTimeDescription`
  - `SystemDiskOperationTimeName`
  - `SystemDiskOperationTimeUnit`
  - `SystemDiskOperationsName`
  - `SystemDiskOperationsUnit`
  - `SystemFilesystemLimitDescription`
  - `SystemFilesystemLimitName`
  - `SystemFilesystemLimitUnit`
  - `SystemFilesystemUsageDescription`
  - `SystemFilesystemUsageName`
  - `SystemFilesystemUsageUnit`
  - `SystemFilesystemUtilizationName`
  - `SystemFilesystemUtilizationUnit`
  - `SystemLinuxMemoryAvailableDescription`
  - `SystemLinuxMemoryAvailableName`
  - `SystemLinuxMemoryAvailableUnit`
  - `SystemLinuxMemorySlabUsageDescription`
  - `SystemLinuxMemorySlabUsageName`
  - `SystemLinuxMemorySlabUsageUnit`
  - `SystemMemoryLimitDescription`
  - `SystemMemoryLimitName`
  - `SystemMemoryLimitUnit`
  - `SystemMemorySharedDescription`
  - `SystemMemorySharedName`
  - `SystemMemorySharedUnit`
  - `SystemMemoryUsageDescription`
  - `SystemMemoryUsageName`
  - `SystemMemoryUsageUnit`
  - `SystemMemoryUtilizationName`
  - `SystemMemoryUtilizationUnit`
  - `SystemNetworkConnectionsName`
  - `SystemNetworkConnectionsUnit`
  - `SystemNetworkDroppedDescription`
  - `SystemNetworkDroppedName`
  - `SystemNetworkDroppedUnit`
  - `SystemNetworkErrorsDescription`
  - `SystemNetworkErrorsName`
  - `SystemNetworkErrorsUnit`
  - `SystemNetworkIoName`
  - `SystemNetworkIoUnit`
  - `SystemNetworkPacketsName`
  - `SystemNetworkPacketsUnit`
  - `SystemPagingFaultsName`
  - `SystemPagingFaultsUnit`
  - `SystemPagingOperationsName`
  - `SystemPagingOperationsUnit`
  - `SystemPagingUsageDescription`
  - `SystemPagingUsageName`
  - `SystemPagingUsageUnit`
  - `SystemPagingUtilizationName`
  - `SystemPagingUtilizationUnit`
  - `SystemProcessCountDescription`
  - `SystemProcessCountName`
  - `SystemProcessCountUnit`
  - `SystemProcessCreatedDescription`
  - `SystemProcessCreatedName`
  - `SystemProcessCreatedUnit`
  - `SystemUptimeDescription`
  - `SystemUptimeName`
  - `SystemUptimeUnit`
- `go.opentelemetry.io/otel/semconv/v1.32.0/vcsconv`
  - `VCSChangeCountDescription`
  - `VCSChangeCountName`
  - `VCSChangeCountUnit`
  - `VCSChangeDurationDescription`
  - `VCSChangeDurationName`
  - `VCSChangeDurationUnit`
  - `VCSChangeTimeToApprovalDescription`
  - `VCSChangeTimeToApprovalName`
  - `VCSChangeTimeToApprovalUnit`
  - `VCSChangeTimeToMergeDescription`
  - `VCSChangeTimeToMergeName`
  - `VCSChangeTimeToMergeUnit`
  - `VCSContributorCountDescription`
  - `VCSContributorCountName`
  - `VCSContributorCountUnit`
  - `VCSRefCountDescription`
  - `VCSRefCountName`
  - `VCSRefCountUnit`
  - `VCSRefLinesDeltaDescription`
  - `VCSRefLinesDeltaName`
  - `VCSRefLinesDeltaUnit`
  - `VCSRefRevisionsDeltaDescription`
  - `VCSRefRevisionsDeltaName`
  - `VCSRefRevisionsDeltaUnit`
  - `VCSRefTimeDescription`
  - `VCSRefTimeName`
  - `VCSRefTimeUnit`
  - `VCSRepositoryCountDescription`
  - `VCSRepositoryCountName`
  - `VCSRepositoryCountUnit`

Note the following declarations from the `kestrel` namespace have been dropped.

- `KestrelActiveConnectionsDescription`
- `KestrelActiveConnectionsName`
- `KestrelActiveConnectionsUnit`
- `KestrelActiveTLSHandshakesDescription`
- `KestrelActiveTLSHandshakesName`
- `KestrelActiveTLSHandshakesUnit`
- `KestrelConnectionDurationDescription`
- `KestrelConnectionDurationName`
- `KestrelConnectionDurationUnit`
- `KestrelQueuedConnectionsDescription`
- `KestrelQueuedConnectionsName`
- `KestrelQueuedConnectionsUnit`
- `KestrelQueuedRequestsDescription`
- `KestrelQueuedRequestsName`
- `KestrelQueuedRequestsUnit`
- `KestrelRejectedConnectionsDescription`
- `KestrelRejectedConnectionsName`
- `KestrelRejectedConnectionsUnit`
- `KestrelTLSHandshakeDurationDescription`
- `KestrelTLSHandshakeDurationName`
- `KestrelTLSHandshakeDurationUnit`
- `KestrelUpgradedConnectionsDescription`
- `KestrelUpgradedConnectionsName`
- `KestrelUpgradedConnectionsUnit`

If you use any of these declarations in your Go application, please [open an issue] describing your use-case.

## Dropped deprecations

The following declarations have been deprecated in the [OpenTelemetry Semantic Conventions].
Refer to the respective documentation in that repository for deprecation instructions for each type.

- `FeatureFlagEvaluationReasonCached`
- `FeatureFlagEvaluationReasonDefault`
- `FeatureFlagEvaluationReasonDisabled`
- `FeatureFlagEvaluationReasonError`
- `FeatureFlagEvaluationReasonKey`
- `FeatureFlagEvaluationReasonSplit`
- `FeatureFlagEvaluationReasonStale`
- `FeatureFlagEvaluationReasonStatic`
- `FeatureFlagEvaluationReasonTargetingMatch`
- `FeatureFlagEvaluationReasonUnknown`
- `FeatureFlagVariant`
- `FeatureFlagVariantKey`

### Renames

The following renames have been introduced to better match Go and industry naming standards.
Be sure to update any use from `go.opentelemetry.io/otel/semconv/v1.31.0` with the equivalent in `go.opentelemetry.io/otel/semconv/v1.32.0`.

| `v1.31.0` | `v1.32.0` |
| --- | --- |
| `CPUModeIowait` | `CPUModeIOWait` |
| `CloudPlatformAWSOpenshift` | `CloudPlatformAWSOpenShift` |
| `CloudPlatformAlibabaCloudFc` | `CloudPlatformAlibabaCloudFC` |
| `CloudPlatformAlibabaCloudOpenshift` | `CloudPlatformAlibabaCloudOpenShift` |
| `CloudPlatformAzureOpenshift` | `CloudPlatformAzureOpenShift` |
| `CloudPlatformGCPOpenshift` | `CloudPlatformGCPOpenShift` |
| `CloudPlatformIbmCloudOpenshift` | `CloudPlatformIBMCloudOpenShift` |
| `CloudPlatformOracleCloudOke` | `CloudPlatformOracleCloudOKE` |
| `CloudPlatformTencentCloudCvm` | `CloudPlatformTencentCloudCVM` |
| `CloudPlatformTencentCloudScf` | `CloudPlatformTencentCloudSCF` |
| `CloudProviderIbmCloud` | `CloudProviderIBMCloud` |
| `CloudeventsEventID` | `CloudEventsEventID` |
| `CloudeventsEventIDKey` | `CloudEventsEventIDKey` |
| `CloudeventsEventSource` | `CloudEventsEventSource` |
| `CloudeventsEventSourceKey` | `CloudEventsEventSourceKey` |
| `CloudeventsEventSpecVersion` | `CloudEventsEventSpecVersion` |
| `CloudeventsEventSpecVersionKey` | `CloudEventsEventSpecVersionKey` |
| `CloudeventsEventSubject` | `CloudEventsEventSubject` |
| `CloudeventsEventSubjectKey` | `CloudEventsEventSubjectKey` |
| `CloudeventsEventType` | `CloudEventsEventType` |
| `CloudeventsEventTypeKey` | `CloudEventsEventTypeKey` |
| `CloudfoundryAppID` | `CloudFoundryAppID` |
| `CloudfoundryAppIDKey` | `CloudFoundryAppIDKey` |
| `CloudfoundryAppInstanceID` | `CloudFoundryAppInstanceID` |
| `CloudfoundryAppInstanceIDKey` | `CloudFoundryAppInstanceIDKey` |
| `CloudfoundryAppName` | `CloudFoundryAppName` |
| `CloudfoundryAppNameKey` | `CloudFoundryAppNameKey` |
| `CloudfoundryOrgID` | `CloudFoundryOrgID` |
| `CloudfoundryOrgIDKey` | `CloudFoundryOrgIDKey` |
| `CloudfoundryOrgName` | `CloudFoundryOrgName` |
| `CloudfoundryOrgNameKey` | `CloudFoundryOrgNameKey` |
| `CloudfoundryProcessID` | `CloudFoundryProcessID` |
| `CloudfoundryProcessIDKey` | `CloudFoundryProcessIDKey` |
| `CloudfoundryProcessType` | `CloudFoundryProcessType` |
| `CloudfoundryProcessTypeKey` | `CloudFoundryProcessTypeKey` |
| `CloudfoundrySpaceID` | `CloudFoundrySpaceID` |
| `CloudfoundrySpaceIDKey` | `CloudFoundrySpaceIDKey` |
| `CloudfoundrySpaceName` | `CloudFoundrySpaceName` |
| `CloudfoundrySpaceNameKey` | `CloudFoundrySpaceNameKey` |
| `CloudfoundrySystemID` | `CloudFoundrySystemID` |
| `CloudfoundrySystemIDKey` | `CloudFoundrySystemIDKey` |
| `CloudfoundrySystemInstanceID` | `CloudFoundrySystemInstanceID` |
| `CloudfoundrySystemInstanceIDKey` | `CloudFoundrySystemInstanceIDKey` |
| `ContainerCsiPluginName` | `ContainerCSIPluginName` |
| `ContainerCsiPluginNameKey` | `ContainerCSIPluginNameKey` |
| `ContainerCsiVolumeID` | `ContainerCSIVolumeID` |
| `ContainerCsiVolumeIDKey` | `ContainerCSIVolumeIDKey` |
| `DBSystemNameClickhouse` | `DBSystemNameClickHouse` |
| `DBSystemNameCockroachdb` | `DBSystemNameCockroachDB` |
| `DBSystemNameFirebirdsql` | `DBSystemNameFirebirdSQL` |
| `DBSystemNameIbmDb2` | `DBSystemNameIBMDB2` |
| `DBSystemNameIbmInformix` | `DBSystemNameIBMInformix` |
| `DBSystemNameIbmNetezza` | `DBSystemNameIBMNetezza` |
| `DBSystemNameInfluxdb` | `DBSystemNameInfluxDB` |
| `DBSystemNameOpensearch` | `DBSystemNameOpenSearch` |
| `DBSystemNameSapHana` | `DBSystemNameSAPHANA` |
| `DBSystemNameSapMaxDB` | `DBSystemNameSAPMaxDB` |
| `DBSystemNameSqlite` | `DBSystemNameSQLite` |
| `DiskIoDirectionKey` | `DiskIODirectionKey` |
| `DiskIoDirectionRead` | `DiskIODirectionRead` |
| `DiskIoDirectionWrite` | `DiskIODirectionWrite` |
| `FaaSTriggerPubsub` | `FaaSTriggerPubSub` |
| `GCPGceInstanceHostname` | `GCPGCEInstanceHostname` |
| `GCPGceInstanceHostnameKey` | `GCPGCEInstanceHostnameKey` |
| `GCPGceInstanceName` | `GCPGCEInstanceName` |
| `GCPGceInstanceNameKey` | `GCPGCEInstanceNameKey` |
| `GenAIOpenaiRequestServiceTierAuto` | `GenAIOpenAIRequestServiceTierAuto` |
| `GenAIOpenaiRequestServiceTierDefault` | `GenAIOpenAIRequestServiceTierDefault` |
| `GenAIOpenaiRequestServiceTierKey` | `GenAIOpenAIRequestServiceTierKey` |
| `GenAIOpenaiResponseServiceTier` | `GenAIOpenAIResponseServiceTier` |
| `GenAIOpenaiResponseServiceTierKey` | `GenAIOpenAIResponseServiceTierKey` |
| `GenAIOpenaiResponseSystemFingerprint` | `GenAIOpenAIResponseSystemFingerprint` |
| `GenAIOpenaiResponseSystemFingerprintKey` | `GenAIOpenAIResponseSystemFingerprintKey` |
| `GenAISystemAzAIOpenai` | `GenAISystemAzAIOpenAI` |
| `GenAISystemIbmWatsonxAI` | `GenAISystemIBMWatsonxAI` |
| `GenAISystemOpenai` | `GenAISystemOpenAI` |
| `GeoCountryIsoCode` | `GeoCountryISOCode` |
| `GeoCountryIsoCodeKey` | `GeoCountryISOCodeKey` |
| `GeoRegionIsoCode` | `GeoRegionISOCode` |
| `GeoRegionIsoCodeKey` | `GeoRegionISOCodeKey` |
| `GraphqlDocument` | `GraphQLDocument` |
| `GraphqlDocumentKey` | `GraphQLDocumentKey` |
| `GraphqlOperationName` | `GraphQLOperationName` |
| `GraphqlOperationNameKey` | `GraphQLOperationNameKey` |
| `GraphqlOperationTypeKey` | `GraphQLOperationTypeKey` |
| `GraphqlOperationTypeMutation` | `GraphQLOperationTypeMutation` |
| `GraphqlOperationTypeQuery` | `GraphQLOperationTypeQuery` |
| `GraphqlOperationTypeSubscription` | `GraphQLOperationTypeSubscription` |
| `IosAppStateActive` | `IOSAppStateActive` |
| `IosAppStateBackground` | `IOSAppStateBackground` |
| `IosAppStateForeground` | `IOSAppStateForeground` |
| `IosAppStateInactive` | `IOSAppStateInactive` |
| `IosAppStateKey` | `IOSAppStateKey` |
| `IosAppStateTerminate` | `IOSAppStateTerminate` |
| `K8SHpaName` | `K8SHPAName` |
| `K8SHpaNameKey` | `K8SHPANameKey` |
| `K8SHpaUID` | `K8SHPAUID` |
| `K8SHpaUIDKey` | `K8SHPAUIDKey` |
| `MessagingEventhubsMessageEnqueuedTime` | `MessagingEventHubsMessageEnqueuedTime` |
| `MessagingEventhubsMessageEnqueuedTimeKey` | `MessagingEventHubsMessageEnqueuedTimeKey` |
| `MessagingGCPPubsubMessageAckDeadline` | `MessagingGCPPubSubMessageAckDeadline` |
| `MessagingGCPPubsubMessageAckDeadlineKey` | `MessagingGCPPubSubMessageAckDeadlineKey` |
| `MessagingGCPPubsubMessageAckID` | `MessagingGCPPubSubMessageAckID` |
| `MessagingGCPPubsubMessageAckIDKey` | `MessagingGCPPubSubMessageAckIDKey` |
| `MessagingGCPPubsubMessageDeliveryAttempt` | `MessagingGCPPubSubMessageDeliveryAttempt` |
| `MessagingGCPPubsubMessageDeliveryAttemptKey` | `MessagingGCPPubSubMessageDeliveryAttemptKey` |
| `MessagingGCPPubsubMessageOrderingKey` | `MessagingGCPPubSubMessageOrderingKey` |
| `MessagingGCPPubsubMessageOrderingKeyKey` | `MessagingGCPPubSubMessageOrderingKeyKey` |
| `MessagingRabbitmqDestinationRoutingKey` | `MessagingRabbitMQDestinationRoutingKey` |
| `MessagingRabbitmqDestinationRoutingKeyKey` | `MessagingRabbitMQDestinationRoutingKeyKey` |
| `MessagingRabbitmqMessageDeliveryTag` | `MessagingRabbitMQMessageDeliveryTag` |
| `MessagingRabbitmqMessageDeliveryTagKey` | `MessagingRabbitMQMessageDeliveryTagKey` |
| `MessagingRocketmqConsumptionModelBroadcasting` | `MessagingRocketMQConsumptionModelBroadcasting` |
| `MessagingRocketmqConsumptionModelClustering` | `MessagingRocketMQConsumptionModelClustering` |
| `MessagingRocketmqConsumptionModelKey` | `MessagingRocketMQConsumptionModelKey` |
| `MessagingRocketmqMessageDelayTimeLevel` | `MessagingRocketMQMessageDelayTimeLevel` |
| `MessagingRocketmqMessageDelayTimeLevelKey` | `MessagingRocketMQMessageDelayTimeLevelKey` |
| `MessagingRocketmqMessageDeliveryTimestamp` | `MessagingRocketMQMessageDeliveryTimestamp` |
| `MessagingRocketmqMessageDeliveryTimestampKey` | `MessagingRocketMQMessageDeliveryTimestampKey` |
| `MessagingRocketmqMessageGroup` | `MessagingRocketMQMessageGroup` |
| `MessagingRocketmqMessageGroupKey` | `MessagingRocketMQMessageGroupKey` |
| `MessagingRocketmqMessageKeys` | `MessagingRocketMQMessageKeys` |
| `MessagingRocketmqMessageKeysKey` | `MessagingRocketMQMessageKeysKey` |
| `MessagingRocketmqMessageTag` | `MessagingRocketMQMessageTag` |
| `MessagingRocketmqMessageTagKey` | `MessagingRocketMQMessageTagKey` |
| `MessagingRocketmqMessageTypeDelay` | `MessagingRocketMQMessageTypeDelay` |
| `MessagingRocketmqMessageTypeFifo` | `MessagingRocketMQMessageTypeFifo` |
| `MessagingRocketmqMessageTypeKey` | `MessagingRocketMQMessageTypeKey` |
| `MessagingRocketmqMessageTypeNormal` | `MessagingRocketMQMessageTypeNormal` |
| `MessagingRocketmqMessageTypeTransaction` | `MessagingRocketMQMessageTypeTransaction` |
| `MessagingRocketmqNamespace` | `MessagingRocketMQNamespace` |
| `MessagingRocketmqNamespaceKey` | `MessagingRocketMQNamespaceKey` |
| `MessagingServicebusDispositionStatusAbandon` | `MessagingServiceBusDispositionStatusAbandon` |
| `MessagingServicebusDispositionStatusComplete` | `MessagingServiceBusDispositionStatusComplete` |
| `MessagingServicebusDispositionStatusDeadLetter` | `MessagingServiceBusDispositionStatusDeadLetter` |
| `MessagingServicebusDispositionStatusDefer` | `MessagingServiceBusDispositionStatusDefer` |
| `MessagingServicebusDispositionStatusKey` | `MessagingServiceBusDispositionStatusKey` |
| `MessagingServicebusMessageDeliveryCount` | `MessagingServiceBusMessageDeliveryCount` |
| `MessagingServicebusMessageDeliveryCountKey` | `MessagingServiceBusMessageDeliveryCountKey` |
| `MessagingServicebusMessageEnqueuedTime` | `MessagingServiceBusMessageEnqueuedTime` |
| `MessagingServicebusMessageEnqueuedTimeKey` | `MessagingServiceBusMessageEnqueuedTimeKey` |
| `MessagingSystemAWSSqs` | `MessagingSystemAWSSQS` |
| `MessagingSystemActivemq` | `MessagingSystemActiveMQ` |
| `MessagingSystemEventgrid` | `MessagingSystemEventGrid` |
| `MessagingSystemEventhubs` | `MessagingSystemEventHubs` |
| `MessagingSystemGCPPubsub` | `MessagingSystemGCPPubSub` |
| `MessagingSystemJms` | `MessagingSystemJMS` |
| `MessagingSystemRabbitmq` | `MessagingSystemRabbitMQ` |
| `MessagingSystemRocketmq` | `MessagingSystemRocketMQ` |
| `MessagingSystemServicebus` | `MessagingSystemServiceBus` |
| `NetworkCarrierIcc` | `NetworkCarrierICC` |
| `NetworkCarrierIccKey` | `NetworkCarrierICCKey` |
| `NetworkCarrierMcc` | `NetworkCarrierMCC` |
| `NetworkCarrierMccKey` | `NetworkCarrierMCCKey` |
| `NetworkCarrierMnc` | `NetworkCarrierMNC` |
| `NetworkCarrierMncKey` | `NetworkCarrierMNCKey` |
| `NetworkIoDirectionKey` | `NetworkIODirectionKey` |
| `NetworkIoDirectionReceive` | `NetworkIODirectionReceive` |
| `NetworkIoDirectionTransmit` | `NetworkIODirectionTransmit` |
| `NetworkTypeIpv4` | `NetworkTypeIPv4` |
| `NetworkTypeIpv6` | `NetworkTypeIPv6` |
| `OciManifestDigest` | `OCIManifestDigest` |
| `OciManifestDigestKey` | `OCIManifestDigestKey` |
| `OpentracingRefTypeChildOf` | `OpenTracingRefTypeChildOf` |
| `OpentracingRefTypeFollowsFrom` | `OpenTracingRefTypeFollowsFrom` |
| `OpentracingRefTypeKey` | `OpenTracingRefTypeKey` |
| `ProcessExecutableBuildIDGnu` | `ProcessExecutableBuildIDGNU` |
| `ProcessExecutableBuildIDGnuKey` | `ProcessExecutableBuildIDGNUKey` |
| `RPCJsonrpcErrorCode` | `RPCJSONRPCErrorCode` |
| `RPCJsonrpcErrorCodeKey` | `RPCJSONRPCErrorCodeKey` |
| `RPCJsonrpcErrorMessage` | `RPCJSONRPCErrorMessage` |
| `RPCJsonrpcErrorMessageKey` | `RPCJSONRPCErrorMessageKey` |
| `RPCJsonrpcRequestID` | `RPCJSONRPCRequestID` |
| `RPCJsonrpcRequestIDKey` | `RPCJSONRPCRequestIDKey` |
| `RPCJsonrpcVersion` | `RPCJSONRPCVersion` |
| `RPCJsonrpcVersionKey` | `RPCJSONRPCVersionKey` |
| `SignalrConnectionStatusAppShutdown` | `SignalRConnectionStatusAppShutdown` |
| `SignalrConnectionStatusKey` | `SignalRConnectionStatusKey` |
| `SignalrConnectionStatusNormalClosure` | `SignalRConnectionStatusNormalClosure` |
| `SignalrConnectionStatusTimeout` | `SignalRConnectionStatusTimeout` |
| `SignalrTransportKey` | `SignalRTransportKey` |
| `SignalrTransportLongPolling` | `SignalRTransportLongPolling` |
| `SignalrTransportServerSentEvents` | `SignalRTransportServerSentEvents` |
| `SignalrTransportWebSockets` | `SignalRTransportWebSockets` |
| `TelemetrySDKLanguageWebjs` | `TelemetrySDKLanguageWebJS` |

[OpenTelemetry Semantic Conventions]: https://github.com/open-telemetry/semantic-conventions
[open an issue]: https://github.com/open-telemetry/opentelemetry-go/issues/new?template=Blank+issue
