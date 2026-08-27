using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Google.Protobuf.WellKnownTypes;
using Temporalio.Common;
using Temporalio.Converters;
using Temporalio.Workflows;
using ApiCommon = Temporalio.Api.Common.V1;
using ApiDeployment = Temporalio.Api.Deployment.V1;
using ApiFailure = Temporalio.Api.Failure.V1;
using ApiTaskQueue = Temporalio.Api.TaskQueue.V1;
using ApiWorkflow = Temporalio.Api.Workflow.V1;

namespace NexGen.Support
{
    internal static class TemporalWorkflowContext
    {
        internal static string WorkflowNamespace() => Workflow.Info.Namespace;
    }

    internal static class TemporalFunctionNames
    {
        internal static (MethodInfo Method, IReadOnlyCollection<object?> Args) ExtractCall<TDelegate>(Expression<TDelegate> expression)
        {
            if (expression.Body is not MethodCallExpression call)
            {
                throw new ArgumentException("Expression must be a single method call", nameof(expression));
            }
            var method = call.Method;
            var args = call.Arguments.Select(arg => Expression.Lambda<Func<object?>>(Expression.Convert(arg, typeof(object))).Compile()()).ToArray();
            return (method, args);
        }

        internal static string WorkflowName(MethodInfo method)
        {
            if (method.GetCustomAttribute<WorkflowRunAttribute>() == null)
            {
                throw new ArgumentException($"{method} missing WorkflowRun attribute");
            }
            var definition = WorkflowDefinition.Create(method.ReflectedType ??
                throw new ArgumentException($"{method} has no reflected type"));
            return definition.Name ??
                throw new ArgumentException(
                    $"{method} cannot be used directly since it is a dynamic workflow");
        }

        internal static string SignalName(MethodInfo method)
        {
            var definition = WorkflowSignalDefinition.FromMethod(method);
            return definition.Name ??
                throw new ArgumentException(
                    $"{method} cannot be used directly since it is a dynamic signal");
        }
    }

    internal static class ProtoExtensions
    {
        internal static ApiCommon.WorkflowType ToWorkflowTypeProto(this string value, IPayloadConverter? payloadConverter = null) =>
            new() { Name = value };

        internal static string FromWorkflowTypeProto(ApiCommon.WorkflowType value, IPayloadConverter? payloadConverter = null) =>
            value.Name;

        internal static ApiTaskQueue.TaskQueue ToTaskQueueProto(this string value, IPayloadConverter? payloadConverter = null) =>
            new() { Name = value };

        internal static string FromTaskQueueProto(ApiTaskQueue.TaskQueue value, IPayloadConverter? payloadConverter = null) =>
            value.Name;

        internal static ApiCommon.Payload ToPayload(object? value, IPayloadConverter? payloadConverter = null) =>
            (payloadConverter ?? Workflow.PayloadConverter).ToPayload(value);

        internal static object? FromPayload(ApiCommon.Payload payload, IPayloadConverter? payloadConverter = null) =>
            (payloadConverter ?? Workflow.PayloadConverter).ToValue<object?>(payload);

        internal static ApiCommon.Payloads ToPayloads(IEnumerable<object?> values, IPayloadConverter? payloadConverter = null)
        {
            var payloads = new ApiCommon.Payloads();
            payloads.Payloads_.AddRange((payloadConverter ?? Workflow.PayloadConverter).ToPayloads(values as IReadOnlyCollection<object?> ?? new List<object?>(values)));
            return payloads;
        }

        internal static IReadOnlyCollection<object?> FromPayloads(ApiCommon.Payloads payloads, IPayloadConverter? payloadConverter = null) =>
            payloads.Payloads_.Select(payload => FromPayload(payload, payloadConverter)).ToArray();

        internal static ApiFailure.Failure ToFailureProto(this Exception value, IPayloadConverter? payloadConverter = null) =>
            DataConverter.Default.FailureConverter.ToFailure(value, payloadConverter ?? Workflow.PayloadConverter);

        internal static Exception FromFailureProto(ApiFailure.Failure value, IPayloadConverter? payloadConverter = null) =>
            DataConverter.Default.FailureConverter.ToException(value, payloadConverter ?? Workflow.PayloadConverter);

        internal static Duration ToProto(this TimeSpan value, IPayloadConverter? payloadConverter = null) =>
            Duration.FromTimeSpan(value);

        internal static TimeSpan FromDurationProto(Duration value, IPayloadConverter? payloadConverter = null) =>
            value.ToTimeSpan();

        internal static ApiCommon.RetryPolicy ToProto(this Temporalio.Common.RetryPolicy value, IPayloadConverter? payloadConverter = null) =>
            ToRetryPolicy(value);

        internal static Temporalio.Common.RetryPolicy FromRetryPolicyProto(ApiCommon.RetryPolicy value, IPayloadConverter? payloadConverter = null) =>
            FromRetryPolicy(value);

        internal static ApiCommon.Memo ToProto(this IReadOnlyDictionary<string, object?> value, IPayloadConverter? payloadConverter = null) =>
            ToMemo(value, payloadConverter);

        internal static IReadOnlyDictionary<string, object?> FromMemoProto(ApiCommon.Memo value, IPayloadConverter? payloadConverter = null) =>
            value.Fields.ToDictionary(
                item => item.Key,
                item => FromPayload(item.Value, payloadConverter));

        internal static ApiCommon.Priority ToProto(this Temporalio.Common.Priority value, IPayloadConverter? payloadConverter = null) =>
            ToPriority(value);

        internal static Temporalio.Common.Priority FromPriorityProto(ApiCommon.Priority value, IPayloadConverter? payloadConverter = null) =>
            new(
                value.PriorityKey == 0 ? null : value.PriorityKey,
                value.FairnessKey,
                value.FairnessWeight == 0 ? null : (float)value.FairnessWeight);

        internal static ApiWorkflow.VersioningOverride ToProto(this Temporalio.Common.VersioningOverride value, IPayloadConverter? payloadConverter = null) =>
            ToVersioningOverride(value);

        internal static Temporalio.Common.SearchAttributeCollection FromSearchAttributesProto(ApiCommon.SearchAttributes value, IPayloadConverter? payloadConverter = null) =>
            Temporalio.Common.SearchAttributeCollection.FromProto(value);

        internal static Temporalio.Common.VersioningOverride? FromVersioningOverrideProto(ApiWorkflow.VersioningOverride versioningOverride, IPayloadConverter? payloadConverter = null)
        {
            if (versioningOverride.AutoUpgrade)
            {
                return new Temporalio.Common.VersioningOverride.AutoUpgrade();
            }
            if (versioningOverride.Pinned is { } pinned)
            {
                return new Temporalio.Common.VersioningOverride.Pinned(
                    new Temporalio.Common.WorkerDeploymentVersion(
                        pinned.Version.DeploymentName,
                        pinned.Version.BuildId),
                    (Temporalio.Common.VersioningOverride.PinnedOverrideBehavior)pinned.Behavior);
            }
            return null;
        }

        private static ApiCommon.RetryPolicy ToRetryPolicy(Temporalio.Common.RetryPolicy policy)
        {
            var proto = new ApiCommon.RetryPolicy
            {
                InitialInterval = Duration.FromTimeSpan(policy.InitialInterval),
                BackoffCoefficient = policy.BackoffCoefficient,
                MaximumAttempts = policy.MaximumAttempts,
            };
            if (policy.MaximumInterval is { } maximumInterval)
            {
                proto.MaximumInterval = Duration.FromTimeSpan(maximumInterval);
            }
            if (policy.NonRetryableErrorTypes is { Count: > 0 } nonRetryableErrorTypes)
            {
                proto.NonRetryableErrorTypes.AddRange(nonRetryableErrorTypes);
            }
            return proto;
        }

        private static Temporalio.Common.RetryPolicy FromRetryPolicy(ApiCommon.RetryPolicy policy)
        {
            var retryPolicy = new Temporalio.Common.RetryPolicy
            {
                InitialInterval = policy.InitialInterval?.ToTimeSpan() ?? TimeSpan.FromSeconds(1),
                BackoffCoefficient = (float)policy.BackoffCoefficient,
                MaximumAttempts = policy.MaximumAttempts,
            };
            if (policy.MaximumInterval is { } maximumInterval)
            {
                retryPolicy.MaximumInterval = maximumInterval.ToTimeSpan();
            }
            if (policy.NonRetryableErrorTypes.Count > 0)
            {
                retryPolicy.NonRetryableErrorTypes = policy.NonRetryableErrorTypes.ToArray();
            }
            return retryPolicy;
        }

        private static ApiCommon.Memo ToMemo(IReadOnlyDictionary<string, object?> memo, IPayloadConverter? payloadConverter)
        {
            var proto = new ApiCommon.Memo();
            foreach (var item in memo)
            {
                if (item.Value == null)
                {
                    throw new ArgumentException($"Memo value for {item.Key} is null", nameof(memo));
                }
                proto.Fields.Add(item.Key, ToPayload(item.Value, payloadConverter));
            }
            return proto;
        }

        private static ApiCommon.Priority ToPriority(Temporalio.Common.Priority priority) => new()
        {
            PriorityKey = priority.PriorityKey ?? 0,
            FairnessKey = priority.FairnessKey ?? string.Empty,
            FairnessWeight = priority.FairnessWeight ?? 0f,
        };

        private static ApiWorkflow.VersioningOverride ToVersioningOverride(Temporalio.Common.VersioningOverride versioningOverride) =>
            versioningOverride switch
            {
                Temporalio.Common.VersioningOverride.Pinned pinned => new ApiWorkflow.VersioningOverride
                {
#pragma warning disable CS0612
                    Behavior = Temporalio.Api.Enums.V1.VersioningBehavior.Pinned,
                    PinnedVersion = pinned.Version.ToCanonicalString(),
#pragma warning restore CS0612
                    Pinned = new ApiWorkflow.VersioningOverride.Types.PinnedOverride
                    {
                        Version = new ApiDeployment.WorkerDeploymentVersion
                        {
                            DeploymentName = pinned.Version.DeploymentName,
                            BuildId = pinned.Version.BuildId,
                        },
                        Behavior = (ApiWorkflow.VersioningOverride.Types.PinnedOverrideBehavior)pinned.Behavior,
                    },
                },
                Temporalio.Common.VersioningOverride.AutoUpgrade _ => new ApiWorkflow.VersioningOverride
                {
#pragma warning disable CS0612
                    Behavior = Temporalio.Api.Enums.V1.VersioningBehavior.AutoUpgrade,
#pragma warning restore CS0612
                    AutoUpgrade = true,
                },
                _ => throw new ArgumentException("Unknown versioning override type", nameof(versioningOverride)),
            };
    }
}
