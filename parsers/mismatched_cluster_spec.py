from .utils import *

__MISMATCHED_REGEX = r".*\[MISMATCHED_CLUSTER_SPEC].*\nNew code path cluster spec: (.*)" \
                     r"\nLegacy code path cluster spec: (.*).*"


def __dueToCustomTags(mismatched_cluster_specs):
    result = CategorizationResult("Custom tags")
    filtered = []
    for data in mismatched_cluster_specs:
        (legacy_cluster_spec, latest_cluster_spec, metadata) = data
        orgId, pipelineId = metadata['org_id'], metadata['pipeline_id']

        def canonicalize(cluster_spec):
            new_cluster = cluster_spec['new_cluster']
            canonicalized_new_cluster = new_cluster
            if 'custom_tags' in new_cluster:
                custom_tags = new_cluster['custom_tags']
                # sort custom_tags
                canonicalized_new_cluster['custom_tags'] = sorted(custom_tags)
            # remove checksum
            canonicalized_new_cluster['spark_conf']['pipelines.clusterSpecChecksum'] = None
            cluster_spec['new_cluster'] = canonicalized_new_cluster
            return cluster_spec

        canonicalized_legacy = canonicalize(copy.deepcopy(legacy_cluster_spec))
        canonicalized_latest = canonicalize(copy.deepcopy(latest_cluster_spec))
        if canonicalized_legacy == canonicalized_latest:
            policyId = get_policy_id(canonicalized_latest)
            result.addWithPolicyId(orgId, pipelineId, policyId)
        else:
            filtered.append(data)
    return result, filtered


def __dueToAutoscalingRetry(mismatched_cluster_specs):
    filtered = []
    result = CategorizationResult("Autoscaling retry")
    for data in mismatched_cluster_specs:
        (legacy_cluster_spec, latest_cluster_spec, metadata) = data
        orgId, pipelineId = metadata['org_id'], metadata['pipeline_id']

        def canonicalize(cluster_spec):
            clusterSpecResult = copy.deepcopy(cluster_spec)
            new_cluster = clusterSpecResult['new_cluster']
            canonicalized_new_cluster = new_cluster
            if 'autoscale' in new_cluster and 'profile' in new_cluster['autoscale'] and \
                    new_cluster['autoscale']['profile'] == 'ENHANCED':
                # remove autoscale and replace it with fix size
                canonicalized_new_cluster['num_workers'] = new_cluster['autoscale']['min_workers']
                del canonicalized_new_cluster['autoscale']
            # remove checksum
            del canonicalized_new_cluster['spark_conf']['pipelines.clusterSpecChecksum']
            clusterSpecResult['new_cluster'] = canonicalized_new_cluster
            return clusterSpecResult

        canonicalized_legacy = canonicalize(legacy_cluster_spec)
        canonicalized_latest = canonicalize(latest_cluster_spec)
        if canonicalized_legacy == canonicalized_latest:
            policyId = get_policy_id(canonicalized_latest)
            result.addWithPolicyId(orgId, pipelineId, policyId)
        else:

            filtered.append(data)
    return result, filtered


def __dueToDltWorkloadType(mismatched_cluster_specs):
    filtered = []
    result = CategorizationResult("DLT workload type")
    for data in mismatched_cluster_specs:
        (legacy_cluster_spec, latest_cluster_spec, metadata) = data
        orgId, pipelineId = metadata['org_id'], metadata['pipeline_id']

        def canonicalize(clusterSpec):
            clusterSpecCopy = copy.deepcopy(clusterSpec)
            new_cluster = clusterSpec['new_cluster']

            if 'billing_info' in new_cluster and 'dlt_billing_info' in new_cluster[
                'billing_info'] and 'dlt_workload_type' in new_cluster['billing_info']['dlt_billing_info']:
                del clusterSpecCopy['new_cluster']['billing_info']['dlt_billing_info']['dlt_workload_type']
            del clusterSpecCopy['new_cluster']['spark_conf']['pipelines.clusterSpecChecksum']
            return clusterSpecCopy

        if canonicalize(legacy_cluster_spec) == canonicalize(latest_cluster_spec):
            policyId = get_policy_id(legacy_cluster_spec)
            result.addWithPolicyId(orgId, pipelineId, policyId)
        else:
            filtered.append(data)
    return result, filtered


def __dueToSingleNodeClusterInPolicy(truncated_logs, enhanced):
    filtered = []
    result = CategorizationResult(f"Single node cluster in cluster policy ({'Enhanced' if enhanced else 'Legacy'})")
    for data in truncated_logs:
        (legacy_cluster_spec_raw, latest_cluster_spec_raw, metadata) = data
        orgId, pipelineId = metadata['org_id'], metadata['pipeline_id']
        latest_cluster_spec = json.loads(latest_cluster_spec_raw)

        def is_single_node(cluster_spec):
            new_cluster = cluster_spec['new_cluster']
            return new_cluster['num_workers'] == 0

        def is_ea_or_legacy(cluster_spec_raw):
            if enhanced:
                return "pipelines.advancedAutoscaling.enabled" in cluster_spec_raw and \
                    "pipelines.autoscaling.maxNumExecutors" in cluster_spec_raw
            else:
                return "pipelines.advancedAutoscaling.enabled" not in cluster_spec_raw and \
                    "pipelines.autoscaling.maxNumExecutors" not in cluster_spec_raw

        if is_single_node(latest_cluster_spec) == is_ea_or_legacy(legacy_cluster_spec_raw):
            policyId = get_policy_id(latest_cluster_spec)
            result.addWithPolicyId(orgId, pipelineId, policyId)
        else:
            filtered.append(data)
    return result, filtered


def __dueToFirstOnDemand(mismatched_cluster_specs):
    filtered = []
    result = CategorizationResult("First on demand")
    for data in mismatched_cluster_specs:
        (legacy_cluster_spec, latest_cluster_spec, metadata) = data
        orgId, pipelineId = metadata['org_id'], metadata['pipeline_id']

        def removeFirstOnDemand(cluster_spec):
            new_cluster = cluster_spec['new_cluster']
            canonicalized_new_cluster = new_cluster
            if 'aws_attributes' in new_cluster and 'first_on_demand' in new_cluster['aws_attributes']:
                del canonicalized_new_cluster['aws_attributes']['first_on_demand']
            if 'billing_info' in new_cluster and 'dlt_billing_info' in new_cluster[
                'billing_info'] and 'dlt_workload_type' in new_cluster['billing_info']['dlt_billing_info']:
                del canonicalized_new_cluster['billing_info']['dlt_billing_info']['dlt_workload_type']
            # remove checksum
            del canonicalized_new_cluster['spark_conf']['pipelines.clusterSpecChecksum']
            cluster_spec['new_cluster'] = canonicalized_new_cluster
            return cluster_spec

        canonicalized_legacy = removeFirstOnDemand(copy.deepcopy(legacy_cluster_spec))
        canonicalized_latest = removeFirstOnDemand(copy.deepcopy(latest_cluster_spec))
        if canonicalized_legacy == canonicalized_latest:
            policyId = get_policy_id(canonicalized_latest)
            result.addWithPolicyId(orgId, pipelineId, policyId)
        else:
            filtered.append(data)
    return result, filtered


def __dueToInstancePoolAdjustment(mismatched_cluster_specs):
    filtered = []
    result = CategorizationResult("Instance pool adjustment")
    for data in mismatched_cluster_specs:
        (legacy_cluster_spec, latest_cluster_spec, metadata) = data
        orgId, pipelineId = metadata['org_id'], metadata['pipeline_id']

        def removeInstancePoolDefaults(cluster_spec):
            clusterSpecResult = copy.deepcopy(cluster_spec)
            new_cluster = clusterSpecResult['new_cluster']
            canonicalized_new_cluster = new_cluster

            if 'instance_pool_id' not in new_cluster:
                return clusterSpecResult

            if 'node_type_id' in new_cluster:
                del canonicalized_new_cluster['node_type_id']
            if 'enable_elastic_disk' in new_cluster:
                del canonicalized_new_cluster['enable_elastic_disk']
            if 'aws_attributes' in new_cluster:
                canonicalized_new_cluster['aws_attributes'] = {}
            if 'autoscale' in new_cluster and 'profile' in new_cluster['autoscale'] and \
                    new_cluster['autoscale']['profile'] == 'ENHANCED':
                # remove autoscale and replace it with fix size
                canonicalized_new_cluster['num_workers'] = new_cluster['autoscale']['min_workers']
                del canonicalized_new_cluster['autoscale']
            # remove checksum
            del canonicalized_new_cluster['spark_conf']['pipelines.clusterSpecChecksum']
            clusterSpecResult['new_cluster'] = canonicalized_new_cluster
            return clusterSpecResult

        canonicalized_legacy = removeInstancePoolDefaults(legacy_cluster_spec)
        canonicalized_latest = removeInstancePoolDefaults(latest_cluster_spec)
        if canonicalized_legacy == canonicalized_latest:
            policyId = get_policy_id(canonicalized_latest)
            result.addWithPolicyId(orgId, pipelineId, policyId)
        else:
            filtered.append(data)
    return result, filtered


def parseMismatchClusterSpec(response, shardName, matchedClusterSpecSummaryResult, onlyNewSucceedSummaryResult):
    parsedLogs, failedToParse = parseLogsFromResponse(response, __MISMATCHED_REGEX)
    mismatchedClusterSpecs, truncatedLogs = filterTruncatedLogs(parsedLogs)

    # intact logs
    instancePoolAdjustment, ignoreInstancePoolAdjustment = __dueToInstancePoolAdjustment(mismatchedClusterSpecs)
    customTagsResult, ignoreCustomTags = __dueToCustomTags(ignoreInstancePoolAdjustment)
    autoscalingRetryResult, ignoreAutoscaling = __dueToAutoscalingRetry(ignoreCustomTags)
    dltWorkloadTypeResult, ignoreDltWorkloadType = __dueToDltWorkloadType(ignoreAutoscaling)
    firstOnDemandResult, ignoreDueToFirstOnDemand = __dueToFirstOnDemand(ignoreDltWorkloadType)

    # truncated logs
    singleNodeEnhancedResult, ignoreSingleNodeEnhanced = __dueToSingleNodeClusterInPolicy(truncatedLogs,
                                                                                          enhanced=True)
    singleNodeLegacyResult, ignoreSingleNodeLegacy = __dueToSingleNodeClusterInPolicy(ignoreSingleNodeEnhanced,
                                                                                      enhanced=False)

    uncategorizedResult = UncategorizedResult(ignoreDueToFirstOnDemand)
    uncategorizedTruncatedResult = UncategorizedResult(ignoreSingleNodeLegacy, name="Uncategorized truncated logs")

    matchedClusterSpecSummaryResult.add(customTagsResult)
    matchedClusterSpecSummaryResult.add(dltWorkloadTypeResult)

    onlyNewSucceedSummaryResult.add(instancePoolAdjustment)

    metricsResultSummary = SummaryResult("MISMATCHED_CLUSTER_SPEC")
    metricsResultSummary.add(firstOnDemandResult)
    metricsResultSummary.add(singleNodeEnhancedResult)
    metricsResultSummary.add(singleNodeLegacyResult)
    metricsResultSummary.add(uncategorizedResult)
    metricsResultSummary.add(uncategorizedTruncatedResult)

    return f"""shardName={shardName}
{metricsResultSummary.summary()}
=================== DETAILS ===================
{autoscalingRetryResult}
{firstOnDemandResult}
{singleNodeEnhancedResult}
{singleNodeLegacyResult}
{uncategorizedResult}
{uncategorizedTruncatedResult}

-------- Failed to parse --------
count={len(failedToParse)}
log JSON dump={json.dumps(failedToParse)}
""", metricsResultSummary
