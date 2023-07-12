from .utils import *

__FILE_PATH = "data/mismatch_cluster_spec.json"
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

        canonicalized_legacy = canonicalize(legacy_cluster_spec)
        canonicalized_latest = canonicalize(latest_cluster_spec)
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
            new_cluster = cluster_spec['new_cluster']
            canonicalized_new_cluster = new_cluster
            if 'autoscale' in new_cluster and 'profile' in new_cluster['autoscale'] and \
                    new_cluster['autoscale']['profile'] == 'ENHANCED':
                # remove autoscale and replace it with fix size
                canonicalized_new_cluster['num_workers'] = new_cluster['autoscale']['min_workers']
                del canonicalized_new_cluster['autoscale']
            # remove checksum
            del canonicalized_new_cluster['spark_conf']['pipelines.clusterSpecChecksum']
            cluster_spec['new_cluster'] = canonicalized_new_cluster
            return cluster_spec

        canonicalized_legacy = canonicalize(legacy_cluster_spec)
        canonicalized_latest = canonicalize(latest_cluster_spec)
        if canonicalized_legacy == canonicalized_latest:
            policyId = get_policy_id(canonicalized_latest)
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
                    "pipelines.autoscaling.enableAutoscalingProfile" in cluster_spec_raw and \
                    "pipelines.autoscaling.maxNumExecutors" in cluster_spec_raw
            else:
                return "pipelines.advancedAutoscaling.enabled" not in cluster_spec_raw and \
                    "pipelines.autoscaling.enableAutoscalingProfile" in cluster_spec_raw and \
                    "pipelines.autoscaling.maxNumExecutors" not in cluster_spec_raw

        if is_single_node(latest_cluster_spec) == is_ea_or_legacy(legacy_cluster_spec_raw):
            policyId = get_policy_id(latest_cluster_spec)
            result.addWithPolicyId(orgId, pipelineId, policyId)
        else:
            filtered.append(data)
    return result, filtered


def parseMismatchClusterSpec(response, shardName):
    parsedLogs, failedToParse = parseLogsFromResponse(response, __MISMATCHED_REGEX)
    mismatchedClusterSpecs, truncatedLogs = filterTruncatedLogs(parsedLogs)

    # intact logs
    customTagsResult, ignoreCustomTags = __dueToCustomTags(mismatchedClusterSpecs)
    autoscalingRetryResult, ignoreAutoscaling = __dueToAutoscalingRetry(ignoreCustomTags)

    # truncated logs
    singleNodeEnhancedResult, ignoreSingleNodeEnhanced = __dueToSingleNodeClusterInPolicy(truncatedLogs,
                                                                                          enhanced=True)
    singleNodeLegacyResult, ignoreSingleNodeLegacy = __dueToSingleNodeClusterInPolicy(ignoreSingleNodeEnhanced,
                                                                                      enhanced=False)

    uncategorizedResult = UncategorizedResult(ignoreAutoscaling)
    uncategorizedTruncatedResult = UncategorizedResult(ignoreSingleNodeLegacy, name="Uncategorized truncated logs")

    return f"""type=MISMATCHED_CLUSTER_SPEC
shardName={shardName}
total={len(parsedLogs) + len(failedToParse)}
{customTagsResult.__repr__abbr__()}
{autoscalingRetryResult.__repr__abbr__()}
{singleNodeEnhancedResult.__repr__abbr__()}
{singleNodeLegacyResult.__repr__abbr__()}
{uncategorizedResult.__repr__abbr__()}
{uncategorizedTruncatedResult.__repr__abbr__()}

=================== DETAILS ===================
{customTagsResult}
{autoscalingRetryResult}
{singleNodeEnhancedResult}
{singleNodeLegacyResult}
{uncategorizedResult}
{uncategorizedTruncatedResult}

-------- Failed to parse --------
count={len(failedToParse)}
log JSON dump={json.dumps(failedToParse)}
"""
