from .utils import *

__ONLY_LEGACY_SUCCEED_REGEX = r".*\[ONLY_LEGACY_CODE_PATH_SUCCEED].*\nNew code path failure: \n([\S\s]*)" \
                              r"\nLegacy code path cluster spec: (.*).*"


def __dueToApplyPolicyRpc(only_legacy_succeed_specs):
    result = CategorizationResult("ApplyPolicy RPC validation")
    error = 'INVALID_PARAMETER_VALUE: Exactly 1 of virtual_cluster_size, num_workers or autoscale must be specified.'
    filtered = []
    for data in only_legacy_succeed_specs:
        (legacyClusterSpec, latest_error, metadata) = data
        policyId = get_policy_id(legacyClusterSpec)
        orgId, pipelineId = metadata['org_id'], metadata['pipeline_id']
        if error in latest_error:
            result.addWithPolicyId(orgId, pipelineId, policyId)
        elif 'TEMPORARILY_UNAVAILABLE' in latest_error or 'INTERNAL_ERROR' in latest_error:
            continue
        else:
            filtered.append(data)
    return result, filtered


def __dueToEnableElasticDisk(only_legacy_succeed_specs):
    filtered = []
    result = CategorizationResult("enable_elastic_disk=True")
    regex = r"The cluster policy for the \"default\" cluster in the pipeline settings is not\n" \
            r"compatible with the Delta Live Tables because of the following error:[\s]+Cluster " \
            r"attribute (.*) for cluster 'default' is not allowed for a pipeline.*"
    for data in only_legacy_succeed_specs:
        (legacy_cluster_spec, latest_error, metadata) = data
        policyId = get_policy_id(legacy_cluster_spec)
        orgId, pipelineId = metadata['org_id'], metadata['pipeline_id']

        regex_result = re.search(regex, latest_error)
        if regex_result is not None:
            attribute = regex_result.group(1)[1:-1]
            policyValue = legacy_cluster_spec['new_cluster'][attribute]
            if attribute == 'enable_elastic_disk' and policyValue:
                result.addWithPolicyId(orgId, pipelineId, policyId)
            else:
                filtered.append(data)
        else:
            filtered.append(data)

    return result, filtered


def __dueToDisallowedClusterAttributes(only_legacy_succeed_specs):
    filtered = []
    result = DisallowedClusterAttributesResult()
    regex = r"The cluster policy for the \"default\" cluster in the pipeline settings is not\n" \
            r"compatible with the Delta Live Tables because of the following error:[\s]+Cluster " \
            r"attribute (.*) for cluster 'default' is not allowed for a pipeline.*"
    for data in only_legacy_succeed_specs:
        (legacy_cluster_spec, latest_error, metadata) = data
        policyId = get_policy_id(legacy_cluster_spec)
        orgId, pipelineId = metadata['org_id'], metadata['pipeline_id']

        regex_result = re.search(regex, latest_error)
        if regex_result is not None:
            attribute = regex_result.group(1)[1:-1]
            policyValue = legacy_cluster_spec['new_cluster'][attribute]
            result.add(attribute, policyValue, orgId, pipelineId, policyId)
        else:
            filtered.append(data)

    return result, filtered


def parseOnlyLegacySucceed(response, shardName):
    parsedLogs, failedToParse = parseLogsFromResponse(response, __ONLY_LEGACY_SUCCEED_REGEX)
    onlyLegacySucceedLogs = [(json.loads(x), y, metadata) for (x, y, metadata) in parsedLogs]
    applyPolicyResult, ignoreApplyPolicy = __dueToApplyPolicyRpc(onlyLegacySucceedLogs)
    enableElasticDiskResult, ignoreEnableElasticDisk = __dueToEnableElasticDisk(ignoreApplyPolicy)
    disallowedAttrsResult, ignoreDisallowedAttrs = __dueToDisallowedClusterAttributes(ignoreEnableElasticDisk)

    uncategorized = UncategorizedResult(ignoreDisallowedAttrs)

    metricsResultSummary = SummaryResult("ONLY_LEGACY_CODE_PATH_SUCCEED")
    metricsResultSummary.add(enableElasticDiskResult)
    metricsResultSummary.add(disallowedAttrsResult)
    metricsResultSummary.add(uncategorized)

    return f"""shardName={shardName}
{metricsResultSummary.summary()}
=================== DETAILS ===================
{applyPolicyResult}
{enableElasticDiskResult}
{disallowedAttrsResult}
{uncategorized}

-------- Failed to parse --------
count={len(failedToParse)}
log JSON dump={json.dumps(failedToParse)}
""", metricsResultSummary
