from .utils import *

__ONLY_NEW_SUCCEED_REGEX = r".*\[ONLY_NEW_CODE_PATH_SUCCEED].*\nNew code path cluster spec: " \
                           r"(.*)\nLegacy code path failure: ([\S\s]*).*"


def __dueToNodeTypeId(only_legacy_succeed_specs):
    filtered = []
    result = CategorizationResult("Node Type ID")
    error = 'INVALID_PARAMETER_VALUE: Validation failed for node_type_id'
    for data in only_legacy_succeed_specs:
        (legacyError, latestClusterSpec, metadata) = data
        policyId = get_policy_id(latestClusterSpec)
        orgId, pipelineId = metadata['org_id'], metadata['pipeline_id']
        if error in legacyError:
            result.addWithPolicyId(orgId, pipelineId, policyId)
        elif 'REQUEST_LIMIT_EXCEEDED' in legacyError:
            continue
        else:
            filtered.append(data)
    return result, filtered


def parseOnlyNewSucceed(response, shardName):
    parsedLogs, failedToParse = parseLogsFromResponse(response, __ONLY_NEW_SUCCEED_REGEX)
    onlyNewSucceedLogs = [(x, json.loads(y), metadata) for (x, y, metadata) in parsedLogs]
    nodeTypeIdResult, ignoreNodeTypeId = __dueToNodeTypeId(onlyNewSucceedLogs)

    uncategorized = UncategorizedResult(ignoreNodeTypeId)
    metricsResultSummary = SummaryResult("ONLY_NEW_CODE_PATH_SUCCEED")
    metricsResultSummary.add(nodeTypeIdResult)
    metricsResultSummary.add(uncategorized)

    return f"""shardName={shardName}
{metricsResultSummary.summary()}
=================== DETAILS ===================
{nodeTypeIdResult}
{uncategorized}

-------- Failed to parse --------
count={len(failedToParse)}
log JSON dump={json.dumps(failedToParse)}
""", metricsResultSummary
