from .utils import *

__BOTH_CODE_PATH_FAILED = r".*\[BOTH_OLD_AND_NEW_CODE_PATH_FAILED].*\nNew code path failure: \n" \
                          r"([\s\S]*)Legacy code path failure: ([\s\S]*).*"


def __hasSameErrorMessage(both_code_path_failed):
    filtered = []
    result = CategorizationResult("Same error message")
    latest_regex = r".*:[\s ]+(.*)[\s ]+.*"
    legacy_regex = r"(.*)"
    for data in both_code_path_failed:
        (legacy_error_raw, latest_error_raw, metadata) = data
        orgId, pipelineId = metadata['org_id'], metadata['pipeline_id']
        latest_error = re.search(latest_regex, latest_error_raw).group(1)
        legacy_error = re.search(legacy_regex, legacy_error_raw).group(1)
        if legacy_error.strip() == latest_error.strip():
            result.add(orgId, pipelineId)
        else:
            filtered.append(data)

    return result, filtered


def parseBothCodePathFailed(response, shardName):
    bothFailed, failedToParse = parseLogsFromResponse(response, regex=__BOTH_CODE_PATH_FAILED)
    sameErrorMessageResult, ignoreSameMessage = __hasSameErrorMessage(bothFailed)

    uncategorizedResult = UncategorizedResult(ignoreSameMessage)

    metricsResultSummary = SummaryResult("BOTH_LEGACY_AND_NEW_CODE_PATH_FAILED")
    metricsResultSummary.add(sameErrorMessageResult)
    metricsResultSummary.add(uncategorizedResult)

    return f"""shardName={shardName}
{metricsResultSummary.summary()}

=================== DETAILS ===================
{uncategorizedResult}

-------- Failed to parse --------
count={len(failedToParse)}
log JSON dump={json.dumps(failedToParse)}
""", metricsResultSummary
