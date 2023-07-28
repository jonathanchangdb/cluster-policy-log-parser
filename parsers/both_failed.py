from .utils import *

__BOTH_CODE_PATH_FAILED = r".*\[BOTH_OLD_AND_NEW_CODE_PATH_FAILED].*\nNew code path failure: \n" \
                          r"([\s\S]*)Legacy code path failure: ([\s\S]*).*"


def __hasSameErrorMessage(both_code_path_failed):
    filtered = []
    result = CategorizationResult("Same error message")
    latest_regex = r".*:[\s ]+(.*)"
    legacy_regex = r"(.*)"
    for data in both_code_path_failed:
        (legacy_error_raw, latest_error_raw, metadata) = data
        orgId, pipelineId = metadata['org_id'], metadata['pipeline_id']
        latest_error = re.search(latest_regex, latest_error_raw.strip()).group(1).strip()
        legacy_error = re.search(legacy_regex, legacy_error_raw.strip()).group(1).strip()
        if legacy_error == latest_error:
            result.add(orgId, pipelineId)
        elif legacy_error == f'INVALID_PARAMETER_VALUE: {latest_error}':
            result.add(orgId, pipelineId)
        elif "RESOURCE_DOES_NOT_EXIST: Can't find a cluster policy with id" in legacy_error:
            result.add(orgId, pipelineId)
        elif "None.get" in legacy_error or "Validation failed for node_type_id, the value must be dlt" in legacy_error:
            continue
        elif "REQUEST_LIMIT_EXCEEDED" in latest_error or "REQUEST_LIMIT_EXCEEDED" in legacy_error:
            continue
        else:
            filtered.append(data)

    return result, filtered


def __dueToLatestErrorIsSubset(both_code_path_failed, shardName):
    filtered = []
    result = CategorizationResult("Latest error is subset of legacy")
    latest_regex = r".*:[\s ]+(.*)"
    legacy_regex = r"(.*)"
    for data in both_code_path_failed:
        (legacy_error_raw, latest_error_raw, metadata) = data
        orgId, pipelineId = metadata['org_id'], metadata['pipeline_id']
        latest_error = re.search(latest_regex, latest_error_raw.strip()).group(1).strip()
        legacy_error = re.search(legacy_regex, legacy_error_raw.strip()).group(1).strip()
        if latest_error.startswith('INVALID_PARAMETER_VALUE: '):
            latest_error = latest_error.replace('INVALID_PARAMETER_VALUE: ', '')
        if legacy_error.startswith('INVALID_PARAMETER_VALUE: '):
            legacy_error = legacy_error.replace('INVALID_PARAMETER_VALUE: ', '')
        if set(latest_error.split('; ')).issubset(set(legacy_error.split('; '))):
            result.add(orgId, pipelineId)
        else:
            latest_error = re.search(latest_regex, latest_error_raw.strip()).group(0).strip()
            if latest_error.startswith('INVALID_PARAMETER_VALUE: '):
                latest_error = latest_error.replace('INVALID_PARAMETER_VALUE: ', '')
            if set(latest_error.split('; ')).issubset(set(legacy_error.split('; '))):
                result.add(orgId, pipelineId)
            else:
                if shardName == 'az-westus':
                    print(f"legacy_error:{legacy_error.split('; ')},latest_error:{latest_error.strip('; ')}")
                filtered.append(data)

    return result, filtered


def __dueToDisallowedClusterAttributes(both_code_path_failed):
    filtered = []
    result = DisallowedClusterAttributesResult()
    regex = r"Cluster attribute (.*) for cluster 'default' is not allowed for a pipeline.*"
    latest_regex = r".*:[\s ]+(.*)[\s ]+.*"
    for data in both_code_path_failed:
        (legacy_error_raw, latest_error_raw, metadata) = data
        orgId, pipelineId = metadata['org_id'], metadata['pipeline_id']
        latest_error = re.search(latest_regex, latest_error_raw.strip()).group(1).strip()
        if "for cluster 'default' is not allowed for a pipeline" in latest_error:
            attribute = re.search(regex, latest_error).group(1)[1:-1]
            result.addWithoutPolicyId(attribute, orgId, pipelineId)
        else:
            filtered.append(data)

    return result, filtered


def parseBothCodePathFailed(response, shardName):
    bothFailed, failedToParse = parseLogsFromResponse(response, regex=__BOTH_CODE_PATH_FAILED)
    sameErrorMessageResult, ignoreSameMessage = __hasSameErrorMessage(bothFailed)
    latestErrorIsSubset, ignoreLatestErrorIsSubset = __dueToLatestErrorIsSubset(ignoreSameMessage, shardName)
    disallowedClusterAttributes, ignoreDisallowedClusterAttributes = __dueToDisallowedClusterAttributes(
        ignoreLatestErrorIsSubset)

    uncategorizedResult = UncategorizedResult(ignoreDisallowedClusterAttributes)

    metricsResultSummary = SummaryResult("BOTH_LEGACY_AND_NEW_CODE_PATH_FAILED")
    metricsResultSummary.add(sameErrorMessageResult)
    metricsResultSummary.add(latestErrorIsSubset)
    metricsResultSummary.add(uncategorizedResult)

    return f"""shardName={shardName}
{metricsResultSummary.summary()}

=================== DETAILS ===================
{sameErrorMessageResult}
{latestErrorIsSubset}
{uncategorizedResult}

-------- Failed to parse --------
count={len(failedToParse)}
log JSON dump={json.dumps(failedToParse)}
""", metricsResultSummary
