from .utils import *


def __parse(response):
    logs = get_raw_logs_from_response(response)
    orgIds = {}
    pipelineIds = {}

    for log in logs:
        orgId = get_org_id(log)
        pipelineId = get_pipeline_id(log)
        if orgId not in orgIds:
            orgIds[orgId] = 0
        if pipelineId not in pipelineIds:
            pipelineIds[pipelineId] = 0
        orgIds[orgId] += 1
        pipelineIds[pipelineId] += 1

    return logs, orgIds, pipelineIds


def parseMatchedClusterSpec(response, shardName):
    logs, orgIds, pipelineIds = __parse(response)
    metricsResultSummary = SummaryResult("MATCHED_CLUSTER_SPEC")
    metricsResultSummary.count = len(logs)
    metricsResultSummary.orgIds = orgIds
    metricsResultSummary.pipelineIds = pipelineIds

    return f"""shardName={shardName}
{metricsResultSummary.summary()}
""", metricsResultSummary
