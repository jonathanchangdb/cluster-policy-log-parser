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

    return f"""type=MATCHED_CLUSTER_SPEC
shardName={shardName}
total={len(logs)}
# of orgs={len(orgIds)}
# of pipelines={len(pipelineIds)}

OrgIds details:
{prettyPrintDict(sortDictByValue(orgIds))}
PipelineIds details:
{prettyPrintDict(sortDictByValue(pipelineIds))}
    
JSON dump:
orgIds:{json.dumps(orgIds)}
pipelines:{json.dumps(pipelineIds)}
"""
