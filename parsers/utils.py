import json
import re
import logging


def get_policy_id(cluster_spec):
    return cluster_spec['new_cluster']['policy_id']


def get_policy_id_from_truncated(log):
    try:
        regex = r'.*"policy_id":"(.{16})",'
        result = re.search(regex, log)
        return result.group(1)
    except Exception as ex:
        logging.error(f"[get_policy_id_from_truncated] parse policy_id failed. log={log}", ex)
        return None


def get_org_id(log):
    regex = r'deltaPipelinesOrgId=([0-9]+)'
    result = re.search(regex, log)
    return result.group(1)


def get_pipeline_id(log):
    regex = r'deltaPipelinesPipelineId=(.{36})'
    result = re.search(regex, log)
    return result.group(1)


def get_message_texts(log):
    hits = log['rawResponse']['hits']['hits']
    return [hit['_source']['messageText'] for hit in hits]


def get_raw_logs(filename):
    with open(filename) as file:
        file_content_raw = file.read()
    all_logs = json.loads(file_content_raw, strict=False)
    return get_message_texts(all_logs)


def get_raw_logs_from_response(response):
    all_logs = json.loads(response, strict=False)
    return get_message_texts(all_logs)


def filterTruncatedLogs(parsedLogs):
    intactLogs, truncatedLogs = [], []
    for data in parsedLogs:
        (x, y, metadata) = data
        try:
            intactLogs.append((json.loads(x), json.loads(y), metadata))
        except:
            truncatedLogs.append(data)
    return intactLogs, truncatedLogs


def parse_logs(file_path, regex):
    logs = get_raw_logs(file_path)
    parsed_logs = []
    failed_to_parse = []
    metadata = {}

    for log in logs:
        try:
            result = re.search(regex, log)
            new_code_path_result = result.group(1)
            legacy_code_path_result = result.group(2)
            metadata['org_id'] = get_org_id(log)
            metadata['pipeline_id'] = get_pipeline_id(log)
            parsed_logs.append((legacy_code_path_result, new_code_path_result, metadata))
        except:
            failed_to_parse.append(log)

    return parsed_logs, failed_to_parse


def parseLogsFromResponse(response, regex):
    logs = get_raw_logs_from_response(response)
    parsed_logs = []
    failed_to_parse = []
    metadata = {}

    for log in logs:
        try:
            result = re.search(regex, log)
            new_code_path_result = result.group(1)
            legacy_code_path_result = result.group(2)
            metadata['org_id'] = get_org_id(log)
            metadata['pipeline_id'] = get_pipeline_id(log)
            parsed_logs.append((legacy_code_path_result, new_code_path_result, metadata))
        except:
            failed_to_parse.append(log)

    return parsed_logs, failed_to_parse


def parseUncategorized(uncategorized):
    orgIds, pipelineIds, logs = {}, {}, {}
    for (legacy, latest, metadata) in uncategorized:
        orgId = metadata['org_id']
        pipelineId = metadata['pipeline_id']
        logKey = f"{orgId}_{pipelineId}"

        orgIds.setdefault(orgId, 0)
        pipelineIds.setdefault(pipelineId, 0)
        logs.setdefault(logKey, [])

        orgIds[orgId] += 1
        pipelineIds[pipelineId] += 1
        logs[logKey].append({"legacy": legacy, "latest": latest})

    return orgIds, pipelineIds, logs


def sortDictByValue(x):
    return {k: v for k, v in sorted(x.items(), key=lambda item: item[1], reverse=True)}


def prettyPrintDict(x):
    resultStr = ""
    for (k, v) in x.items():
        resultStr += f"{k}: {v}\n"
    return resultStr


class CategorizationResult:
    category = None
    count = 0
    orgIds = {}
    pipelineIds = {}
    policyIds = {}

    def __init__(self, category):
        self.category = category

    def addWithPolicyId(self, orgId, pipelineId, policyId):
        self.policyIds.setdefault(policyId, 0)
        self.policyIds[policyId] += 1
        self.add(orgId, pipelineId)

    def add(self, orgId, pipelineId):
        self.count += 1
        self.orgIds.setdefault(orgId, 0)
        self.pipelineIds.setdefault(pipelineId, 0)

        self.orgIds[orgId] += 1
        self.pipelineIds[pipelineId] += 1

    def __repr__abbr__(self):
        self.orgIds = sortDictByValue(self.orgIds)
        self.pipelineIds = sortDictByValue(self.pipelineIds)
        self.policyIds = sortDictByValue(self.policyIds)
        return f"""\ncategory={self.category}
count={self.count}
# of orgs={len(self.orgIds)}
# of pipelines={len(self.pipelineIds)}
# of policies={len(self.pipelineIds)}"""

    def __repr__(self):
        self.orgIds = sortDictByValue(self.orgIds)
        self.pipelineIds = sortDictByValue(self.pipelineIds)
        self.policyIds = sortDictByValue(self.policyIds)
        return f"""-------- {self.category} --------
count={self.count}
# of orgs={len(self.orgIds)}
# of pipelines={len(self.pipelineIds)}
# of policies={len(self.pipelineIds)}

OrgIds details:
{prettyPrintDict(self.orgIds)}
PipelineIds details:
{prettyPrintDict(self.pipelineIds)}
PolicyIds details:
{prettyPrintDict(self.policyIds)}
JSON dump:
orgIds={json.dumps(self.orgIds)}
pipelineIds={json.dumps(self.pipelineIds)}
policyIds={json.dumps(self.policyIds)}
"""


class DisallowedClusterAttributesResult:
    category = "Disallowed cluster attributes"
    count = 0
    orgIds = {}
    pipelineIds = {}
    policyIds = {}
    attributes = {}

    def add(self, attribute, orgId, pipelineId, policyId):
        self.count += 1
        self.orgIds.setdefault(orgId, 0)
        self.policyIds.setdefault(policyId, 0)
        self.pipelineIds.setdefault(pipelineId, 0)
        self.attributes.setdefault(attribute, {
            "count": 0,
            "orgIds": set(),
            "pipelineIds": set(),
            "policyIds": set()
        })

        self.orgIds[orgId] += 1
        self.policyIds[policyId] += 1
        self.pipelineIds[pipelineId] += 1

        self.attributes[attribute]["count"] += 1
        self.attributes[attribute]["orgIds"].add(orgId)
        self.attributes[attribute]["policyIds"].add(policyId)
        self.attributes[attribute]["pipelineIds"].add(pipelineId)

    def __printAttributes(self):
        result = ""
        for (attribute, stats) in self.attributes.items():
            result += f"[{attribute}] count={stats['count']},orgIds={stats['orgIds']},pipelineIds={stats['pipelineIds']},policyIds={stats['policyIds']}\n"

    def __serializableAttributes(self):
        newAttributes = {}
        for (attribute, stats) in self.attributes.items():
            newAttributes[attribute] = {
                "count": stats["count"],
                "orgIds": list(stats["orgIds"]),
                "pipelineIds": list(stats["pipelineIds"]),
                "policyIds": list(stats["policyIds"])
            }
        return newAttributes

    def __repr__abbr__(self):
        self.orgIds = sortDictByValue(self.orgIds)
        self.pipelineIds = sortDictByValue(self.pipelineIds)
        self.policyIds = sortDictByValue(self.policyIds)

        return f"""\ncategory={self.category}
count={self.count}
# of orgs={len(self.orgIds)}
# of pipelines={len(self.pipelineIds)}
# of policies={len(self.pipelineIds)}
attributes={self.__printAttributes()}
"""

    def __repr__(self):
        self.orgIds = sortDictByValue(self.orgIds)
        self.pipelineIds = sortDictByValue(self.pipelineIds)
        self.policyIds = sortDictByValue(self.policyIds)
        return f"""-------- {self.category} --------
count={self.count}
# of orgs={len(self.orgIds)}
# of pipelines={len(self.pipelineIds)}
# of policies={len(self.pipelineIds)}
attributes={self.__printAttributes()}

OrgIds details:
{prettyPrintDict(self.orgIds)}
PipelineIds details:
{prettyPrintDict(self.pipelineIds)}
PolicyIds details:
{prettyPrintDict(self.policyIds)}
PolicyIds details:
{prettyPrintDict(self.attributes)}
JSON dump:
orgIds={json.dumps(self.orgIds)}
pipelineIds={json.dumps(self.pipelineIds)}
policyIds={json.dumps(self.policyIds)}
attributes={json.dumps(self.__serializableAttributes())}
"""


class UncategorizedResult:
    name = None
    count = 0
    orgIds = {}
    pipelineIds = {}
    logs = {}

    def __init__(self, uncategorizedLogs, name="Uncategorized"):
        self.name = name
        for (legacy, latest, metadata) in uncategorizedLogs:
            orgId = metadata['org_id']
            pipelineId = metadata['pipeline_id']
            logKey = f"{orgId}_{pipelineId}"

            self.logs.setdefault(logKey, [])
            self.orgIds.setdefault(orgId, 0)
            self.pipelineIds.setdefault(pipelineId, 0)

            self.orgIds[orgId] += 1
            self.pipelineIds[pipelineId] += 1
            self.logs[logKey].append({"legacy": legacy, "latest": latest})

    def __repr__abbr__(self):
        return f"""\nname={self.name}
count={self.count}
# of orgs={len(self.orgIds)}
# of pipelines={len(self.pipelineIds)}"""

    def __repr__(self):
        self.orgIds = sortDictByValue(self.orgIds)
        self.pipelineIds = sortDictByValue(self.pipelineIds)
        return f"""-------- {self.name} --------
count={self.count}
# of orgs={len(self.orgIds)}
# of pipelines={len(self.pipelineIds)}
# of policies={len(self.pipelineIds)}

OrgIds details:
{prettyPrintDict(self.orgIds)}
PipelineIds details:
{prettyPrintDict(self.pipelineIds)}
JSON dump:
orgIds={json.dumps(self.orgIds)}
pipelineIds={json.dumps(self.pipelineIds)}
logs={json.dumps(self.logs)}
"""
