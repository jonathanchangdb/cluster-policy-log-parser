import json
import re


def convertToSetIfNeeded(x):
    if isinstance(x, set):
        return x
    elif isinstance(x, dict):
        return set(x.keys())
    else:
        set(x)


def get_policy_id(cluster_spec):
    return cluster_spec['new_cluster']['policy_id']


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


def sortDictByValue(x):
    return {k: v for k, v in sorted(x.items(), key=lambda item: item[1], reverse=True)}


def prettyPrintDict(x):
    resultStr = ""
    for (k, v) in x.items():
        resultStr += f"{k}: {v}\n"
    return resultStr


class CategorizationResult:
    def __init__(self, name):
        self.name = name
        self.count = 0
        self.orgIds = {}
        self.pipelineIds = {}
        self.policyIds = {}

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
        return f"""\ncategory={self.name}
count={self.count}
# of orgs={len(self.orgIds)}
# of pipelines={len(self.pipelineIds)}
# of policies={len(self.pipelineIds)}"""

    def __repr__(self):
        self.orgIds = sortDictByValue(self.orgIds)
        self.pipelineIds = sortDictByValue(self.pipelineIds)
        self.policyIds = sortDictByValue(self.policyIds)
        return f"""-------- {self.name} --------
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

    def summaryOneLine(self):
        return f"""total = {self.count}, pipelineIds = {len(self.pipelineIds)}, orgIds = {len(self.orgIds)}, policies = {len(self.policyIds)}\n"""


class DisallowedClusterAttributesResult:
    def __init__(self):
        self.name = "Disallowed cluster attributes"
        self.count = 0
        self.orgIds = {}
        self.pipelineIds = {}
        self.policyIds = {}
        self.attributes = {}

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

        return f"""\ncategory={self.name}
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
        return f"""-------- {self.name} --------
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

    def summaryOneLine(self):
        return f"""total = {self.count}, pipelineIds = {len(self.pipelineIds)}, orgIds = {len(self.orgIds)}, policies = {len(self.policyIds)}\n"""


class UncategorizedResult:
    def __init__(self, uncategorizedLogs, name="Uncategorized"):
        self.name = name
        self.count = 0
        self.orgIds = {}
        self.pipelineIds = {}
        self.policyIds = {}
        self.logs = {}
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

    def summaryOneLine(self):
        return f"""total = {self.count}, pipelineIds = {len(self.pipelineIds)}, orgIds = {len(self.orgIds)}, policies = {len(self.policyIds)}\n"""


class SummaryResult:
    def __init__(self, name):
        self.count = 0
        self.orgIds = set()
        self.pipelineIds = set()
        self.policyIds = set()
        self.results = []
        self.name = name

    def add(self, result):
        self.count += result.count
        self.orgIds = self.orgIds.union(convertToSetIfNeeded(result.orgIds))
        self.pipelineIds = self.pipelineIds.union(convertToSetIfNeeded(result.pipelineIds))
        self.policyIds = self.policyIds.union(convertToSetIfNeeded(result.policyIds))
        self.results.append(result)

    def summary(self):
        return f"""[{self.name}]
======= Summary =======
count={self.count},
total # of org={len(self.orgIds)}
total # of pipeline={len(self.pipelineIds)}
total # of policy={len(self.policyIds)}"""

    def summaryOneLine(self):
        return f"""total = {self.count}, pipelineIds = {len(self.pipelineIds)}, orgIds = {len(self.orgIds)}, policies = {len(self.policyIds)}\n"""


class OverallSummaryResult(SummaryResult):
    def __init__(self, name):
        super().__init__(name)
        self.matchedClusterSpec = SummaryResult("MATCHED_CLUSTER_SPEC")
        self.mismatchedClusterSpec = SummaryResult("MATCHED_CLUSTER_SPEC")
        self.onlyLegacySucceed = SummaryResult("MATCHED_CLUSTER_SPEC")
        self.onlyNewSucceed = SummaryResult("MATCHED_CLUSTER_SPEC")
        self.bothFailed = SummaryResult("MATCHED_CLUSTER_SPEC")

    def add(self, result):
        super().add(result)
        if isinstance(result, OverallSummaryResult):
            self.matchedClusterSpec.add(result.matchedClusterSpec)
            self.mismatchedClusterSpec.add(result.mismatchedClusterSpec)
            self.onlyLegacySucceed.add(result.onlyLegacySucceed)
            self.onlyNewSucceed.add(result.onlyNewSucceed)
            self.bothFailed.add(result.bothFailed)
        else:
            if result.name == "MATCHED_CLUSTER_SPEC":
                self.matchedClusterSpec.add(result)
            elif result.name == "MISMATCHED_CLUSTER_SPEC":
                self.mismatchedClusterSpec.add(result)
            elif result.name == "ONLY_LEGACY_CODE_PATH_SUCCEED":
                self.onlyLegacySucceed.add(result)
            elif result.name == "ONLY_NEW_CODE_PATH_SUCCEED":
                self.onlyNewSucceed.add(result)
            elif result.name == "BOTH_LEGACY_AND_NEW_CODE_PATH_FAILED":
                self.bothFailed.add(result)

    def summaryPerShard(self):
        return f"""[{self.name}]
======= Summary =======
count={self.count},
total # of org={len(self.orgIds)}
total # of pipeline={len(self.pipelineIds)}
total # of policy={len(self.policyIds)}
[MATCHED_CLUSTER_SPEC]:
total = {self.matchedClusterSpec.count}, pipelineIds = {len(self.matchedClusterSpec.pipelineIds)}, orgIds = {len(self.matchedClusterSpec.orgIds)}, policies = {len(self.matchedClusterSpec.policyIds)}
[MISMATCHED_CLUSTER_SPEC]:
total = {self.mismatchedClusterSpec.count}, pipelineIds = {len(self.mismatchedClusterSpec.pipelineIds)}, orgIds = {len(self.mismatchedClusterSpec.orgIds)}, policies = {len(self.mismatchedClusterSpec.policyIds)}
[ONLY_LEGACY_CODE_PATH_SUCCEED]:
total = {self.onlyLegacySucceed.count}, pipelineIds = {len(self.onlyLegacySucceed.pipelineIds)}, orgIds = {len(self.onlyLegacySucceed.orgIds)}, policies = {len(self.onlyLegacySucceed.policyIds)}
[ONLY_NEW_CODE_PATH_SUCCEED]:
total = {self.onlyNewSucceed.count}, pipelineIds = {len(self.onlyNewSucceed.pipelineIds)}, orgIds = {len(self.onlyNewSucceed.orgIds)}, policies = {len(self.onlyNewSucceed.policyIds)}
[BOTH_LEGACY_AND_NEW_CODE_PATH_FAILED]:
total = {self.bothFailed.count}, pipelineIds = {len(self.bothFailed.pipelineIds)}, orgIds = {len(self.bothFailed.orgIds)}, policies = {len(self.bothFailed.policyIds)}
"""

    def __repr__(self):
        resultStr = self.summaryPerShard()
        resultStr += "\n\n======= Details =======\n"
        for result in self.results:
            resultStr += f"({result.name})\n"
            resultStr += result.summaryOneLine()
            for subResult in result.results:
                resultStr += f"- {subResult.name} " + subResult.summaryOneLine()
            resultStr += '\n'
        return resultStr
