import copy
import json
import logging
import re

logging.basicConfig(level=logging.INFO)


def convertToSetIfNeeded(x):
    if isinstance(x, set):
        return x
    elif isinstance(x, dict):
        return set(x.keys())
    else:
        set(x)


def findOne(cond, data):
    for x in data:
        if cond(x):
            return x


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


def mergeTwoDictionary(dict1, dict2):
    result = copy.deepcopy(dict1)
    for key in dict2:
        if key in result:
            result[key] += dict2[key]
        else:
            result[key] = dict2[key]
    return result


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

    def addWithoutPolicyId(self, attribute, orgId, pipelineId):
        self.count += 1
        self.orgIds.setdefault(orgId, 0)
        self.pipelineIds.setdefault(pipelineId, 0)
        self.attributes.setdefault(attribute, {
            "count": 0,
            "orgIds": set(),
            "pipelineIds": set(),
            "policyIds": set()
        })

        self.orgIds[orgId] += 1
        self.pipelineIds[pipelineId] += 1

        self.attributes[attribute]["count"] += 1
        self.attributes[attribute]["orgIds"].add(orgId)
        self.attributes[attribute]["pipelineIds"].add(pipelineId)

    def add(self, attribute, policyValue, orgId, pipelineId, policyId):
        self.count += 1
        self.orgIds.setdefault(orgId, 0)
        self.policyIds.setdefault(policyId, 0)
        self.pipelineIds.setdefault(pipelineId, 0)
        self.attributes.setdefault(f'{attribute}_{policyValue}', {
            "count": 0,
            "orgIds": set(),
            "pipelineIds": set(),
            "policyIds": set()
        })

        self.orgIds[orgId] += 1
        self.policyIds[policyId] += 1
        self.pipelineIds[pipelineId] += 1

        self.attributes[f'{attribute}_{policyValue}']["count"] += 1
        self.attributes[f'{attribute}_{policyValue}']["orgIds"].add(orgId)
        self.attributes[f'{attribute}_{policyValue}']["policyIds"].add(policyId)
        self.attributes[f'{attribute}_{policyValue}']["pipelineIds"].add(pipelineId)

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

            self.count += 1
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
        self.orgIds = {}
        self.pipelineIds = {}
        self.policyIds = {}
        self.attributes = {}
        self.results = []
        self.name = name

    def add(self, result):
        self.count += result.count
        self.orgIds = mergeTwoDictionary(self.orgIds, result.orgIds)
        self.pipelineIds = mergeTwoDictionary(self.pipelineIds, result.pipelineIds)
        self.policyIds = mergeTwoDictionary(self.policyIds, result.policyIds)
        self.results.append(result)

    def summary(self):
        return f"""[{self.name}]
======= Summary =======
count={self.count},
total # of org={len(self.orgIds)}
total # of pipeline={len(self.pipelineIds)}
total # of policy={len(self.policyIds)}
"""

    def summaryOneLine(self):
        return f"""total = {self.count}, pipelineIds = {len(self.pipelineIds)}, orgIds = {len(self.orgIds)}, policies = {len(self.policyIds)}\n"""


class ShardSummaryResult(SummaryResult):
    def __init__(self, name):
        super().__init__(name)
        self.matchedClusterSpec = SummaryResult("MATCHED_CLUSTER_SPEC")
        self.mismatchedClusterSpec = SummaryResult("MISMATCHED_CLUSTER_SPEC")
        self.onlyLegacySucceed = SummaryResult("ONLY_LEGACY_CODE_PATH_SUCCEED")
        self.onlyNewSucceed = SummaryResult("ONLY_NEW_CODE_PATH_SUCCEED")
        self.bothFailed = SummaryResult("BOTH_LEGACY_AND_NEW_CODE_PATH_FAILED")

    def add(self, result):
        super().add(result)
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

    @staticmethod
    def mergeSubResults(metricSummaryResult):
        merged = SummaryResult(metricSummaryResult.name)
        summaryResults = metricSummaryResult.results

        if len(metricSummaryResult.results) > 0:
            for categoryResult in summaryResults[0].results:
                merged.add(categoryResult)

            for summaryResult in summaryResults[1:]:
                categoryResults = summaryResult.results
                for mergedCategoryResult in merged.results:
                    toMerge = findOne(lambda x: x.name == mergedCategoryResult.name, categoryResults)
                    mergedCategoryResult.count += toMerge.count
                    mergedCategoryResult.orgIds = mergeTwoDictionary(mergedCategoryResult.orgIds, toMerge.orgIds)
                    mergedCategoryResult.pipelineIds = mergeTwoDictionary(mergedCategoryResult.pipelineIds,
                                                                          toMerge.pipelineIds)
                    mergedCategoryResult.policyIds = mergeTwoDictionary(mergedCategoryResult.policyIds,
                                                                        toMerge.policyIds)

        merged.count = metricSummaryResult.count
        merged.orgIds = metricSummaryResult.orgIds
        merged.pipelineIds = metricSummaryResult.pipelineIds
        merged.policyIds = metricSummaryResult.policyIds

        return merged

    @staticmethod
    def summarySubResults(result, detailed=False):
        resultStr = result.summaryOneLine()
        for subResult in result.results:
            resultStr += f"- {subResult.name} " + subResult.summaryOneLine()
            if detailed:
                for categoryResult in subResult.results:
                    resultStr += f"    - {categoryResult.name} " + categoryResult.summaryOneLine()
        return resultStr + '\n'

    def summaryPerShard(self):
        self.matchedClusterSpec = ShardSummaryResult.mergeSubResults(self.matchedClusterSpec)
        self.mismatchedClusterSpec = ShardSummaryResult.mergeSubResults(self.mismatchedClusterSpec)
        self.onlyNewSucceed = ShardSummaryResult.mergeSubResults(self.onlyNewSucceed)
        self.onlyLegacySucceed = ShardSummaryResult.mergeSubResults(self.onlyLegacySucceed)
        self.bothFailed = ShardSummaryResult.mergeSubResults(self.bothFailed)
        return f"""[{self.name}]
======= Summary =======
count={self.count},
total # of org={len(self.orgIds)}
total # of pipeline={len(self.pipelineIds)}
total # of policy={len(self.policyIds)}
[MATCHED_CLUSTER_SPEC]:
{self.summarySubResults(self.matchedClusterSpec)}
[MISMATCHED_CLUSTER_SPEC]:
{self.summarySubResults(self.mismatchedClusterSpec)}
[ONLY_LEGACY_CODE_PATH_SUCCEED]:
{self.summarySubResults(self.onlyLegacySucceed)}
[ONLY_NEW_CODE_PATH_SUCCEED]:
{self.summarySubResults(self.onlyNewSucceed)}
[BOTH_LEGACY_AND_NEW_CODE_PATH_FAILED]:
{self.summarySubResults(self.bothFailed)}
    """

    def __repr__(self):
        resultStr = self.summaryPerShard()
        resultStr += "\n\n======= Details =======\n"
        for result in self.results:
            resultStr += f"({result.name})\n"
            resultStr += self.summarySubResults(result)
        return resultStr


class GlobalSummaryResult(SummaryResult):
    def __init__(self, name):
        super().__init__(name)
        self.matchedClusterSpec = SummaryResult("MATCHED_CLUSTER_SPEC")
        self.mismatchedClusterSpec = SummaryResult("MISMATCHED_CLUSTER_SPEC")
        self.onlyLegacySucceed = SummaryResult("ONLY_LEGACY_CODE_PATH_SUCCEED")
        self.onlyNewSucceed = SummaryResult("ONLY_NEW_CODE_PATH_SUCCEED")
        self.bothFailed = SummaryResult("BOTH_LEGACY_AND_NEW_CODE_PATH_FAILED")

    def add(self, shardSummaryResult: ShardSummaryResult):
        super().add(shardSummaryResult)
        self.matchedClusterSpec.add(shardSummaryResult.matchedClusterSpec)
        self.mismatchedClusterSpec.add(shardSummaryResult.mismatchedClusterSpec)
        self.onlyLegacySucceed.add(shardSummaryResult.onlyLegacySucceed)
        self.onlyNewSucceed.add(shardSummaryResult.onlyNewSucceed)
        self.bothFailed.add(shardSummaryResult.bothFailed)

    @staticmethod
    def mergeSubResults(metricSummaryResult):
        merged = SummaryResult(metricSummaryResult.name)
        summaryResults: list[SummaryResult] = metricSummaryResult.results

        if len(metricSummaryResult.results) > 0:
            for categoryResult in summaryResults[0].results:
                merged.add(categoryResult)

            for summaryResult in summaryResults[1:]:
                categoryResults = summaryResult.results
                for mergedCategoryResult in merged.results:
                    toMerge = findOne(lambda x: x.name == mergedCategoryResult.name, categoryResults)
                    mergedCategoryResult.count += toMerge.count
                    mergedCategoryResult.orgIds = convertToSetIfNeeded(mergedCategoryResult.orgIds).union(
                        convertToSetIfNeeded(toMerge.orgIds))
                    mergedCategoryResult.pipelineIds = convertToSetIfNeeded(mergedCategoryResult.pipelineIds).union(
                        convertToSetIfNeeded(toMerge.pipelineIds))
                    mergedCategoryResult.policyIds = convertToSetIfNeeded(mergedCategoryResult.policyIds).union(
                        convertToSetIfNeeded(toMerge.policyIds))

        merged.count = metricSummaryResult.count
        merged.orgIds = metricSummaryResult.orgIds
        merged.pipelineIds = metricSummaryResult.pipelineIds
        merged.policyIds = metricSummaryResult.policyIds

        return merged

    @staticmethod
    def summarySubResults(result, detailed=False):
        resultStr = result.summaryOneLine()
        for subResult in result.results:
            resultStr += f"- {subResult.name} " + subResult.summaryOneLine()
            if detailed:
                for categoryResult in subResult.results:
                    resultStr += f"    - {categoryResult.name} " + categoryResult.summaryOneLine()
        return resultStr + '\n'

    def summaryPerShard(self):
        matchedClusterSpec = GlobalSummaryResult.mergeSubResults(self.matchedClusterSpec)
        mismatchedClusterSpec = GlobalSummaryResult.mergeSubResults(self.mismatchedClusterSpec)
        onlyNewSucceed = GlobalSummaryResult.mergeSubResults(self.onlyNewSucceed)
        onlyLegacySucceed = GlobalSummaryResult.mergeSubResults(self.onlyLegacySucceed)
        bothFailed = GlobalSummaryResult.mergeSubResults(self.bothFailed)
        return f"""[{self.name}]
======= Summary =======
count={self.count},
total # of org={len(self.orgIds)}
total # of pipeline={len(self.pipelineIds)}
total # of policy={len(self.policyIds)}
[MATCHED_CLUSTER_SPEC]:
{self.summarySubResults(matchedClusterSpec)}
[MISMATCHED_CLUSTER_SPEC]:
{self.summarySubResults(mismatchedClusterSpec)}
[ONLY_LEGACY_CODE_PATH_SUCCEED]:
{self.summarySubResults(onlyLegacySucceed)}
[ONLY_NEW_CODE_PATH_SUCCEED]:
{self.summarySubResults(onlyNewSucceed)}
[BOTH_LEGACY_AND_NEW_CODE_PATH_FAILED]:
{self.summarySubResults(bothFailed)}
    """

    def __repr__(self):
        resultStr = self.summaryPerShard()
        resultStr += "\n\n======= Details =======\n"
        for result in self.results:
            resultStr += f"({result.name})\n"
            resultStr += self.summarySubResults(result, detailed=True)
        return resultStr
