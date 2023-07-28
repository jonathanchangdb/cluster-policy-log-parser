import json
import logging
import time
from pathlib import Path

import requests

from parsers.both_failed import parseBothCodePathFailed
from parsers.matched_cluster_spec import parseMatchedClusterSpec
from parsers.mismatched_cluster_spec import parseMismatchClusterSpec
from parsers.only_legacy_succeed import parseOnlyLegacySucceed
from parsers.only_new_succeed import parseOnlyNewSucceed
from parsers.utils import GlobalSummaryResult, ShardSummaryResult

logging.basicConfig(level=logging.INFO)

__KIBANA_JSON_PATH = "data/kibana.json"
__RESULT_PATH = "result"


def getPayload(query, startTimestamp, endTimestamp):
    return json.dumps({
        "params": {
            "index": "logstash-log-*",
            "body": {
                "version": True,
                "size": 10000,
                "sort": [
                    {
                        "timestamp": {
                            "order": "desc",
                            "unmapped_type": "boolean"
                        }
                    }
                ],
                "aggs": {
                    "2": {
                        "date_histogram": {
                            "field": "timestamp",
                            "fixed_interval": "30s",
                            "time_zone": "UTC",
                            "min_doc_count": 1
                        }
                    }
                },
                "stored_fields": [
                    "*"
                ],
                "script_fields": {},
                "docvalue_fields": [
                    {
                        "field": "dateUtc",
                        "format": "date_time"
                    },
                    {
                        "field": "esProcessTimestamp",
                        "format": "date_time"
                    },
                    {
                        "field": "timestamp",
                        "format": "date_time"
                    },
                    {
                        "field": "uploadTimestamp",
                        "format": "date_time"
                    }
                ],
                "_source": {
                    "excludes": []
                },
                "query": {
                    "bool": {
                        "must": [],
                        "filter": [
                            {
                                "multi_match": {
                                    "type": "phrase",
                                    "query": query,
                                    "lenient": True
                                }
                            },
                            {
                                "range": {
                                    "timestamp": {
                                        "gte": f"{startTimestamp.strftime('%Y-%m-%d')}T00:00:00.000Z",
                                        "lte": f"{endTimestamp.strftime('%Y-%m-%d')}T23:59:59.999Z",
                                        "format": "strict_date_optional_time"
                                    }
                                }
                            }
                        ],
                        "should": [],
                        "must_not": []
                    }
                },
                "highlight": {
                    "pre_tags": [
                        "@opensearch-dashboards-highlighted-field@"
                    ],
                    "post_tags": [
                        "@/opensearch-dashboards-highlighted-field@"
                    ],
                    "fields": {
                        "*": {}
                    },
                    "fragment_size": 2147483647
                }
            },
            "preference": 1688752179863
        }
    })


def getHeaders(cookie):
    return {
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Content-Type': 'application/json',
        'Cookie': cookie,
        'Referer': 'https://kibana-azure-australiaeast.cloud.databricks.com/app/discover?security_tenant=service_logs',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
        'osd-version': '1.3.5',
        'sec-ch-ua': '"Not.A/Brand";v="8", "Chromium";v="114", "Google Chrome";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"'
    }


def __parse_uris():
    with open(__KIBANA_JSON_PATH) as file:
        file_content_raw = file.read()
    urls = json.loads(file_content_raw)
    return [url.split(',') for url in urls]


def __sendRequest(url, cookie, query, startTimestamp, endTimestamp):
    try:
        response = requests.request("POST", url,
                                    headers=getHeaders(cookie),
                                    data=getPayload(query=query,
                                                    startTimestamp=startTimestamp,
                                                    endTimestamp=endTimestamp),
                                    timeout=120)
        if response.status_code != 200:
            raise Exception(f"Failed to request URL:{url} message:{response.text}")
        return response.text

    except Exception as e:
        logging.error(f'Failed URL:{url}', e)


def __matchedClusterSpec(url, cookie, startTimestamp, endTimestamp, directory, shardName):
    response = __sendRequest(url=url, cookie=cookie, query="[MATCHED_CLUSTER_SPEC]", startTimestamp=startTimestamp,
                             endTimestamp=endTimestamp)
    content, summaryResult = parseMatchedClusterSpec(response=response, shardName=shardName)
    with open(f"{directory}/matched-cluster-spec.txt", 'w') as matchedClusterSpecFile:
        matchedClusterSpecFile.write(content)
    return summaryResult


def __mismatchedClusterSpec(url, cookie, startTimestamp, endTimestamp, directory, shardName,
                            matchedClusterSpecSummaryResult, onlyNewSucceedSummaryResult):
    response = __sendRequest(url=url, cookie=cookie, query="[MISMATCHED_CLUSTER_SPEC]", startTimestamp=startTimestamp,
                             endTimestamp=endTimestamp)
    content, summaryResult = parseMismatchClusterSpec(response=response, shardName=shardName,
                                                      matchedClusterSpecSummaryResult=matchedClusterSpecSummaryResult,
                                                      onlyNewSucceedSummaryResult=onlyNewSucceedSummaryResult)
    with open(f"{directory}/mismatched-cluster-spec.txt", 'w') as mismatchedClusterSpecFile:
        mismatchedClusterSpecFile.write(content)
    return summaryResult


def __onlyLegacySucceed(url, cookie, startTimestamp, endTimestamp, directory, shardName):
    response = __sendRequest(url=url, cookie=cookie, query="[ONLY_LEGACY_CODE_PATH_SUCCEED]",
                             startTimestamp=startTimestamp,
                             endTimestamp=endTimestamp)
    content, summaryResult = parseOnlyLegacySucceed(response=response, shardName=shardName)
    with open(f"{directory}/only-legacy-code-path-succeed.txt", 'w') as onlyLegacySucceedFile:
        onlyLegacySucceedFile.write(content)
    return summaryResult


def __onlyNewSucceed(url, cookie, startTimestamp, endTimestamp, directory, shardName):
    response = __sendRequest(url=url, cookie=cookie, query="[ONLY_NEW_CODE_PATH_SUCCEED]",
                             startTimestamp=startTimestamp,
                             endTimestamp=endTimestamp)
    content, summaryResult = parseOnlyNewSucceed(response=response, shardName=shardName)
    with open(f"{directory}/only-new-code-path-succeed.txt", 'w') as onlyNewSucceedFile:
        onlyNewSucceedFile.write(content)
    return summaryResult


def __bothFailed(url, cookie, startTimestamp, endTimestamp, directory, shardName):
    response = __sendRequest(url=url, cookie=cookie, query="[BOTH_OLD_AND_NEW_CODE_PATH_FAILED]",
                             startTimestamp=startTimestamp,
                             endTimestamp=endTimestamp)
    content, summaryResult = parseBothCodePathFailed(response=response, shardName=shardName)
    with open(f"{directory}/both-old-and-new-code-path-failed.txt", 'w') as bothFailed:
        bothFailed.write(content)
    return summaryResult


def __parseOneShard(url, cookie, startTimestamp, endTimestamp, directory, shardName):
    shardSummaryResult = ShardSummaryResult(shardName)

    matchedClusterSpecSummaryResult = __matchedClusterSpec(url=url, cookie=cookie, startTimestamp=startTimestamp,
                                                           endTimestamp=endTimestamp,
                                                           directory=directory, shardName=shardName)
    onlyNewSucceedSummaryResult = __onlyNewSucceed(url=url, cookie=cookie, startTimestamp=startTimestamp,
                                                   endTimestamp=endTimestamp,
                                                   directory=directory, shardName=shardName)
    shardSummaryResult.add(__mismatchedClusterSpec(url=url, cookie=cookie, startTimestamp=startTimestamp,
                                                   endTimestamp=endTimestamp,
                                                   directory=directory, shardName=shardName,
                                                   matchedClusterSpecSummaryResult=matchedClusterSpecSummaryResult,
                                                   onlyNewSucceedSummaryResult=onlyNewSucceedSummaryResult))
    shardSummaryResult.add(__onlyLegacySucceed(url=url, cookie=cookie, startTimestamp=startTimestamp,
                                               endTimestamp=endTimestamp,
                                               directory=directory, shardName=shardName))
    shardSummaryResult.add(matchedClusterSpecSummaryResult)
    shardSummaryResult.add(onlyNewSucceedSummaryResult)
    shardSummaryResult.add(
        __bothFailed(url=url, cookie=cookie, startTimestamp=startTimestamp, endTimestamp=endTimestamp,
                     directory=directory, shardName=shardName)
    )
    with open(f"{directory}/summary.txt", 'w') as summaryFile:
        summaryFile.write(shardSummaryResult.__repr__())
    return shardSummaryResult


def parse(cookie, startTimestamp, endTimestamp):
    startDate = startTimestamp.strftime('%Y-%m-%d')
    endDate = endTimestamp.strftime('%Y-%m-%d')
    resultDirectoryName = startDate if startDate == endDate else f'{startDate}_{endDate}'
    globalSummaryResult = GlobalSummaryResult(resultDirectoryName)
    directory = f"{__RESULT_PATH}/{resultDirectoryName}"
    Path(directory).mkdir(parents=True, exist_ok=True)

    uris = __parse_uris()
    for (idx, (shardName, kibanaUri)) in enumerate(uris):
        time.sleep(1)
        try:
            shardDirectory = f"{directory}/{shardName}"
            url = f"https://{kibanaUri}/internal/search/opensearch"
            Path(shardDirectory).mkdir(parents=True, exist_ok=True)

            globalSummaryResult.add(
                __parseOneShard(url=url, cookie=cookie, startTimestamp=startTimestamp, endTimestamp=endTimestamp,
                                directory=shardDirectory, shardName=shardName)
            )
            logging.info(f'[{idx + 1}/{len(uris)}] {shardName} finished.')
        except Exception as ex:
            logging.error(f"[{idx + 1}/{len(uris)}] {shardName} failed!", ex)

    with open(f"{directory}/summary.txt", 'w') as summaryFile:
        summaryFile.write(globalSummaryResult.__repr__())
    return globalSummaryResult


def __writeTestDataForOneShard(url, cookie, startTimestamp, endTimestamp, directory):
    with open(f"{directory}/matched-cluster-spec.json", 'w') as matchedClusterSpecFile:
        response = __sendRequest(url=url, cookie=cookie, query="[MATCHED_CLUSTER_SPEC]", startTimestamp=startTimestamp,
                                 endTimestamp=endTimestamp)
        matchedClusterSpecFile.write(response)
    with open(f"{directory}/mismatched-cluster-spec.json", 'w') as matchedClusterSpecFile:
        response = __sendRequest(url=url, cookie=cookie, query="[MISMATCHED_CLUSTER_SPEC]",
                                 startTimestamp=startTimestamp,
                                 endTimestamp=endTimestamp)
        matchedClusterSpecFile.write(response)
    with open(f"{directory}/only-legacy-code-path-succeed.json", 'w') as matchedClusterSpecFile:
        response = __sendRequest(url=url, cookie=cookie, query="[ONLY_LEGACY_CODE_PATH_SUCCEED]",
                                 startTimestamp=startTimestamp,
                                 endTimestamp=endTimestamp)
        matchedClusterSpecFile.write(response)
    with open(f"{directory}/only-new-code-path-succeed.json", 'w') as matchedClusterSpecFile:
        response = __sendRequest(url=url, cookie=cookie, query="[ONLY_NEW_CODE_PATH_SUCCEED]",
                                 startTimestamp=startTimestamp,
                                 endTimestamp=endTimestamp)
        matchedClusterSpecFile.write(response)
    with open(f"{directory}/both-old-and-new-code-path-failed.json", 'w') as matchedClusterSpecFile:
        response = __sendRequest(url=url, cookie=cookie, query="[BOTH_OLD_AND_NEW_CODE_PATH_FAILED]",
                                 startTimestamp=startTimestamp,
                                 endTimestamp=endTimestamp)
        matchedClusterSpecFile.write(response)


def writeTestData(cookie, startTimestamp, endTimestamp):
    directory = f"test-data"
    Path(directory).mkdir(parents=True, exist_ok=True)

    uris = __parse_uris()
    for (idx, (shardName, kibanaUri)) in enumerate(uris):
        time.sleep(1)
        try:
            shardDirectory = f"{directory}/{shardName}"
            url = f"https://{kibanaUri}/internal/search/opensearch"
            Path(shardDirectory).mkdir(parents=True, exist_ok=True)

            __writeTestDataForOneShard(url=url, cookie=cookie, startTimestamp=startTimestamp, endTimestamp=endTimestamp,
                                       directory=shardDirectory)
            logging.info(f'[{idx + 1}/{len(uris)}] {shardName} finished.')
        except Exception as ex:
            logging.error(f"[{idx + 1}/{len(uris)}] {shardName} failed!", ex)


def __matchedClusterSpecTestData(directory, shardName):
    with open(f"test-data/{shardName}/matched-cluster-spec.json", 'r') as testData:
        content, summaryResult = parseMatchedClusterSpec(response=testData.read(),
                                                         shardName=shardName)
    with open(f"{directory}/matched-cluster-spec.txt", 'w') as matchedClusterSpecFile:
        matchedClusterSpecFile.write(content)
    return summaryResult


def __mismatchedClusterSpecTestData(directory, shardName, matchedClusterSpecSummaryResult, onlyNewSucceedSummaryResult):
    with open(f"test-data/{shardName}/mismatched-cluster-spec.json", 'r') as testData:
        content, summaryResult = parseMismatchClusterSpec(response=testData.read(),
                                                          shardName=shardName,
                                                          matchedClusterSpecSummaryResult=matchedClusterSpecSummaryResult,
                                                          onlyNewSucceedSummaryResult=onlyNewSucceedSummaryResult)
    with open(f"{directory}/mismatched-cluster-spec.txt", 'w') as mismatchedClusterSpecFile:
        mismatchedClusterSpecFile.write(content)
    return summaryResult


def __onlyLegacySucceedTestData(directory, shardName):
    with open(f"test-data/{shardName}/only-legacy-code-path-succeed.json", 'r') as testData:
        content, summaryResult = parseOnlyLegacySucceed(response=testData.read(),
                                                        shardName=shardName)
    with open(f"{directory}/only-legacy-code-path-succeed.txt", 'w') as onlyLegacySucceedFile:
        onlyLegacySucceedFile.write(content)
    return summaryResult


def __onlyNewSucceedTestData(directory, shardName):
    with open(f"test-data/{shardName}/only-new-code-path-succeed.json", 'r') as testData:
        content, summaryResult = parseOnlyNewSucceed(response=testData.read(), shardName=shardName)
    with open(f"{directory}/only-new-code-path-succeed.txt", 'w') as onlyNewSucceedFile:
        onlyNewSucceedFile.write(content)
    return summaryResult


def __bothFailedTestData(directory, shardName):
    with open(f"test-data/{shardName}/both-old-and-new-code-path-failed.json", 'r') as testData:
        content, summaryResult = parseBothCodePathFailed(response=testData.read(), shardName=shardName)
    with open(f"{directory}/both-old-and-new-code-path-failed.txt", 'w') as bothFailed:
        bothFailed.write(content)
    return summaryResult


def __parseOneShardFromTestData(directory, shardName):
    shardSummaryResult = ShardSummaryResult(shardName)
    matchedClusterSpecSummaryResult = __matchedClusterSpecTestData(directory=directory, shardName=shardName)
    onlyNewSucceedSummaryResult = __onlyNewSucceedTestData(directory=directory, shardName=shardName)
    shardSummaryResult.add(
        __mismatchedClusterSpecTestData(directory=directory, shardName=shardName,
                                        matchedClusterSpecSummaryResult=matchedClusterSpecSummaryResult,
                                        onlyNewSucceedSummaryResult=onlyNewSucceedSummaryResult))
    shardSummaryResult.add(__onlyLegacySucceedTestData(directory=directory, shardName=shardName))
    shardSummaryResult.add(__bothFailedTestData(directory=directory, shardName=shardName))
    shardSummaryResult.add(onlyNewSucceedSummaryResult)
    shardSummaryResult.add(matchedClusterSpecSummaryResult)
    with open(f"{directory}/summary.txt", 'w') as summaryFile:
        summaryFile.write(shardSummaryResult.__repr__())
    return shardSummaryResult


def parseTestData():
    globalSummaryResult = GlobalSummaryResult("test-result")
    directory = f"test-result"
    Path(directory).mkdir(parents=True, exist_ok=True)

    uris = __parse_uris()
    for (idx, (shardName, kibanaUri)) in enumerate(uris):
        try:
            shardDirectory = f"{directory}/{shardName}"
            Path(shardDirectory).mkdir(parents=True, exist_ok=True)

            globalSummaryResult.add(__parseOneShardFromTestData(directory=shardDirectory, shardName=shardName))
            logging.info(f'[{idx + 1}/{len(uris)}] {shardName} finished.')
        except Exception as ex:
            logging.error(f"[{idx + 1}/{len(uris)}] {shardName} failed!", ex)

    with open(f"{directory}/summary.txt", 'w') as summaryFile:
        summaryFile.write(globalSummaryResult.__repr__())
    return globalSummaryResult
