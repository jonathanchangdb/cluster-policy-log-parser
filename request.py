import json
import logging
import time
import requests
from datetime import datetime, timedelta
from pathlib import Path
from tqdm import tqdm

from parsers.both_failed import parseBothCodePathFailed
from parsers.matched_cluster_spec import parseMatchedClusterSpec
from parsers.mismatched_cluster_spec import parseMismatchClusterSpec
from parsers.only_legacy_succeed import parseOnlyLegacySucceed

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
                                                    endTimestamp=endTimestamp))
        if response.status_code != 200:
            raise Exception(f"Failed to request URL:{url} message:{response.text}")
        return response.text

    except Exception as e:
        logging.error(f'Failed URL:{url}', e)


def __matchedClusterSpec(url, cookie, startTimestamp, endTimestamp, directory, shardName):
    response = __sendRequest(url=url, cookie=cookie, query="[MATCHED_CLUSTER_SPEC]", startTimestamp=startTimestamp,
                             endTimestamp=endTimestamp)
    with open(f"{directory}/matched-cluster-spec.txt", 'w') as matchedClusterSpecFile:
        matchedClusterSpecFile.write(parseMatchedClusterSpec(response=response, shardName=shardName))


def __mismatchedClusterSpec(url, cookie, startTimestamp, endTimestamp, directory, shardName):
    response = __sendRequest(url=url, cookie=cookie, query="[MISMATCHED_CLUSTER_SPEC]", startTimestamp=startTimestamp,
                             endTimestamp=endTimestamp)
    with open(f"{directory}/mismatched-cluster-spec.txt", 'w') as mismatchedClusterSpecFile:
        mismatchedClusterSpecFile.write(parseMismatchClusterSpec(response=response, shardName=shardName))


def __onlyLegacySucceed(url, cookie, startTimestamp, endTimestamp, directory, shardName):
    response = __sendRequest(url=url, cookie=cookie, query="[ONLY_LEGACY_CODE_PATH_SUCCEED]",
                             startTimestamp=startTimestamp,
                             endTimestamp=endTimestamp)
    with open(f"{directory}/only-legacy-code-path-succeed.txt", 'w') as onlyLegacySucceedFile:
        onlyLegacySucceedFile.write(parseOnlyLegacySucceed(response=response, shardName=shardName))


def __bothFailed(url, cookie, startTimestamp, endTimestamp, directory, shardName):
    response = __sendRequest(url=url, cookie=cookie, query="[BOTH_OLD_AND_NEW_CODE_PATH_FAILED]",
                             startTimestamp=startTimestamp,
                             endTimestamp=endTimestamp)
    with open(f"{directory}/both-old-and-new-code-path-failed.txt", 'w') as bothFailed:
        bothFailed.write(parseBothCodePathFailed(response=response, shardName=shardName))


def parse(cookie, startTimestamp=(datetime.today() - timedelta(days=1)),
          endTimestamp=datetime.today()):
    for (shardName, kibanaUri) in tqdm(__parse_uris()):
        time.sleep(0.25)
        try:
            directory = f"{__RESULT_PATH}/{endTimestamp.strftime('%Y-%m-%d')}/{shardName}"
            url = f"https://{kibanaUri}/internal/search/opensearch"
            Path(directory).mkdir(parents=True, exist_ok=True)

            __matchedClusterSpec(url=url, cookie=cookie, startTimestamp=startTimestamp, endTimestamp=endTimestamp,
                                 directory=directory, shardName=shardName)
            __mismatchedClusterSpec(url=url, cookie=cookie, startTimestamp=startTimestamp, endTimestamp=endTimestamp,
                                    directory=directory, shardName=shardName)
            __onlyLegacySucceed(url=url, cookie=cookie, startTimestamp=startTimestamp, endTimestamp=endTimestamp,
                                directory=directory, shardName=shardName)
            __bothFailed(url=url, cookie=cookie, startTimestamp=startTimestamp, endTimestamp=endTimestamp,
                         directory=directory, shardName=shardName)
            logging.info(f'{shardName} finished.')
        except Exception as ex:
            logging.error(f"{shardName} failed!", ex)
