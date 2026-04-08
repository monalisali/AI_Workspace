import os
import logging
import requests
import json
import re
from requests.auth import HTTPBasicAuth

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('es_data.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def read_all_from_es(index=None, article_type=None, ntps_id=None, size=10000):
    index = index or os.getenv("ES_INDEX", "aw_prod1")
    es_host = os.getenv("ES_HOST", "es-cn-v641fgtry001dnl1g.public.elasticsearch.aliyuncs.com")
    es_port = os.getenv("ES_PORT", "9200")
    es_user = os.getenv("ES_USER", "elastic")
    es_password = os.getenv("ES_PASSWORD", "1qaz2wsx#EDC")
    es_use_ssl = os.getenv("ES_USE_SSL", "false").lower() == "true"

    scheme = "https" if es_use_ssl else "http"
    url = f"{scheme}://{es_host}:{es_port}/{index}/_search"

    auth = HTTPBasicAuth(es_user, es_password)
    must = []
    if article_type:
        must.append({"match": {"articleType": article_type}})
    if ntps_id:
        if isinstance(ntps_id, list):
            must.append({"terms": {"ntpsId": ntps_id}})
        else:
            must.append({"term": {"ntpsId": ntps_id}})

    params = {"size": size}
    payload = {}
    
    #查询结果中不会包含'attachments'字段
    if must:
        payload = {"query": {"bool": {"must": must}}, "_source": {"excludes": ["attachments"]}}
    else:
        payload = {"query": {"match_all": {}}, "_source": {"excludes": ["attachments"]}}

    #打印查询条件
    curl_cmd = f"""curl -X POST "{url}" -u "{es_user}:{es_password}" -H "Content-Type: application/json" -d '{json.dumps(payload)}'"""
    if params:
        curl_cmd += " \\\n  " + " \\\n  ".join([f'"{k}={v}"' for k, v in params.items()])
    logger.info("\n=== ES查询语句 (可直接执行) ===")
    logger.info(curl_cmd)
    logger.info("=== ===\n")
    

    response = requests.post(url, auth=auth, json=payload, params=params, verify=False)
    response.raise_for_status()

    hits = response.json()["hits"]["hits"]
    documents = [hit["_source"] for hit in hits]

    return documents


def extract_dispform_ids(documents):
    results = {}
    for doc in documents:
        ntps_id = doc.get("ntpsId")
        fulltext = doc.get("fullText", "")
        if ntps_id and fulltext:
            matches = re.findall(r'DispForm\.aspx\?ID=(\d+)', fulltext)
            results[ntps_id] = matches
    return results


def extract_doc_numbers(documents):
    results = {}
    for doc in documents:
        ntps_id = doc.get("ntpsId")
        fulltext = doc.get("fullText", "")
        if ntps_id and fulltext:
            matches = re.findall(r'\[(\d{4})\]\s*(\d+)号', fulltext)
            results[ntps_id] = [f"[{year}] {num}号" for year, num in matches]
    return results


def extract_book_quotes(documents):
    results = {}
    for doc in documents:
        ntps_id = doc.get("ntpsId")
        fulltext = doc.get("fullText", "")
        if ntps_id and fulltext:
            matches = re.findall(r'《([^》]+)》', fulltext)
            cleaned = []
            for m in matches:
                m = m.strip()
                if not m or re.match(r'^https?://', m) or re.match(r'^[\w-]+\.(css|js|html|png|jpg|jpeg|gif|svg|ico)$', m, re.IGNORECASE):
                    continue
                if len(m) < 200:
                    cleaned.append(m)
            results[ntps_id] = cleaned
    return results

'''
def parse_doc_numbers(extracted):
    for ntps_id, doc_list in extracted.items():
        for doc in doc_list:
            match = re.match(r'\[(\d{4})\]\s*(\d+)号', doc)
            if match:
                year = int(match.group(1))
                doc_no = match.group(2)
                yield ntps_id, year, doc_no
'''

def parse_doc_num(extracted):
    result = {}
    for ntps_id, doc_list in extracted.items():
        if ntps_id not in result:
             result[ntps_id] = []
        for doc in doc_list:
            match = re.match(r'\[(\d{4})\]\s*(\d+)号', doc)
            if match:
                year = int(match.group(1))
                doc_no = match.group(2)
                result[ntps_id].append({"year": year, "docNumber":doc_no})
    return result

def query_by_year_and_docno(year, doc_no, index=None):
    index = index or os.getenv("ES_INDEX", "aw_prod1")
    es_host = os.getenv("ES_HOST", "es-cn-v641fgtry001dnl1g.public.elasticsearch.aliyuncs.com")
    es_port = os.getenv("ES_PORT", "9200")
    es_user = os.getenv("ES_USER", "elastic")
    es_password = os.getenv("ES_PASSWORD", "1qaz2wsx#EDC")
    es_use_ssl = os.getenv("ES_USE_SSL", "false").lower() == "true"

    scheme = "https" if es_use_ssl else "http"
    url = f"{scheme}://{es_host}:{es_port}/{index}/_search"

    auth = HTTPBasicAuth(es_user, es_password)
    payload = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"yearPublish": year}},
                    {"term": {"docNo": doc_no}}
                ]
            }
        },
        "_source": ["ntpsId"]
    }

    response = requests.post(url, auth=auth, json=payload, verify=False)
    response.raise_for_status()

    hits = response.json()["hits"]["hits"]
    return [hit["_source"]["ntpsId"] for hit in hits]

def get_ntpsid_by_docNum(doc_list):
    temp = {}
    result = {}
    for ntps_id, items in doc_list.items():
        if ntps_id not in result:
            temp[ntps_id] = []
            for i in items:
                r = query_by_year_and_docno(i["year"],i["docNumber"])
                temp[ntps_id].append(r)

    # doc_list中一个ntpsid会有多个文号，如：{'28785': ['[2014] 109号', '[2009] 59号'] }
    # temp中会变成{'28785': [['12768', '19253', '38070'], ['27236', '2946']]}, 一个ntpsid会有多个数组。
    # 要把数组合并起来，变成{'28785': ['12768', '19253', '38070','27236', '2946']},
    for id,item in temp.items():
        result[id] = []
        for i in item:
            result[id].extend(i)     
        result[id] = list(set(result[id]))

    return result
            
def append_ntpsIds(source, target):
    for id,item in target.items():
        for i in item:
            if i not in source[id]:
                source[id] = []
                source[id].append(i)
    return source

        

    


if __name__ == "__main__":
    #从data.json中读取10个问题的文章ntpsid
    with open("data.json", "r", encoding="utf-8") as f:
        data = json.load(f)
    
    all_related_ids = []
    for item in data.get("ntpsId", []):
        all_related_ids.extend(item.get("relatedArticleId", []))
    
    all_related_ids = [str(id) for id in all_related_ids]
    docs = read_all_from_es(article_type="regulatoin", ntps_id=all_related_ids)
    logger.info("---------------------------------打印文章开始-------------------------")
    logger.info(f"Total documents: {len(docs)}")
    #for doc in docs:
        #logger.info(doc)
    #logger.info("---------------------------------打印文章结束-------------------------")

    logger.info("---------------------------------打印关联文章开始-------------------------")
    logger.info("用'DispForm.aspx?ID='进行解析")
    extracted_ntpsIds = extract_dispform_ids(docs)
    logger.info(extracted_ntpsIds)

    logger.info("用'[YYYY]X号'进行解析")
    extracted_numbers = extract_doc_numbers(docs)
    logger.info(extracted_numbers)

    logger.info("《》'进行解析")
    extracted_quotes = extract_book_quotes(docs)
    logger.info(extracted_quotes)
    logger.info("---------------------------------打印关联文章结束-------------------------")
    logger.info("通过year和doc_no获取ntpsId")
    all_parsed_docNumbers = parse_doc_num(extracted_numbers)
    all_docNo_ntpsIds = get_ntpsid_by_docNum(all_parsed_docNumbers)
    ss = append_ntpsIds(extracted_ntpsIds,all_docNo_ntpsIds)
    aa = "ss"

    '''
    all_results = {}
    for ntps_id, year, doc_no in parse_doc_numbers(extracted_numbers):
        result = query_by_year_and_docno(year, doc_no)
        key = f"[{year}] {doc_no}号"
        all_results[key] = result
        logger.info(f"{key}: {result}")
    '''