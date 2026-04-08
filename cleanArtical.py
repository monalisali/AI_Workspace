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
    for doc in docs:
        logger.info(doc)
    logger.info("---------------------------------打印文章结束-------------------------")

    logger.info("---------------------------------打印关联文章开始-------------------------")
    logger.info("用'DispForm.aspx?ID='进行解析")
    extracted = extract_dispform_ids(docs)
    logger.info(extracted)

    logger.info("用'[YYYY]X号'进行解析")
    extracted_numbers = extract_doc_numbers(docs)
    logger.info(extracted_numbers)

    logger.info("《》'进行解析")
    extracted_quotes = extract_book_quotes(docs)
    logger.info(extracted_quotes)
    logger.info("---------------------------------打印关联文章结束-------------------------")