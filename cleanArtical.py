import os
import logging
import requests
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
 

def read_all_from_es(index=None, article_type=None, ntps_id=None, size=None):
    index = index or os.getenv("ES_INDEX", "aw_prod1")
    es_host = os.getenv("ES_HOST", "es-cn-v641fgtry001dnl1g.public.elasticsearch.aliyuncs.com")
    es_port = os.getenv("ES_PORT", "9200")
    es_user = os.getenv("ES_USER", "elastic")
    es_password = os.getenv("ES_PASSWORD", "1qaz2wsx#EDC")
    es_use_ssl = os.getenv("ES_USE_SSL", "false").lower() == "true"

    scheme = "https" if es_use_ssl else "http"
    url = f"{scheme}://{es_host}:{es_port}/{index}/_search"

    auth = HTTPBasicAuth(es_user, es_password)
    params = {"size": size}

    must = []
    if article_type:
        must.append({"term": {"articleType": article_type}})
    if ntps_id:
        if isinstance(ntps_id, list):
            must.append({"terms": {"ntpsId": ntps_id}})
        else:
            must.append({"term": {"ntpsId": ntps_id}})

    if must:
        payload = {"query": {"bool": {"must": must}}}
    else:
        payload = {"query": {"match_all": {}}}

    response = requests.post(url, auth=auth, json=payload, params=params, verify=False)
    response.raise_for_status()

    hits = response.json()["hits"]["hits"]
    documents = [hit["_source"] for hit in hits]

    return documents


if __name__ == "__main__":
    docs = read_all_from_es(article_type = "regulatoin",ntps_id=["46416", "13254", "8003","9764"])
    logger.info(f"Total documents: {len(docs)}")
    for doc in docs:
        logger.info(doc)