import os
import logging
import requests
import json
import re
from requests.auth import HTTPBasicAuth
from neo4j import GraphDatabase

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
    env = _read_dockerfile_env()
    index = index or env.get("ES_INDEX", os.getenv("ES_INDEX"))
    es_host = env.get("ES_HOST", os.getenv("ES_HOST"))
    es_port = env.get("ES_PORT", os.getenv("ES_PORT"))
    es_user = env.get("ES_USER", os.getenv("ES_USER"))
    es_password = env.get("ES_PASSWORD", os.getenv("ES_PASSWORD"))
    es_use_ssl = env.get("ES_USE_SSL", os.getenv("ES_USE_SSL", "false")).lower() == "true"

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
    env = _read_dockerfile_env()
    index = index or env.get("ES_INDEX", os.getenv("ES_INDEX"))
    es_host = env.get("ES_HOST", os.getenv("ES_HOST"))
    es_port = env.get("ES_PORT", os.getenv("ES_PORT"))
    es_user = env.get("ES_USER", os.getenv("ES_USER"))
    es_password = env.get("ES_PASSWORD", os.getenv("ES_PASSWORD"))
    es_use_ssl = env.get("ES_USE_SSL", os.getenv("ES_USE_SSL", "false")).lower() == "true"

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
            for val in i:
                if val != id:
                    result[id].append(val)
        result[id] = list(set(result[id]))

    return result
            
def append_ntpsIds(source, target):
    for id,item in target.items():
        for i in item:
            if i not in source[id]:
                if source.get(id) is None:
                    source[id] = []
                source[id].append(i)
    return source


#获取fullText中<a href="/_layouts/Redirect.aspx 开头的href
def extract_redirect_links(documents):
    results = {}
    total_count = 0
    for doc in documents:
        ntps_id = doc.get("ntpsId")
        fulltext = doc.get("fullText", "")
        if ntps_id and fulltext:
            pattern = r'<a\s+[^>]*href=["\'](/_layouts/Redirect\.aspx[^"\']*)["\'][^>]*>'
            matches = re.findall(pattern, fulltext, re.IGNORECASE)
            results[ntps_id] = [{"href": href} for href in matches]
            total_count += len(matches)
    logger.info(f"共找到 {total_count} 个 Redirect.aspx 链接")
    
    with open("docs/redirectLink.txt", "w", encoding="utf-8") as f:
        for ntps_id, links in results.items():
            for link in links:
                f.write(f"{ntps_id}\t{link['href']}\n")
    
    return results



#读取DockerFile配置
def _read_dockerfile_env():
    dockerfile_path = os.path.join(os.path.dirname(__file__), "DockerFile")
    env = {}
    if os.path.exists(dockerfile_path):
        with open(dockerfile_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line.startswith("ENV MG_HOST="):
                    env["MG_HOST"] = line.split("=", 1)[1]
                elif line.startswith("ENV MG_PORT="):
                    env["MG_PORT"] = line.split("=", 1)[1]
                elif line.startswith("ENV ES_HOST="):
                    env["ES_HOST"] = line.split("=", 1)[1]
                elif line.startswith("ENV ES_PORT="):
                    env["ES_PORT"] = line.split("=", 1)[1]
                elif line.startswith("ENV ES_INDEX="):
                    env["ES_INDEX"] = line.split("=", 1)[1]
                elif line.startswith("ENV ES_USER="):
                    env["ES_USER"] = line.split("=", 1)[1]
                elif line.startswith("ENV ES_PASSWORD="):
                    env["ES_PASSWORD"] = line.split("=", 1)[1]
                elif line.startswith("ENV ES_USE_SSL="):
                    env["ES_USE_SSL"] = line.split("=", 1)[1]
    return env


#如果文章ntpsid和关联文章的ntpsid都存在于图数据库，则返回
def filter_existing_in_graphdb(data):
    env = _read_dockerfile_env()
    mg_host = env.get("MG_HOST", os.getenv("MG_HOST"))
    mg_port = env.get("MG_PORT", os.getenv("MG_PORT"))
    uri = f"bolt://{mg_host}:{mg_port}"
    driver = GraphDatabase.driver(uri)
    
    result = {}
    with driver.session() as session:
        for ntps_id, related_ids in data.items():
            ntps_id_str = str(ntps_id)
            check_query = "MATCH (n) WHERE n.ntpsId = $ntps_id RETURN n.ntpsId"
            check_result = list(session.run(check_query, ntps_id=ntps_id_str))
            if not check_result:
                logger.info(f"ntps_id {ntps_id} 不存在于图数据库中，跳过")
                continue
            
            related_ids = [str(id) for id in related_ids]
            if related_ids:
                placeholders = ", ".join([f"$id{i}" for i in range(len(related_ids))])
                query = f"MATCH (n) WHERE n.ntpsId IN [{placeholders}] RETURN n.ntpsId"
                params = {f"id{i}": rid for i, rid in enumerate(related_ids)}
                records = list(session.run(query, **params))
                existing_ids = [record["n.ntpsId"] for record in records]
                result[ntps_id] = existing_ids
    
    driver.close()
    return {k: v for k, v in result.items() if v}

#通过ntpsid获取关系
#filter_ids_in_graph_relations(existedIds_in_graph,get_graph_relation)
def filter_ids_in_graph_relations(existedIds_in_graph, get_graph_relation):
    result = {}
    all_relation_ids = set()
    for rel in get_graph_relation:
        start_data = rel.get("start", {})
        end_data = rel.get("end", {})
        if "ntpsId" in start_data:
            all_relation_ids.add(str(start_data["ntpsId"]))
        if "ntpsId" in end_data:
            all_relation_ids.add(str(end_data["ntpsId"]))
    
    for ntps_id, related_ids in existedIds_in_graph.items():
        existing = [rid for rid in related_ids if rid in all_relation_ids]
        if existing:
            result[ntps_id] = existing
    
    return result

#查询图数据库node
def query_from_graphdb(limit=10):
    env = _read_dockerfile_env()
    mg_host = env.get("MG_HOST", os.getenv("MG_HOST"))
    mg_port = env.get("MG_PORT", os.getenv("MG_PORT"))
    
    uri = f"bolt://{mg_host}:{mg_port}"
    driver = GraphDatabase.driver(uri)
    
    query = "MATCH (n) RETURN n LIMIT $limit"
    
    with driver.session() as session:
        result = session.run(query, limit=limit)
        records = list(result)
    
    driver.close()
    
    return [dict(record["n"]) for record in records]

# 查询图数据库有node数量
def count_graphdb_records():
    env = _read_dockerfile_env()
    mg_host = env.get("MG_HOST", os.getenv("MG_HOST"))
    mg_port = env.get("MG_PORT", os.getenv("MG_PORT"))
    
    uri = f"bolt://{mg_host}:{mg_port}"
    driver = GraphDatabase.driver(uri)
    
    query = "MATCH (n) RETURN count(n) AS total"
    
    with driver.session() as session:
        result = session.run(query)
        total = result.single()["total"]
    
    driver.close()
    
    return total

#查询图数据库的关系（边）
def query_all_graphdb_relationships(limit=10):
    env = _read_dockerfile_env()
    mg_host = env.get("MG_HOST", os.getenv("MG_HOST"))
    mg_port = env.get("MG_PORT", os.getenv("MG_PORT"))
    
    uri = f"bolt://{mg_host}:{mg_port}"
    driver = GraphDatabase.driver(uri)
    
    query = "MATCH (a)-[r]->(b) RETURN a, type(r), b LIMIT $limit"
    
    with driver.session() as session:
        result = session.run(query, limit=limit)
        records = list(result)
    
    driver.close()
    
    return [
        {
            "start": dict(record["a"]),
            "relationship": record["type(r)"],
            "end": dict(record["b"])
        }
        for record in records
    ]

def create_property_relations(existedIds_in_graph, property_ids=None):
    if property_ids is not None:
        properties = property_ids if isinstance(property_ids, list) else [property_ids]
    else:
        properties = list(existedIds_in_graph.keys())
    
    env = _read_dockerfile_env()
    mg_host = env.get("MG_HOST", os.getenv("MG_HOST"))
    mg_port = env.get("MG_PORT", os.getenv("MG_PORT"))
    uri = f"bolt://{mg_host}:{mg_port}"
    driver = GraphDatabase.driver(uri)
    
    total_created = 0
    with driver.session() as session:
        for prop_id in properties:
            if prop_id not in existedIds_in_graph:
                logger.info(f"当前文章 {prop_id} 不存在, existedIds_in_graph keys: {list(existedIds_in_graph.keys())}")
                continue
            
            values = existedIds_in_graph[prop_id]
            logger.info(f"当前文章 {prop_id} 有关联文章: {values}, 数量: {len(values)}")
            
            if not values:
                logger.info(f"当前文章 {prop_id} 没有值，无需创建关系")
                continue
            
            relation_type = "RELATED_TO"
            created_count = 0
            for val in values:
                #先检查关系是否已经存在
                check_query1 = f"""
                MATCH (a)-[r:`{relation_type}`]->(b)
                WHERE a.ntpsId = $prop_id AND b.ntpsId = $val
                RETURN r
                """
                check_query2 = f"""
                MATCH (a)-[r:`{relation_type}`]->(b)
                WHERE a.ntpsId = $val AND b.ntpsId = $prop_id
                RETURN r
                """
                exists1 = list(session.run(check_query1, prop_id=prop_id, val=val))
                exists2 = list(session.run(check_query2, prop_id=prop_id, val=val))
                
                if not exists1:
                    #文章->关联文章的关系
                    query1 = f"""
                    MATCH (a), (b)
                    WHERE a.ntpsId = $prop_id AND b.ntpsId = $val
                    CREATE (a)-[r:`{relation_type}`]->(b)
                    RETURN a, b
                    """
                    result1 = list(session.run(query1, prop_id=prop_id, val=val))
                    if result1:
                        created_count += 1
                        logger.info(f"当前文章 {prop_id} 创建[文章->关联文章]关系：文章 {{{prop_id}}} -> 关联文章 {{{val}}}")
                
                if not exists2:
                    #关联文章->文章的关系
                    query2 = f"""
                    MATCH (a), (b)
                    WHERE a.ntpsId = $val AND b.ntpsId = $prop_id
                    CREATE (a)-[r:`{relation_type}`]->(b)
                    RETURN a, b
                    """
                    result2 = list(session.run(query2, prop_id=prop_id, val=val))
                    if result2:
                        created_count += 1
                        logger.info(f"当前文章 {prop_id} 创建[关联文章->文章]关系：关联文章 {{{val}}} -> 文章 {{{prop_id}}}")
            
            logger.info(f"当前文章 {prop_id} 创建了 {created_count} 条关系")
            total_created += created_count
    
    driver.close()
    return total_created

if __name__ == "__main__":
    #从data.json中读取10个问题的文章ntpsid
    with open("data.json", "r", encoding="utf-8") as f:
        data = json.load(f)
    
    all_related_ids = []
    for item in data.get("ntpsId", []):
        all_related_ids.extend(item.get("relatedArticleId", []))
    
    all_related_ids = [str(id) for id in all_related_ids]
    #1.从ES中读取文章
    docs = read_all_from_es(article_type="regulatoin", ntps_id=all_related_ids)
    logger.info("---------------------------------打印文章开始-------------------------")
    logger.info(f"Total documents: {len(docs)}")
    #for doc in docs:
        #logger.info(doc)
    
    logger.info("---------------------------------打印文章结束-------------------------")
    
    
    logger.info("---------------------------------打印关联文章开始-------------------------")
    logger.info("查找fullText中'DispForm.aspx?ID='后的ntpsId")
    #2 查找关联文章
    #2.1 通过'DispForm.aspx?ID='进行解析
    extracted_ntpsIds = extract_dispform_ids(docs)
    logger.info(extracted_ntpsIds)
    #2.2 通过'文号'进行解析
    logger.info("查找fullTex中所有'[YYYY]X号'")
    extracted_numbers = extract_doc_numbers(docs)
    logger.info(extracted_numbers)
    #2.3 通过'《》'进行解析
    logger.info("查找fullText中所有'《》'")
    extracted_quotes = extract_book_quotes(docs)
    logger.info(extracted_quotes)
    logger.info("保存redirect link到docs/redirectLink.txt")
    #2.4 统一保存redirectLink
    #redirectLink在fullTex中是加密的，只有点击后才能从跳转后的url找看到ntpsId
    extract_redirect_links(docs)
    logger.info("---------------------------------打印关联文章结束-------------------------")

    #3. 建立文章关系
    #3.1 获取'文号'对应的ntpsId
    logger.info("获取'文号'对应的ntpsId")
    all_parsed_docNumbers = parse_doc_num(extracted_numbers)
    all_docNo_ntpsIds = get_ntpsid_by_docNum(all_parsed_docNumbers)
    #3.2 合并key相同的数据
    #把通过文号搜出的结果合并到extracted_ntpsIds中
    merged_ids = append_ntpsIds(extracted_ntpsIds,all_docNo_ntpsIds)
    #3.3 确保ntpsId都存在与图数据库
    existedIds_in_graph = filter_existing_in_graphdb(merged_ids)
    #create_property_relations(existedIds_in_graph, ["48844","37001"])
    create_property_relations(existedIds_in_graph)
    a = ''

