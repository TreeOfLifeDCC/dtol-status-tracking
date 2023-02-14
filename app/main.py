from elasticsearch import AsyncElasticsearch
from neo4j import GraphDatabase
from fastapi import FastAPI, status, HTTPException, Depends
from pydantic import BaseModel
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.middleware.cors import CORSMiddleware
from app.schemas import TokenSchema, SystemUser
from app.utils import (
    create_access_token,
    create_refresh_token,
    verify_password
)
from app.deps import get_current_user


es = AsyncElasticsearch(['elasticsearch:9200'])


class StatusBody(BaseModel):
    status: str
    biosamlpe_id: list
    tol_id: str
    species_name: str
    processing_status: str | None = None
    message: str | None = None

class NeoFourJ:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def get_rank(self, param):
        with self.driver.session() as session:
            rank = session.write_transaction(self._get_rank, param)
            return rank

    @staticmethod
    def _get_rank(tx, param):
        result = tx.run(
            'MATCH (parent:Taxonomies)-[:CHILD]->(child:Taxonomies) where parent.name=~' '"' '.*' + param + '.*' '"'
                                                                                                            'RETURN parent')
        return result.single()[0]


app = FastAPI()

origins = [
    "*"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/{data_index}")
async def index(data_index: str, offset: int = 0, limit: int = 15, articleType: str | None = None,
                journalTitle: str | None = None, pubYear: str | None = None):
    body = dict()

    # Aggregations
    body["aggs"] = dict()
    body["aggs"]['journalTitle'] = {
        "terms": {"field": "journalTitle"}
    }
    body["aggs"]['pubYear'] = {
        "terms": {"field": "pubYear"}
    }
    body["aggs"]["articleType"] = {
        "terms": {"field": "articleType"}
    }

    # Filters
    if articleType or journalTitle or pubYear:
        body["query"] = {
            "bool": {
                "filter": list()
            }
        }
    if articleType:
        body["query"]["bool"]["filter"].append({"term": {'articleType': articleType}})
    if journalTitle:
        body["query"]["bool"]["filter"].append({"term": {'journalTitle': journalTitle}})
    if pubYear:
        body["query"]["bool"]["filter"].append({"term": {'pubYear': pubYear}})
    response = await es.search(index=data_index, from_=offset, size=limit, body=body)
    data = dict()
    data['count'] = response['hits']['total']['value']
    data['results'] = response['hits']['hits']
    data['aggregations'] = response['aggregations']
    return data


@app.post("/status_update/")
async def status_update(status: StatusBody, user: SystemUser = Depends(get_current_user)):
    if status.status not in [
        'sample_received', 'sample_released_for_lab_processing',
        'sample_in_sequencing', 'sample_in_assembly',
        'sample_recollection_required'
    ]:
        status.processing_status = 'error'
        status.message = "Couldn't recognize status"
    search_results = es.search(index='data_portal',
                               q=f"_id:{status.species_name}")
    try:
        search_results = search_results['hits']['hits'][0]['_source']
        search_results[status.status] = True
        await es.index(index='data_portal', document=search_results, id=status.species_name)
        status.processing_status = 'success'
        status.message = [
            f'status {status.status} was updated for {status.species_name}']
    except IndexError:
        status.processing_status = 'error'
        status.message = "This record doesn't exist in data portal"
    return status


@app.post('/login', summary="Create access and refresh tokens for user",
          response_model=TokenSchema)
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    user = es.search(index='user', q=f"_id:{form_data.username}")
    try:
        user = user['hits']['hits'][0]['_source']
    except IndexError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Incorrect username"
        )

    hashed_pass = user['password']
    if not verify_password(form_data.password, hashed_pass):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Incorrect password"
        )

    return {
        "access_token": create_access_token(user['username']),
        "refresh_token": create_refresh_token(user['username']),
    }



@app.get("/downloader_utility_data")
async def index(taxonomyFilter: str, data_status: str, experiment_type: str):
    neofourJ = NeoFourJ("bolt://45.88.80.141:30087", "neo4j", "DtolNeo4jAdminUser@123")
    query_param = ' { "'"from"'" : 0, "'"size"'" : 5000, "'"query"'" : { "'"bool"'" : { "'"must"'" : [ '
    if taxonomyFilter != '':
        result = neofourJ.get_rank(taxonomyFilter)
        query_param = query_param + '{ "nested" : { "path" : "taxonomies", "query" : { "nested" : { ' \
                                            '"path" : ' \
                                            '"taxonomies.' + result._properties.get(
                                                'rank') + '"' ', "query" : { "bool" : { ' \
                              '"must" : [{ ' \
                              '"term" : { ' \
                              '"taxonomies.' + result._properties.get(
                                  'rank') + '.scientificName" :''"' + result._properties.get('name') + '"' '}}]}}}}}} '
    if data_status is not None and data_status != '':
        split_array = data_status.split("-")
    if split_array and split_array[0].strip() == 'Biosamples':
        query_param = query_param + ',{ "terms" : { "biosamples" : [''"' + split_array[1].strip() + '"'']}}'
    elif split_array and split_array[0].strip() == 'Raw Data':
        query_param = query_param + ',{ "terms" : { "raw_data" : [''"' + split_array[1].strip() + '"'']}}'
    elif split_array and split_array[0].strip() == 'Mapped Reads':
        query_param = query_param + ',{ "terms" : { "mapped_reads" : [''"' + split_array[
        1].strip() + '"'']}}'
    elif split_array and split_array[0].strip() == 'Assemblies':
        query_param = query_param + ',{ "terms" : { "assemblies_status" : [''"' + split_array[
        1].strip() + '"'']}}'
    elif split_array and split_array[0].strip() == 'Annotation Complete':
        query_param = query_param + ',{ "terms" : { "annotation_complete" : [''"' + split_array[
        1].strip() + '"'']}}'
    elif split_array and split_array[0].strip() == 'Annotation':
        query_param = query_param + ',{ "terms" : { "annotation_status" : [''"' + split_array[
        1].strip() + '"'']}}'
    elif split_array and split_array[0].strip() == 'Genome Notes':
        query_param = query_param + ',{ "nested": {"path": "genome_notes","query": {"bool": {"must": [{"exists": ' \
                                            '{"field": "genome_notes.url"}}]}}}} '
    if experiment_type != '' and experiment_type is not None:
        query_param = query_param + ',{ "nested" : { "path": "experiment", "query" : { "bool" : { "must" : [' \
                                        '{ "term" : { "experiment.library_construction_protocol.keyword" : ' + \
                          '"' + experiment_type + '"' '}}]}}}}'

    query_param = query_param + '] }}}'

    response = es.search(index="data_portal", size=10000, body=query_param)
    neofourJ.close()
    return response