import os

from elasticsearch import AsyncElasticsearch, AIOHttpConnection
from fastapi import FastAPI, status, HTTPException, Depends
from pydantic import BaseModel
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.middleware.cors import CORSMiddleware


ES_HOST = os.getenv('ES_HOST')
ES_USERNAME = os.getenv('ES_USERNAME')
ES_PASSWORD = os.getenv('ES_PASSWORD')

es = AsyncElasticsearch(
    [ES_HOST], connection_class=AIOHttpConnection,
    http_auth=(ES_USERNAME, ES_PASSWORD),
    use_ssl=True, verify_certs=False)


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


@app.get("/downloader_utility_data_with_species/")
async def downloader_utility_data_with_species(species_list: str):
    body = dict()
    result = []
    if species_list != '' and species_list is not None:
        species_list_array = species_list.split(",")
        for organism in species_list_array:
            body["query"] = {
                "bool": {"filter": [{'term': {'_id': organism}}]}}
            response = await es.search(index='data_portal',
                                 body=body)
            result.extend(response['hits']['hits'])
    return result


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

