from elasticsearch import AsyncElasticsearch
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
async def index(data_index: str, offset: int = 0, limit: int = 15, filter: str | None = None):
    body = dict()

    # Aggregations
    body["aggs"] = dict()
    body["aggs"]['journalTitle'] = {
        "terms": {"field": 'journalTitle'}
    }
    body["aggs"]['pubYear'] = {
        "terms": {"field": 'pubYear'}
    }

    # Filters
    if filter:
        filters = filter.split(",")
        body["query"] = {
            "bool": {
                "filter": list()
            }
        }
        for filter_item in filters:
            filter_name, filter_value = filter_item.split(":")
            body["query"]["bool"]["filter"].append({"term": {filter_name: filter_value}})
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
