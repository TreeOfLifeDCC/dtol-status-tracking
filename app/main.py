from elasticsearch import AsyncElasticsearch
from fastapi import FastAPI, status, HTTPException, Depends
from pydantic import BaseModel
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.middleware.cors import CORSMiddleware
from app.neofourj import NeoFourJ
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


@app.get("/downloader_utility_data/")
async def downloader_utility_data(taxonomy_filter: str, data_status: str, experiment_type: str):
    neofourJ = NeoFourJ()
    body = dict()
    if taxonomy_filter != '':
        result = neofourJ.get_rank(taxonomy_filter)
        if taxonomy_filter:
            body["query"] = {
                "bool": {
                    "filter": list()
                }
            }
            nested_dict = {
                "nested": {
                    "path": f"taxonomies.{result.get('parent').get('rank')}",
                    "query": {
                        "bool": {
                            "filter": list()
                        }
                    }
                }
            }
            nested_dict["nested"]["query"]["bool"]["filter"].append(
                {
                    "term": {
                        f"taxonomies.{result.get('parent').get('rank')}.scientificName": result.get('parent').get('name')
                    }
                }
            )
            body["query"]["bool"]["filter"].append(nested_dict)
    if data_status is not None and data_status != '':
        split_array = data_status.split("-")
        if split_array and split_array[0].strip() == 'Biosamples':
            body["query"]["bool"]["filter"].append(
                {"term": {'biosamples': split_array[1].strip()}}
            )
        elif split_array and split_array[0].strip() == 'Raw Data':
            body["query"]["bool"]["filter"].append(
                {"term": {'raw_data': split_array[1].strip()}}
            )
        elif split_array and split_array[0].strip() == 'Mapped Reads':
            body["query"]["bool"]["filter"].append(
                {"term": {'mapped_reads': split_array[1].strip()}})

        elif split_array and split_array[0].strip() == 'Assemblies':
            body["query"]["bool"]["filter"].append(
                {"term": {'assemblies_status': split_array[1].strip()}})
        elif split_array and split_array[0].strip() == 'Annotation Complete':
            body["query"]["bool"]["filter"].append(
                {"term": {'annotation_complete': split_array[1].strip()}})
        elif split_array and split_array[0].strip() == 'Annotation':
            body["query"]["bool"]["filter"].append(
                {"term": {'annotation_status': split_array[1].strip()}})
        elif split_array and split_array[0].strip() == 'Genome Notes':
            nested_dict = {
                "nested": {
                    "path": "genome_notes",
                    "query": {
                        "bool": {
                            "must": [{
                                "exists": {
                                    "field": "genome_notes.url"
                                }
                            }]
                        }
                    }
                }
            }
            body["query"]["bool"]["filter"].append(nested_dict)
    if experiment_type != '' and experiment_type is not None:
        nested_dict = {
            "nested": {
                "path": "experiment",
                "query": {
                    "bool": {
                        "must": [{
                            "term": {
                                "experiment.library_construction_protocol.keyword": experiment_type
                            }
                        }]
                    }
                }
            }
        }
        body["query"]["bool"]["filter"].append(nested_dict)
    response = await es.search(index="data_portal", from_=0, size=10000, body=body)
    total_count = response['hits']['total']['value']
    result = response['hits']['hits']
    results_count = len(response['hits']['hits'])
    while total_count > results_count:
        response1 = await es.search(index="data_portal", from_=results_count, size=10000, body=body)
        result.extend(response1['hits']['hits'])
        results_count += len(response1['hits']['hits'])
    neofourJ.close()
    return result


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


@app.post("/status_update/")
async def status_update(status: StatusBody, user: SystemUser = Depends(get_current_user)):
    if status.status not in [
        'sample_received', 'sample_released_for_lab_processing',
        'sample_in_sequencing', 'sample_in_assembly',
        'sample_recollection_required'
    ]:
        status.processing_status = 'error'
        status.message = "Couldn't recognize status"
        return status
    search_results = await es.search(index='data_portal', q=f"_id:{status.species_name}")
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
    user = await es.search(index='user', q=f"_id:{form_data.username}")
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

