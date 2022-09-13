from elasticsearch import Elasticsearch
from fastapi import FastAPI, status, HTTPException, Depends
from pydantic import BaseModel
from fastapi.security import OAuth2PasswordRequestForm
from app.schemas import TokenSchema, SystemUser
from app.utils import (
    create_access_token,
    create_refresh_token,
    verify_password
)
from app.deps import get_current_user

es = Elasticsearch(['elasticsearch:9200'])


class StatusBody(BaseModel):
    status: str
    biosamlpe_id: list
    tol_id: str
    species_name: str
    processing_status: str | None = None
    message: str | None = None


app = FastAPI()


@app.post("/status_update/")
async def status_update(status: StatusBody,
                        user: SystemUser = Depends(get_current_user)):
    if status.status not in [
        'sample_received', 'sample_released_for_lab_processing',
        'sample_in_assembly', 'sample_recollection_required'
    ]:
        status.processing_status = 'error'
        status.message = "Couldn't recognize status"
    else:
        status.processing_status = 'success'
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
