from fastapi import FastAPI
from pydantic import BaseModel


class StatusBody(BaseModel):
    status: str
    biosamlpe_id: list
    tol_id: str
    species_name: str
    processing_status: str | None = None
    message: str | None = None


app = FastAPI()


@app.post("/status_update/")
async def status_update(status: StatusBody):
    if status.status not in [
        'sample_received', 'sample_released_for_lab_processing',
        'sample_in_assembly', 'sample_recollection_required'
    ]:
        status.processing_status = 'error'
        status.message = "Couldn't recognize status"
    else:
        status.processing_status = 'success'
    return status
