import datetime
import numpy as np
import os
import pandas as pd
import asyncio

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from routers import api, web
from fastapi_utils.tasks import repeat_every

from lib import sweep_jobs

app = FastAPI()

app.include_router(web.router)
app.include_router(api.router)

app.mount("/assets", StaticFiles(directory="assets"), name="assets")

@app.on_event("startup")
@repeat_every(seconds=60 * 60)  # every hour
def clean_jobs():
    # remove jobs that need to be deleted
    sweep_jobs()

