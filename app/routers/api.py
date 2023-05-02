import datetime
import json
import os
import pandas as pd
from enum import Enum
from fastapi import APIRouter, Form, Query, UploadFile, BackgroundTasks
from fastapi.responses import FileResponse
from pydantic import BaseModel

import lib
from lib import resolve, update_sdoh, SDOH_VARS

HOST = ""

router = APIRouter(
    prefix="/api",
    tags=["api"],
    responses={404: {"description": "Not found"}},
)


class TableFormat(str, Enum):
    index = "index",
    split = "split"


class GeographicVariables(BaseModel):
    vars: list[str]

    @classmethod
    def __get_validators__(cls):
        yield cls.validate_to_json

    @classmethod
    def validate_to_json(cls, value):
        if isinstance(value, str):
            return cls(**json.loads(value))
        return value


@router.get("/address/{address_string}")
def _geocode_single_address(address_string: str):
    tract = resolve(address_string)
    return {"tract": tract}


@router.post("/jobs/")  # should this be split into two?
def _submit_job(data_csv: UploadFile, bg: BackgroundTasks,
                geographic_vars: GeographicVariables | None = Form(default=None),
                address_col: str = "address", city_col: str = "", state_col: str = "", zip_col: str = "",
                id_col: str = "", delimiter: str = ",", password: str = Form(default=""),
                n_threads: int = 1):
    if id_col == address_col:
        return {"error": "Identifier column cannot be the same as the address column"}
    if n_threads < 1 or n_threads > 5:
        return {"error": "Number of threads must be between 1 and 5."}

    split_components = False
    if all([city_col, state_col, zip_col]):
        split_components = True
    elif any([city_col, state_col, zip_col]):
        return {
            "error": "If using separate columns for each, address components, all of 'city_col', 'state_col' and 'zip_col' must have a value."}

    job = lib.new_job()
    #     os.mkdir(f"jobs/{job}")

    df = pd.read_csv(data_csv.file, delimiter=delimiter, dtype = 'str')

    all_cols = [address_col, city_col, state_col, zip_col] if split_components else [address_col]
    if id_col:
        all_cols.append(id_col)
    for c in all_cols:
        if c not in df:
            return {"error": f"Data file does not contain column: '{c}'"}

    if id_col:
        duplicated = df.duplicated(subset=id_col)
        dupes = duplicated.sum()
        if dupes > 0:
            dupe_ids = df.loc[duplicated, id_col]
            response = ""
            return {"error": str(dupes) + " duplicate identifiers. Resolve duplicates or deselect identifier column. Duplicates: '" + ", '".join(dupe_ids) + "'"}

        df = df.set_index(id_col)
    else:
        id_col = "id"
    df.index = df.index.rename("id")

    #     df.to_csv(f"jobs/{job}/data.csv")

    df[address_col] = df[address_col].map(lambda s: s.replace('"', "")).map(lambda s: s.replace("*", ""))
    if split_components:
        for c in [city_col, state_col, zip_col]:
            df[c] = df[c].astype(str).map(lambda s: s.replace('"', "")).map(lambda s: s.replace("*", ""))
        df = df.sort_values(by=state_col)
        df["address"] = (df[address_col] + ", " + df[city_col] + ", " + df[state_col] + ", " + df[zip_col].astype(
            str))  # (f"jobs/{job}/inp.csv")
        inp = df[["address"]]
    else:
        inp = df[[address_col]].rename(columns={address_col: "address"})

    sdoh_vars = []

    if geographic_vars is not None:
        sdoh_vars = geographic_vars.vars
        for v in geographic_vars.vars:
            if v not in SDOH_VARS.index:
                return {"error": "invalid geographic variables"}

    lib.new_job_multithread(inp=inp, job=job, id_col=id_col, sdoh_vars=sdoh_vars, pwd=password, partitions=n_threads)
    bg.add_task(lib.submit_partitions, job, n_threads, sdoh_vars)

    return {"job_id": job}


@router.get("/jobs/{job_id}")
def _get_job(job_id: int,
             data_table: bool = Query(default=False, title="Data Table",
                                      description="Return raw data table in json form."),
             table_format: TableFormat = TableFormat.index,  # index or split
             token: str = Query(default="", title="Token",
                                description="Authentication token. Leave blank if no password set. POST to /api/tokens to issue a token (expires in one day).")
             ):
    if not lib.auth_token(job_id, token):
        return {"auth": "failed"}

    df = lib.get_job(job_id)
    if df is not None:
        done, start, end, total, success, fail, complete = lib.get_status(job_id)

        json = {"info": {"id": job_id, "done": done, "start_time": start}}

        completed = sum(~(df["rating"].isnull()))

        json['info']['completed'] = complete
        json['info']['failed'] = fail
        json['info']['succeeded'] = success

        if done:
            json["info"]["end_time"] = end
            json["info"]["run_time"] = (end - start).total_seconds()
            if data_table:
                json["data"] = df.fillna("").to_dict(orient=table_format.value)
        if token:
            json["download"] = HOST + "/api/" + str(job_id) + "/download?token=" + token
        else:
            json["download"] = HOST + "/api/" + str(job_id) + "/download"
        return json
    else:
        return {"error": "invalid job ID", "job": job_id}


@router.put("/jobs/{job_id}", status_code=200)
def _update_geographic_variables(job_id: int, geographic_vars: GeographicVariables,
                                 token: str = Query(default="", title="Token",
                                                    description="Authentication token. Leave blank if no password set. POST to /api/tokens to issue a token (expires in one day).")):
    if not lib.auth_token(job_id, token):
        return {"auth": "failed"}
    update_sdoh(str(job_id), geographic_vars.vars)


@router.delete("/jobs/{job_id}")
def _delete_job(job_id: int, token: str = Query(default="", title="Token",
                                                description="Authentication token. Leave blank if no password set. POST to /api/tokens to issue a token (expires in one day).")):
    if not lib.auth_token(job_id, token):
        return {"auth": "failed"}

    # delete from jobs and address table
    lib.delete_job(job_id)
    return {"delete": "success"}


@router.get("/jobs/{job_id}/status")
def _job_status(job_id: int, token: str = Query(default="", title="Token",
                                                description="Authentication token. Leave blank if no password set. POST to /api/tokens to issue a token (expires in one day).")):
    if not lib.auth_token(job_id, token):
        return {"auth": "failed"}

    res = lib.get_status(job_id)
    if res is not None:
        done, start, end, total, success, fail, complete = res
        json = {"done": done, "completed": complete, "start_time": start}

        json['failed'] = fail
        json['succeeded'] = success

        if done:
            json["run_time"] = (end - start).total_seconds()
        else:
            time = (datetime.datetime.now() - start).total_seconds()
            est_time = "inf"
            if complete > 0:
                est_time = (total - complete) * (time / complete)
            json["run_time"] = time
            json["estimated_time_left"] = est_time
        return json
    else:
        return {"out": "failure: invalid job ID", "job": job_id}


@router.get("/jobs/{job_id}/download")
def _download(bg: BackgroundTasks, job_id: int, token: str = Query(default="", title="Token",
                                                                   description="Authentication token. Leave blank if no password set. POST to /api/tokens to issue a token (expires in one day).")):
    if not lib.auth_token(job_id, token):
        return {"auth": "failed"}

    path = lib.generate_out(job_id, input_addr=True, long_lat=True, norm_addr=True, split_norm_addy=True)
    bg.add_task(os.unlink, path)
    return FileResponse(path=f"temp/{job_id}_out.csv", filename=str(job_id) + "_out.csv", media_type='text/csv')


@router.get("/jobs/{job_id}/failed/download")
def _download_failed(bg: BackgroundTasks, job_id: int, token: str = Query(default="", title="Token",
                                                                          description="Authentication token. Leave blank if no password set. POST to /api/tokens to issue a token (expires in one day).")):
    if not lib.auth_token(job_id, token):
        return {"auth": "failed"}

    path = lib.generate_failed(job_id, input_addr=True, long_lat=True, norm_addr=True, split_norm_addy=True)
    bg.add_task(os.unlink, path)
    return FileResponse(path=f"temp/{job_id}_failed.csv", filename=str(job_id) + "_failed.csv", media_type='text/csv')


@router.post("/map")
def _map_geocoded_to_geographic_variables(data_csv: UploadFile, delimiter: str = ",",
                                          tract_col: str = "tract",
                                          geographic_vars: GeographicVariables | None = Form(default=None)):
    df = pd.read_csv(data_csv.file, delimiter=delimiter, dtype = 'str')

    if tract_col not in df:
        return {"error": f"Data file does not contain column: '{c}'"}

    try:  # need to watch for special characters to prevent sqli
        df[tract_col] = df[tract_col].dropna().astype(int)
    except:
        return templates.TemplateResponse("components/error_alert.html",
                                          context={"request": request, "msg": "Tract must be in FIPS format"})

    for v in geographic_vars.vars:
        if v not in SDOH_VARS.index:
            return {"error": "invalid geographic variables"}

    sdoh_vars = []

    if geographic_vars is not None:
        sdoh_vars = geographic_vars.vars

    sdf = lib.query_sdoh(list(df[tract_col].value_counts().index), sdoh_vars).set_index("tractfips")

    df = df.merge(sdf, left_on=tract_col, right_index=True, how="left")

    tmp_id = lib.save_tmp(df)

    return {"link": f"/api/map/download/{tmp_id}.csv"}


@router.get("/map/download/{id}", include_in_schema=False)
async def do_map_variables(id: str, bg: BackgroundTasks):
    path = f"temp/{id}.csv"
    bg.add_task(os.unlink, path)
    return FileResponse(path=path, filename="mapped_out.csv", media_type='text/csv', background=bg)


@router.post("/tokens")
def _issue_token(job_id: int = Form(), password: str = Form()):
    if lib.auth(job_id, password):
        token, exp = lib.issue_token(job_id)
        return {"token": token, "expiration": exp}
    else:
        return {"auth": "failed"}
