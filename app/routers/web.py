import os
import pandas as pd
import datetime
from fastapi import APIRouter, Request, Form, BackgroundTasks, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from io import StringIO

import lib
from lib import resolve, SDOH_VARS, update_sdoh

router = APIRouter(
    prefix="",
    tags=["web"],
    responses={404: {"description": "Not found"}},
)

templates = Jinja2Templates(directory="templates/")


@router.get('/web/', include_in_schema=False)
def read_form(request: Request):
    return templates.TemplateResponse('base.html', context={'request': request})


@router.post('/web/jobs', include_in_schema=False)
async def display_job(request: Request, pwd: str = Form(default="")):
    form = await request.form()
    job_id = form.get("job_id")
    pwd = form.get("pwd")

    if not lib.auth(job_id, pwd):
        return templates.TemplateResponse('failed_auth.html', context={"request": request})

    context = {'request': request, "job_id": job_id}
    context["sdoh"] = SDOH_VARS.to_dict()

    sdoh_list = list(map(lambda sdoh: [f'{sdoh}', f'{SDOH_VARS[sdoh]}'], SDOH_VARS.index))
    context["sdoh_list_str"] = str(sdoh_list)
    context["sdoh_list"] = sdoh_list

    return templates.TemplateResponse('jobs.html', context)


@router.get('/jobs/{jid}', response_class=HTMLResponse, include_in_schema=False)
def _get_job_html(request: Request, jid: str):
    df = lib.get_job(jid)
    if df is not None:
        done, start, end, total, success, fail, complete = lib.get_status(jid)
        context = {"request": request, "job_id": jid, "done": done, "data": df.head(250)}

        completed = sum(~(df["rating"].isnull()))

        context['completed'] = complete
        context['failed'] = fail
        context['succeeded'] = success
        context['total'] = total

        if done:
            context['time'] = (end - start).total_seconds()

            context['ratings_list'] = str(list(df['rating'].values))

            if not lib.auth(jid, ""):  # temp token
                context['token'] = lib.issue_token(jid)[0]

        elif completed > 0:
            now = (datetime.datetime.now() - start).total_seconds()
            context['est_time'] = (total - complete) * (now / complete)
        return templates.TemplateResponse('components/result.html', context)
    else:
        return templates.TemplateResponse('failed_auth.html', context={"request": request})


@router.post('/jobs', include_in_schema=False)
async def _redirect_job(job_id: str, pwd: str):
    print(pwd)
    return RedirectResponse(url=f'/web/jobs/{job_id}')


@router.put('/jobs/{jid}', include_in_schema=False)
async def _change_sdoh_html(jid: str, request: Request):
    form = await request.form()
    sdoh_vars = form.getlist('sdoh')
    if sdoh_vars:
        update_sdoh(jid, sdoh_vars)
    return _get_job_html(request, jid)


@router.get('/geocode', include_in_schema=False, response_class=HTMLResponse)
async def _geocode_single_html(addr: str = "", addr1: str = "", addr2: str = "", city: str = "", state: str = "",
                               zip: str = ""):
    if addr:
        addr_str = addr
    else:
        if addr2:
            addr2 = f", {addr2}"
        addr_str = f"{addr1}{addr2}, {city}, {state} {zip}"

    return f"<hr>Address: {addr_str}<br>Tract: <mark>{resolve(addr_str)}</mark>"


@router.post("/upload", include_in_schema=False)
async def _upload_file_columns(request: Request, batchfile: UploadFile):
    form = await request.form()
    delim = form.get("delim")

    if not delim:
        return templates.TemplateResponse("components/error_alert.html",
                                          context={"request": request, "msg": "Must provide delimiter"})
    df = pd.read_csv(batchfile.file, delimiter=delim, dtype = 'str')

    context = {"request": request}
    context["df"] = df
    sdoh_list = list(map(lambda sdoh: [f'{sdoh}', f'{SDOH_VARS[sdoh]}'], SDOH_VARS.index))
    context["sdoh_list_str"] = str(sdoh_list)
    context["sdoh_list"] = sdoh_list

    if not delim:
        return templates.TemplateResponse("components/error_alert.html",
                                          context={"request": request, "msg": "Must provide delimiter"})

    return templates.TemplateResponse('components/col_select.html', context=context)


@router.post("/upload_map/", include_in_schema=False)
async def _upload_file_columns_map(request: Request, file: UploadFile):
    form = await request.form()
    delim = form.get("delimiter")

    if not delim:
        return templates.TemplateResponse("components/error_alert.html",
                                          context={"request": request, "msg": "Must provide delimiter"})
    df = pd.read_csv(file.file, delimiter=delim)

    context = {"request": request}
    context["df"] = df
    sdoh_list = list(map(lambda sdoh: [f'{sdoh}', f'{SDOH_VARS[sdoh]}'], SDOH_VARS.index))
    context["sdoh_list_str"] = str(sdoh_list)
    context["sdoh_list"] = sdoh_list

    return templates.TemplateResponse('components/col_select_map.html', context=context)


@router.post("/submit_job", include_in_schema=False)
async def _submit(request: Request, batchfile: UploadFile, bg: BackgroundTasks):
    form = await request.form()

    id_col = form.get("id_col")
    addr_col = form.get("addr_col")
    sdoh_vars = form.getlist("sdoh")
    if id_col == addr_col:
        return templates.TemplateResponse("components/error_alert.html", context={"request": request,
                                                                                  "msg": "Identifier column cannot be the same as the address column"})

    delim = form.get("delim")

    if not delim:
        return templates.TemplateResponse("components/error_alert.html",
                                          context={"request": request, "msg": "Must provide delimiter"})

    n_threads = int(form.get("threads"))

    split_components = form.get("address_components")

    if split_components:
        city_col = form.get("city_col")
        state_col = form.get("state_col")
        zip_col = form.get("zip_col")

        if not all([city_col, state_col, zip_col]):
            return templates.TemplateResponse("components/error_alert.html", context={"request": request,
                                                                                      "msg": "Must have columns for city, state, and zip code."})

    pwd = form.get("pwd")
    job = lib.new_job()

    df = pd.read_csv(batchfile.file, delimiter=delim, dtype = 'str')
    # address should not have * or " in it
    df[addr_col] = df[addr_col].astype(str).map(lambda s: s.replace('"', "")).map(lambda s: s.replace("*", ""))
    if split_components:
        df = df.sort_values(by=zip_col)  # sorting by zip might improve performance
        for c in [city_col, state_col, zip_col]:
            df[c] = df[c].astype(str).map(lambda s: s.replace('"', "")).map(lambda s: s.replace("*", ""))

    if id_col:
        duplicated = df.duplicated(subset=id_col)
        dupes = duplicated.sum()
        if dupes > 0:
            dupe_ids = df.loc[duplicated, id_col]
            response = ""
            return templates.TemplateResponse("components/error_alert.html",
                                              context={"request": request, "msg": "Error: " + str(dupes) + " duplicate identifiers. Resolve duplicates or deselect identifier column. Duplicates: '" + ", '".join(dupe_ids) + "'"})
        df = df.set_index(id_col)
    else:
        id_col = "id"
    df.index = df.index.rename("id")

    if split_components:
        df["address"] = (df[addr_col] + ", " + df[city_col] + ", " + df[state_col] + ", " + df[zip_col].astype(str))
        inp = df[["address"]]
    else:
        inp = df[[addr_col]].rename(columns={addr_col: "address"})

    lib.new_job_multithread(inp=inp, job=job, id_col=id_col, sdoh_vars=sdoh_vars, pwd=pwd, partitions=n_threads)
    bg.add_task(lib.submit_partitions, job, n_threads, sdoh_vars)

    return templates.TemplateResponse("components/confirm_submit.html", context={"request": request, "job": job})


@router.get("/web/map_variables/", include_in_schema=False)
async def map_variables(request: Request):  # do not need to save anything :)
    context = {"request": request}
    return templates.TemplateResponse("map_variables.html", context=context)


@router.get("/web/var_list/", include_in_schema=False)
async def var_list(request: Request):  # do not need to save anything :)
    context = {"request": request,
               "sources": lib.sdoh_databases.sort_values(by=["description", "version", "census_year", "granularity"]),
               "sdoh": lib.sdoh_variables}
    return templates.TemplateResponse("var_list.html", context=context)


@router.post("/web/map_variables/", include_in_schema=False)
async def do_map_variables(request: Request, file: UploadFile):
    context = {"request": request}

    form = await request.form()
    delim = form.get("delimiter")
    granularity = form.get('granularity')
    if granularity in ["longlat"]:
        long_col, lat_col = form.get('long_col'), form.get('lat_col')
    elif granularity in ["county", "tract", "bg"]:
        fips_col = form.get('granularity')
    else:
        return templates.TemplateResponse("components/error_alert.html",
                                          context={"request": request, "msg": "Invalid granularity"})

    if not delim:
        return templates.TemplateResponse("components/error_alert.html",
                                          context={"request": request, "msg": "Must provide delimiter"})
    df = pd.read_csv(file.file, delimiter=delim, dtype = 'str')

    sdoh_vars = form.getlist("sdoh")
    if not sdoh_vars:
        return templates.TemplateResponse("components/error_alert.html", context={"request": request,
                                                                                  "msg": "Must select geographic variables to include"})

    for v in sdoh_vars:
        if v not in lib.sdoh_variables.index:
            return templates.TemplateResponse("components/error_alert.html",
                                              context={"request": request, "msg": "Invalid geographic variables"})

        level = lib.sdoh_variables.loc[v, "level"]
        if (level == "tract" and granularity in ['county']) or (level == "bg" and granularity in ['tract', 'county']):
            return templates.TemplateResponse("components/error_alert.html", context={"request": request,
                                                                                      "msg": v + " requires geographic data at the " + level + "-level resolution"})

    if granularity == "longlat":
        try:  # need to watch for special characters to prevent sqli
            df[fips_col] = df[fips_col].dropna().astype(float)
        except:
            return templates.TemplateResponse("components/error_alert.html", context={"request": request,
                                                                                      "msg": "Longitude and latitude must be numeric"})
    else:
        try:  # need to watch for special characters to prevent sqli
            df[fips_col] = df[fips_col].dropna().astype("int64").astype(str)
        except:
            return templates.TemplateResponse("components/error_alert.html",
                                              context={"request": request, "msg": "Geolocator must be in FIPS format"})

    if granularity == "longlat":
        return templates.TemplateResponse("components/error_alert.html", context={"request": request,
                                                                                  "msg": "Longitude and latitude not supported yet"})
    else:
        sdf = lib.query_sdoh(list(df[fips_col].value_counts().index), granularity, sdoh_vars).set_index("fips")

        df = df.merge(sdf, left_on=fips_col, right_index=True, how="left")

    tmp_id = lib.save_tmp(df)

    context["df"] = df
    context["link"] = f"/api/map/download/{tmp_id}"
    return templates.TemplateResponse("components/map_result.html", context=context)


@router.get("/web/jobs/{job_id}/delete", include_in_schema=False)
def _delete_job(job_id: int, token: str = ""):
    if not lib.auth_token(job_id, token):
        return {"auth": "failed"}

    # delete from jobs and address table
    lib.delete_job(job_id)

    #     response.headers['HX-Redirect'] = "/web/"
    return RedirectResponse(url=f'/web/')

@router.get("/test", include_in_schema=False, response_class=HTMLResponse) # ONLY FOR DEV
async def test(bg: BackgroundTasks):
#     lib.query_sdoh([212219703021, 471870509062], "bg", ["ACS_PCT_VET"])
    bg.add_task(lib.submit_partitions, 25, 6, ['Primary_RUCA_Code_2010'])
    return "test3"
