import datetime
import hashlib
import numpy as np
import os
import pandas as pd
import psycopg2
import re
import secrets
from sqlalchemy import create_engine

import worker

from db_constants import DBHOST, DBNAME, DBUSER, DBPASS, DBYEAR, OTHER_YEAR_BG, TABLENAME


engine = create_engine(f'postgresql://{DBUSER}:{DBPASS}@{DBHOST}:5432/{DBNAME}')

sdoh_databases = None
sdoh_variables = None

def resolve(address, sdoh=[]):
    conn = psycopg2.connect(f"host={DBHOST} dbname={DBNAME} user={DBUSER} password={DBPASS}")
    cur = conn.cursor()

    query = "SELECT get_tract(ST_AsText(ST_SnapToGrid(g.geomout,0.00001)), 'tract_id') FROM geocode(%s,1) AS g"

    cur.execute(query, (address,))
    record = cur.fetchone()

    ret = None

    if record:
        ret = record[0]
    else:
        return None  # could not resolve

    cur.close()
    conn.close()
    return ret


def new_job():  # inserts row, returns job id
    with psycopg2.connect(f"host={DBHOST} dbname={DBNAME} user={DBUSER} password={DBPASS}") as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO jobs DEFAULT VALUES RETURNING id")
            id = cur.fetchone()[0]
            conn.commit()
            return id


def new_job_multithread(inp, job, id_col="id", sdoh_vars=[], partitions=5, pwd=""):
    partitions_done = [False for _ in range(partitions)]

    if pwd != "":
        salt = "geocode" + str(job) + "geocode"
        pw_salted_hash = hashlib.pbkdf2_hmac('sha256', pwd.encode(), salt.encode(), 100000)
    else:
        pw_salted_hash = None

    # create job
    start = datetime.datetime.now()
    with psycopg2.connect(f"host={DBHOST} dbname={DBNAME} user={DBUSER} password={DBPASS}") as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE jobs SET (starttime, done, sdoh, id_col, multithread, partitions, pwd) = (%s, %s, %s, %s, %s, %s, %s) WHERE id = %s",
                (start, False, sdoh_vars, id_col, True, partitions_done, pw_salted_hash, job))

    num_addr = len(inp)

    inp["address"] = inp["address"].map(clean_address)

    inp['partition'] = 0

    indexes = np.array_split(inp.index, partitions, axis=0)
    for i, index in enumerate(indexes):
        inp.loc[index, 'partition'] = i

    inp.columns = [c.lower() for c in inp.columns]
    inp["job"] = job
    inp.to_sql(TABLENAME, engine, if_exists="append")

    engine.dispose()

def submit_partitions(job, partitions=5, sdoh_vars=[]):
    # def t(i):
    #     resolve_batch_partition(job, i, sdoh_vars)
    #
    # Parallel(n_jobs=-1, backend="threading")(map(delayed(t), [i for i in range(partitions)]))
    tasks = []
    for i in range(partitions):
        tasks.append(worker.resolve_batch_partition.apply_async([job, i, sdoh_vars],
                                                   link_error=worker.error_handler.s(),
                                                   # task_id='geocode-' + str(job) + '-' + str(i),
                                                          task_track_started=True))
    return tasks


def get_job(job, input_addr=True, long_lat=True, norm_addr=True, split_norm_addy=False, limit=-1):
    tablename = "master_address_table"

    done = False

    with psycopg2.connect(f"host={DBHOST} dbname={DBNAME} user={DBUSER} password={DBPASS}") as conn:
        with conn.cursor() as cur:

            cur.execute(f"SELECT sdoh, id_col FROM jobs WHERE id = %s", (job,))
            res = cur.fetchone()
            if res:
                sdoh_vars, id_col = res
            else:
                return None

            # determine which tables are necessary
            sdoh_tables = []
            if sdoh_vars:
                dx = sdoh_variables.loc[sdoh_vars]
                sdoh_tables = sdoh_variables.loc[sdoh_vars, "source_id"].unique()

                census_years = sdoh_databases.loc[sdoh_tables, "census_year"]
                levels = sdoh_databases.loc[sdoh_tables, "granularity"]

                sdoh_columns = (dx["source"] + "_" + dx["version"] + "_" + dx["level"] + "." + sdoh_vars).values

            cols = ["id", "rating"]
            if input_addr:
                cols.append("address")
            if long_lat:
                cols.extend(["long", "lat"])
            if norm_addr:
                if split_norm_addy:
                    cols.extend(["new_stno", "new_st", "new_st_type", "new_city", "new_zip"])
                else:
                    cols.append("new_address")

            #             cols.append('tract')
            cols.append('bg_' + DBYEAR)
            for _, i in OTHER_YEAR_BG.items():
                cols.append(i["col"])

            if sdoh_vars:
                cols.extend(sdoh_columns)

            # TODO: LIMIT to top 250, need to left join in merge other cols (would not be 1:1 anymore)
            sql_str = "SELECT " + ", ".join(cols) + f" FROM {tablename}"
            for t in sdoh_tables:
                if levels[t] == "tract":
                    sql_str += f" LEFT JOIN sdoh.{t} on CAST(LEFT({tablename}.bg_{str(census_years[t])}, -1) AS bigint) = {t}.tractfips"
                elif levels[t] == "county":  # census year doesn't matter because county doesn't change...
                    sql_str += f" LEFT JOIN sdoh.{t} on CAST(LEFT({tablename}.bg_{str(census_years[t])}, -7) AS bigint) = {t}.countyfips"
                elif levels[t] == "blockgroup":  # census year doesn't matter because county doesn't change...
                    sql_str += f" LEFT JOIN sdoh.{t} on CAST({tablename}.bg_{str(census_years[t])} AS bigint) = {t}.blockgroupfips"
                else:
                    print('... bad granularity: ', levels[t])
            sql_str += f" WHERE {tablename}.job = %s ORDER BY {tablename}.id"

            #             sql_str = "SELECT " +  ", ".join(cols) + f" FROM {tablename} LEFT JOIN {SDOH_TABLE} on CAST({tablename}.tract AS bigint) = {SDOH_TABLE}.tractfips WHERE {tablename}.job = %s ORDER BY {tablename}.id"

            if limit != -1:
                sql_str += " LIMIT " + str(limit)

            cur.execute(sql_str, (job,))
            res = cur.fetchall()

            df = pd.DataFrame(res, columns=cols).set_index("id")
            df.index = df.index.rename(id_col)
            df = df.sort_values(by="rating")

    return df


def get_status(job):
    done = False
    i = worker.celery.control.inspect()
    with psycopg2.connect(f"host={DBHOST} dbname={DBNAME} user={DBUSER} password={DBPASS}") as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT starttime, endtime, done, partitions FROM jobs WHERE id = %s", (job,))
            res = cur.fetchone()
            if res:
                starttime, endtime, done, partitions = res
            else:
                return None

            cur.execute(f"SELECT count(*) FROM {TABLENAME} WHERE job = %s", (job,))
            total = cur.fetchone()[0]

            cur.execute(f"SELECT count(*) FROM {TABLENAME} WHERE job = %s AND rating >= 0 AND rating < 25", (job,))
            success = cur.fetchone()[0]

            cur.execute(f"SELECT count(*) FROM {TABLENAME} WHERE job = %s AND (rating = -1 OR rating >= 25)", (job,))
            fail = cur.fetchone()[0]

            cur.execute(f"SELECT count(*) FROM {TABLENAME} WHERE job = %s AND rating IS NOT NULL", (job,))
            complete = cur.fetchone()[0]

            if complete == total and not done:
                print('completed job', job, "but not marked complete")
                cur.execute(f"UPDATE jobs SET done = TRUE WHERE id = %s", (job,))
                done = True

    return done, starttime, endtime, total, success, fail, complete


def terminate_job(job, task_ids):
    if task_ids:
        print("terminating", task_ids)
        worker.celery.control.terminate(task_ids)


def suspend_job(job, task_ids):
    terminate_job(job, task_ids)
    worker.fips_year.apply_async([job, DBYEAR], link_error=worker.error_handler.s(),
                                                       task_id='fips-' + str(job))
    worker.fips_year.apply_async([job, DBYEAR], link_error=worker.error_handler.s(),
                                         task_id='fips2010-' + str(job))

    with psycopg2.connect(f"host={DBHOST} dbname={DBNAME} user={DBUSER} password={DBPASS}") as conn:
        with conn.cursor() as cur:
            cur.execute(f"UPDATE jobs SET done = TRUE WHERE id = %s", (job,))


def resume_job(job, task_ids):
    terminate_job(job, task_ids)
    with psycopg2.connect(f"host={DBHOST} dbname={DBNAME} user={DBUSER} password={DBPASS}") as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT partitions FROM jobs WHERE id = %s", (job,))
            res = cur.fetchone()
            if res:
                partitions = res[0]
                n_threads = len(partitions)

                cur.execute(f"UPDATE jobs SET done = FALSE WHERE id = %s", (job,))
                return n_threads

def auth(job, pwd=""):
    with psycopg2.connect(f"host={DBHOST} dbname={DBNAME} user={DBUSER} password={DBPASS}") as conn:
        with conn.cursor() as cur:
            if pwd == "":
                cur.execute("SELECT pwd FROM jobs WHERE id = %s AND pwd IS NULL", (job,))
            else:
                salt = "geocode" + str(job) + "geocode"
                pw_salted_hash = hashlib.pbkdf2_hmac('sha256', pwd.encode(), salt.encode(), 100000)

                cur.execute("SELECT pwd FROM jobs WHERE id = %s AND pwd = %s", (job, pw_salted_hash))

            res = cur.fetchone()
            if res:
                return True
            else:
                return False


def auth_token(job, token=""):
    if token == "":
        return auth(job, "")

    with psycopg2.connect(f"host={DBHOST} dbname={DBNAME} user={DBUSER} password={DBPASS}") as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT expiration FROM tokens WHERE token = %s AND job = %s", (token, job))
            res = cur.fetchone()
            if res:
                exp = res[0]
                if datetime.datetime.now() < exp:
                    return True
                else:
                    return False
            else:
                return False


def issue_token(job, exp_days=1):
    with psycopg2.connect(f"host={DBHOST} dbname={DBNAME} user={DBUSER} password={DBPASS}") as conn:
        with conn.cursor() as cur:
            expire = datetime.datetime.now() + datetime.timedelta(days=exp_days)
            token = secrets.token_urlsafe(32)
            cur.execute("INSERT INTO tokens (job, expiration, token) VALUES (%s, %s, %s)", (job, expire, token))

            return token, expire


def update_sdoh(job, sdoh_vars):
    with psycopg2.connect(f"host={DBHOST} dbname={DBNAME} user={DBUSER} password={DBPASS}") as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE jobs SET sdoh = %s WHERE id = %s", (sdoh_vars, job))


#     generate_out(job)
#     df = get_job(job)
#     df.to_csv(f"jobs/{job}/out.csv")

def generate_out(job, input_addr=True, long_lat=True, norm_addr=True, split_norm_addy=True):
    df = get_job(job, input_addr=input_addr, long_lat=long_lat, norm_addr=norm_addr, split_norm_addy=split_norm_addy)
    done, _, _, _, _, _, _ = get_status(job)

    if not os.path.exists('temp'):
        os.mkdir('temp')

    if done:
        path = f"temp/{job}_out.csv"
        df.to_csv(path)
        return path
    else:
        raise Exception("Can only generate output for a completed job")


def generate_failed(job, input_addr=True, long_lat=True, norm_addr=True, split_norm_addy=True):
    df = get_job(job, input_addr=input_addr, long_lat=long_lat, norm_addr=norm_addr, split_norm_addy=split_norm_addy)
    done, _, _, _, _, _, _ = get_status(job)
    if done:
        path = f"temp/{job}_failed.csv"
        df.loc[(df['rating'] == -1) | (df['rating'] >= 25)].to_csv(path)
        return path
    else:
        raise Exception("Can only generate output for a completed job")


def query_sdoh(fips=[], fips_type="tract", sdoh_vars=[]):
    cols = ['fips'] + sdoh_vars
    fips_table = "(VALUES ('" + "'), ('".join(map(str, fips)) + "')) AS x(fips)"  # literal for postgres table

    sdoh_tables = sdoh_variables.loc[sdoh_vars, "source_id"].unique()

    levels = sdoh_databases.loc[sdoh_tables, "granularity"]

    sql_str = "SELECT " + ", ".join(cols) + f" FROM {fips_table}"
    for t in sdoh_tables:
        if fips_type == levels[t]:
            cast_str = "CAST(x.fips AS bigint)"
        else:
            n = {"bg": 12, "tract": 11, "county": 5}
            diff = n[levels[t]] - n[fips_type]
            cast_str = f"CAST(LEFT(x.fips, {diff}) AS bigint)"

        if levels[t] == "tract":
            sql_str += f" LEFT JOIN sdoh.{t} on {cast_str} = {t}.tractfips"
        elif levels[t] == "county":
            sql_str += f" LEFT JOIN sdoh.{t} on {cast_str} = {t}.countyfips"
        elif levels[t] == "bg":
            sql_str += f" LEFT JOIN sdoh.{t} on {cast_str} = {t}.bgfips"

    with psycopg2.connect(f"host={DBHOST} dbname={DBNAME} user={DBUSER} password={DBPASS}") as conn:
        with conn.cursor() as cur:
            cur.execute(sql_str)

            res = cur.fetchall()
            df = pd.DataFrame(res, columns=cols)
            return df

def save_tmp(df):
    token = secrets.token_urlsafe(16)
    df.to_csv("temp/" + token + ".csv")
    return token


def delete_job(job_id):
    # terminate_job(job_id)
    with psycopg2.connect(f"host={DBHOST} dbname={DBNAME} user={DBUSER} password={DBPASS}") as conn:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {TABLENAME} WHERE job = %s", (job_id,))
            cur.execute(f"DELETE FROM jobs WHERE id = %s", (job_id,))


def clean_address(s):  # remove invalid characters and leading apt/unit
    out = s.replace("*", "").replace('"', "").replace('(', "").replace(")", "").replace("+", "").replace("?", "")
    # TODO: just remove all non alphanumeric/commas/dashes
    # only leading ones cause problems if it seems like it could be regex

    regex = r"(apt|#|po box|p o box|p.o. box|unit|lot)\s?[a-z]?[0-9].*?\s"
    out = re.sub(regex, '', out, flags=re.IGNORECASE)

    return out


def reload_sdoh():
    with psycopg2.connect(f"host={DBHOST} dbname={DBNAME} user={DBUSER} password={DBPASS}") as conn:
        with conn.cursor() as cur:
            global sdoh_databases
            global sdoh_variables

            global SDOH_VARS  # need to deprecate

            cur.execute("SELECT id, name, version, url, description, granularity, census_year FROM sdoh.sdoh_source")
            sdoh_databases = pd.DataFrame(cur.fetchall(),
                                          columns=['id', 'name', 'version', 'url', 'description', 'granularity',
                                                   'census_year']).set_index("id")

            cur.execute("SELECT id, description, level, source, census_year, version FROM sdoh.sdoh")
            sdoh_variables = pd.DataFrame(cur.fetchall(),
                                          columns=['id', "description", "level", "source", "census_year",
                                                   "version"]).set_index('id')
            sdoh_variables['unique_id'] = sdoh_variables['source'] + "_" + sdoh_variables.index + "_" + sdoh_variables[
                'version'] + "_" + sdoh_variables['level']

            sdoh_variables["source_id"] = sdoh_variables["source"] + "_" + sdoh_variables["version"] + "_" + \
                                          sdoh_variables["level"]

            SDOH_VARS = sdoh_variables["description"].fillna("")

def sweep_jobs():
    # need to remove jobs that have been complete for more than 72 hours
    # clean all jobs after 1 month (even if incomplete)

    with psycopg2.connect(f"host={DBHOST} dbname={DBNAME} user={DBUSER} password={DBPASS}") as conn:
        with conn.cursor() as cur:

            cur.execute("SELECT id FROM jobs WHERE done = TRUE AND now() - endtime > interval '72 hours'")

            to_delete = list(map(lambda s: s[0], cur.fetchall()))
            completed_jobs = len(to_delete)

            cur.execute("SELECT id FROM jobs WHERE now() - starttime > interval '1 month' OR starttime IS NULL")

            old_ids = list(map(lambda s: s[0], cur.fetchall()))
            to_delete += old_ids
            old_jobs = len(old_ids)

            for i in to_delete:
                delete_job(i)

            cur.execute("DELETE FROM tokens where expiration < now()")

            token_count = cur.rowcount

            print(f'auto: deleted {completed_jobs} completed jobs, {old_jobs} old jobs, and {token_count} expired tokens')


def setup():
    with psycopg2.connect(f"host={DBHOST} dbname={DBNAME} user={DBUSER} password={DBPASS}") as conn:
        with conn.cursor() as cur:
            cur.execute("""CREATE OR REPLACE FUNCTION get_bg(geom geometry, out bg_id varchar) RETURNS varchar AS $$
                           SELECT bg_id FROM bg WHERE ST_Contains(bg.the_geom, geom); $$ LANGUAGE sql;""")

            cur.execute(f"""CREATE OR REPLACE FUNCTION geocode_multi(jobid integer, n integer, p integer) RETURNS void AS $$
                           BEGIN
                               UPDATE {TABLENAME}
                               SET (rating, new_address, tract, geom, long, lat, norm_addr, new_stno, new_st, new_st_type, new_city, new_state, new_zip) = (COALESCE(g.rating,-1), pprint_addy(g.addy), (get_tract(g.geomout, 'tract_id')), (g.geomout), ST_X(g.geomout)::numeric(8,5), ST_Y(g.geomout)::numeric(8,5), g.addy, (addy).address, (addy).streetname, (addy).streettypeabbrev, (addy).location, (addy).stateabbrev, (addy).zip)
                               FROM (SELECT address, job, id FROM {TABLENAME} WHERE job = jobid AND rating IS NULL AND partition = p ORDER BY id LIMIT n) AS a
                               LEFT JOIN LATERAL geocode(a.address,1) As g ON true
                               WHERE a.id = {TABLENAME}.id AND a.job = {TABLENAME}.job;
                           RETURN;
                           END; $$ LANGUAGE plpgsql;""")

reload_sdoh()
