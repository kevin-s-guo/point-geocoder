import datetime
import hashlib
import numpy as np
import os
import pandas as pd
import psycopg2
import re
import secrets
from joblib import Parallel, delayed
from sqlalchemy import create_engine

DBHOST = "db" #db
DBNAME = "census"
DBUSER = "census" #census
DBPASS = "census" # census
DBYEAR = "2020"  # will require column called "bg_<DBYEAR> in address table"

OTHER_YEAR_BG = {"2010": {"table": "bg_19", "col": "bg_2010"}}  # column for blockgroup in master_address_table

SDOH_TABLE = ""#"sdoh.ahrq_2020_tract"
SDOH_VARS = ""#pd.read_csv("data/ahrq_2020_desc.csv").set_index("variable")['description']

tablename = "master_address_table"

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
    if sdoh_vars:
        sdoh_vars_sql = ", " + f", {SDOH_TABLE}.".join(sdoh_vars)
    else:
        sdoh_vars_sql = ""

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
    inp.to_sql(tablename, engine, if_exists="append")

    engine.dispose()


def resolve_batch_partition(job, partition, sdoh_vars=[]):
    batch_size = 5
    with psycopg2.connect(f"host={DBHOST} dbname={DBNAME} user={DBUSER} password={DBPASS}") as conn:
        with conn.cursor() as cur:
            # determine how many addresses in partition
            cur.execute(
                f"SELECT count(*) from {tablename} where job = %s AND partition = {partition} AND rating IS NULL",
                (job,))
            num_addr = cur.fetchone()[0]

            print('job', job, 'partition', partition, 'samples', num_addr)

            # insert job entry
            if num_addr > 0:
                for i in range(-(num_addr // -batch_size)):
                    cur.execute(f"SELECT geocode_multi(%s, {batch_size}, {partition});", (job,))
                    conn.commit()

            cur.execute(f"SELECT partitions FROM jobs WHERE id = %s", (job,))
            done_array = cur.fetchone()[0]

            print('job', job, "partition", partition, "done")
            done_array[partition] = True

            if all(done_array):
                print('job', job, 'done')
                fips_year(job, DBYEAR)
                fips_year(job, "2010")

                stop = datetime.datetime.now()
                cur.execute(f"UPDATE jobs SET (endtime, done) = (%s, %s) WHERE id = %s", (stop, True, job))
            else:
                cur.execute(f"UPDATE jobs SET partitions = %s WHERE id = %s", (done_array, job))


def submit_partitions(job, partitions=5, sdoh_vars=[]):
    def t(i):
        resolve_batch_partition(job, i, sdoh_vars)

    Parallel(n_jobs=-1, backend="threading")(map(delayed(t), [i for i in range(partitions)]))


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
                print(sdoh_tables, sdoh_columns)
                sdoh_vars_sql = ", " + f", {SDOH_TABLE}.".join(sdoh_vars)
            else:
                sdoh_vars_sql = ""

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
    with psycopg2.connect(f"host={DBHOST} dbname={DBNAME} user={DBUSER} password={DBPASS}") as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT starttime, endtime, done FROM jobs WHERE id = %s", (job,))
            res = cur.fetchone()
            if res:
                starttime, endtime, done = res
            else:
                return None

            cur.execute(f"SELECT count(*) FROM {tablename} WHERE job = %s", (job,))
            total = cur.fetchone()[0]

            cur.execute(f"SELECT count(*) FROM {tablename} WHERE job = %s AND rating >= 0 AND rating < 25", (job,))
            success = cur.fetchone()[0]

            cur.execute(f"SELECT count(*) FROM {tablename} WHERE job = %s AND (rating = -1 OR rating >= 25)", (job,))
            fail = cur.fetchone()[0]

            cur.execute(f"SELECT count(*) FROM {tablename} WHERE job = %s AND rating IS NOT NULL", (job,))
            complete = cur.fetchone()[0]

    return done, starttime, endtime, total, success, fail, complete


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

    dx = sdoh_variables.loc[sdoh_vars]
    sdoh_tables = sdoh_variables.loc[sdoh_vars, "source_id"].unique()

    census_years = sdoh_databases.loc[sdoh_tables, "census_year"]
    levels = sdoh_databases.loc[sdoh_tables, "granularity"]

    sdoh_columns = (dx["source"] + "_" + dx["version"] + "_" + dx["level"] + "." + sdoh_vars).values
    sdoh_vars_sql = ", " + f", {SDOH_TABLE}.".join(sdoh_vars)

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


def fips_year(job, year="2010", skip=False):  # return block group fips
    # it's faster to do this in a big batch with a lateral join than it is to do concurrently while the job is running

    if year not in OTHER_YEAR_BG and year != DBYEAR:
        return None

    # extract job/id/long/lat from main db
    with psycopg2.connect(f"host={DBHOST} dbname={DBNAME} user={DBUSER} password={DBPASS}") as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT job, id, long, lat FROM master_address_table WHERE job = %s AND tract_" + str(
                year) + " IS NULL AND long IS NOT NULL", (job,))

            res = cur.fetchall()
            if res:
                df = pd.DataFrame(res, columns=["job", "id", "long", "lat"])
            else:  # already has 2010 tracts/bgs
                skip = True

            if not skip:
                if year == DBYEAR:
                    sql = """SELECT a.id, tractce, bg_id FROM bg
                             LEFT JOIN LATERAL (SELECT id, long, lat FROM master_address_table WHERE job = %s) AS a ON true
                             WHERE ST_Contains(the_geom, ST_SetSRID(ST_Point(a.long, a.lat), 4269))"""
                else:
                    sql = f"""SELECT a.id, tractce, bg_id FROM {OTHER_YEAR_BG[year]["table"]}
                       LEFT JOIN LATERAL (SELECT id, long, lat FROM master_address_table WHERE job = %s) AS a ON true
                       WHERE ST_Contains(geom, ST_SetSRID(ST_Point(a.long, a.lat), 4269))"""
                cur.execute(sql, (job,))

                res = cur.fetchall()
                df = pd.DataFrame(res, columns=["id", "tract", "block_group"]).set_index("id")

                for id, row in df.iterrows():  # TODO: use a join for efficiency (or at least execute batch)
                    cur.execute("UPDATE master_address_table SET tract_" + str(year) + " = %s, bg_" + str(
                        year) + " = %s WHERE job = %s AND id = %s", (row['tract'], row['block_group'], job, id))

            cur.execute("SELECT id, bg_" + str(
                year) + ", new_address, long, lat FROM master_address_table WHERE job = %s AND long IS NOT NULL",
                        (job,))
            res = cur.fetchall()
            df = pd.DataFrame(res, columns=["id", "bg_fips_" + str(year), "new_address", "long", "lat"])

            return df.set_index('id')


def save_tmp(df):
    token = secrets.token_urlsafe(16)
    df.to_csv("temp/" + token + ".csv")
    return token


def delete_job(job_id):
    with psycopg2.connect(f"host={DBHOST} dbname={DBNAME} user={DBUSER} password={DBPASS}") as conn:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {tablename} WHERE job = %s", (job_id,))
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
            reload_sdoh()
            cur.execute("""CREATE OR REPLACE FUNCTION get_bg(geom geometry, out bg_id varchar) RETURNS varchar AS $$
                           SELECT bg_id FROM bg WHERE ST_Contains(bg.the_geom, geom); $$ LANGUAGE sql;""")

            cur.execute(f"""CREATE OR REPLACE FUNCTION geocode_multi(jobid integer, n integer, p integer) RETURNS void AS $$
                           BEGIN
                               UPDATE {tablename}
                               SET (rating, new_address, tract, geom, long, lat, norm_addr, new_stno, new_st, new_st_type, new_city, new_state, new_zip) = (COALESCE(g.rating,-1), pprint_addy(g.addy), (get_tract(g.geomout, 'tract_id')), (g.geomout), ST_X(g.geomout)::numeric(8,5), ST_Y(g.geomout)::numeric(8,5), g.addy, (addy).address, (addy).streetname, (addy).streettypeabbrev, (addy).location, (addy).stateabbrev, (addy).zip)
                               FROM (SELECT address, job, id FROM {tablename} WHERE job = jobid AND rating IS NULL AND partition = p ORDER BY id LIMIT n) AS a
                               LEFT JOIN LATERAL geocode(a.address,1) As g ON true
                               WHERE a.id = {tablename}.id AND a.job = {tablename}.job;
                           RETURN;
                           END; $$ LANGUAGE plpgsql;""")


setup()
