import os
import pandas as pd
import datetime
import psycopg2
import json
import base64

from celery import Celery

from db_constants import DBHOST, DBNAME, DBUSER, DBPASS, DBYEAR, OTHER_YEAR_BG, TABLENAME


celery = Celery(__name__)
celery.conf.broker_url = os.environ.get("CELERY_BROKER_URL", "redis://redis:6379")
celery.conf.result_backend = os.environ.get("CELERY_RESULT_BACKEND", "redis://redis:6379")
celery.conf.worker_prefetch_multiplier = 0

@celery.task(name="resolve_batch_partition")
def resolve_batch_partition(job, partition, sdoh_vars=[]):
    batch_size = 5
    with psycopg2.connect(f"host={DBHOST} dbname={DBNAME} user={DBUSER} password={DBPASS}") as conn:
        with conn.cursor() as cur:
            # determine how many addresses in partition
            cur.execute(
                f"SELECT count(*) from {TABLENAME} where job = %s AND partition = {partition} AND rating IS NULL",
                (job,))
            num_addr = cur.fetchone()[0]

            # print('job', job, 'partition', partition, 'samples', num_addr)

            # insert job entry
            if num_addr > 0:
                for i in range(-(num_addr // -batch_size)):
                    cur.execute(f"SELECT geocode_multi(%s, {batch_size}, {partition});", (job,))
                    conn.commit()

            cur.execute(f"SELECT partitions FROM jobs WHERE id = %s", (job,))
            done_array = cur.fetchone()[0]

            done_array[partition] = True

            if all(done_array):
                fips_year(job, DBYEAR)
                fips_year(job, "2010")

                cur.execute(f"UPDATE jobs SET (endtime, done, partitions) = (%s, %s, %s) WHERE id = %s",
                            (datetime.datetime.now(), True, done_array, job))
            else:
                cur.execute(f"UPDATE jobs SET partitions = %s WHERE id = %s", (done_array, job))
            return True

    return False


@celery.task(name="fips_year")
def fips_year(job, year="2010", skip=False, ret=True):  # return block group fips
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

            # cur.execute("SELECT id, bg_" + str(
            #     year) + ", new_address, long, lat FROM master_address_table WHERE job = %s AND long IS NOT NULL",
            #             (job,))
            # res = cur.fetchall()
            # df = pd.DataFrame(res, columns=["id", "bg_fips_" + str(year), "new_address", "long", "lat"])

            # return df.set_index('id')

@celery.task
def error_handler(request, exc, traceback):
    print('Task {0} raised exception: {1!r}\n{2!r}'.format(request.id, exc, traceback))
