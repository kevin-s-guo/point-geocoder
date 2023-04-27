import os
import pandas as pd
import psycopg2
import psycopg2.extras
import re
import urllib.request
import shutil

from lib import DBHOST, DBNAME, DBUSER, DBPASS

def load_sdoh_desc(desc_csv_fn, source, version, census_year, granularity, url, desc):  # version has to be a year
    # replace space with _
    df = pd.read_csv(desc_csv_fn)
    df["variable"] = df["variable"].map(lambda s: s.replace(" ", "_"))

    if granularity not in ["zip", "county", "tract", "blockgroup"]:
        return False, "Granularity must be one of 'zip', 'county', 'tract', or 'blockgroup'"

    if census_year not in [2010, 2020]:
        return False, 'Census boundary year must be 2010 or 2020'

    if "variable" not in df.columns or "description" not in df.columns:
        return False, "Variable description csv must have columns for 'variable' and 'description'"

    df = df.set_index("variable")['description'].fillna("")

    vars = list(df.index)

    for v in vars:
        if not re.match(r'^[a-zA-Z0-9-_]+$', v):
            return False, "Variables must have alphanumeric (with dash/underscore) names. Error: " + str(v)

    tablename = source + "_" + str(version) + "_" + granularity
    with psycopg2.connect(f"host={DBHOST} dbname={DBNAME} user={DBUSER} password={DBPASS}") as conn:
        with conn.cursor() as cur:
            # insert into sdoh_source:
            cur.execute(
                "INSERT INTO sdoh.sdoh_source (id, name, version, url, description, granularity, census_year) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (tablename, source, version, url, desc, granularity, census_year))

            # insert each variable
            for v in vars:
                cur.execute(
                    "INSERT INTO sdoh.sdoh (id, description, census_year, level, source, version) VALUES (%s, %s, %s, %s, %s, %s)",
                    (v, df[v], census_year, granularity, source, version))

    return True, f"{len(vars)} variables"


def load_sdoh_data(data_csv_fn, index_col, source, version, granularity):
    # confirm that columns are only alphanumeric
    df = pd.read_csv(data_csv_fn, dtype={index_col: int})
    df = df.rename(columns=lambda s: s.replace(" ", "_")).drop_duplicates().set_index(index_col, verify_integrity=True)

    for col in df:
        if not re.match(r'^[a-zA-Z0-9-_]+$', col):
            return False, "Columns must have alphanumeric (with dash/underscore) names. Error: " + str(col)

    vars = list(df.columns)

    if len(vars) > 1599:
        return False, "Postgres does not support tables with more than 1600 columns. Please split into multiple databases with 1599 or fewer variables."

    # determine which table to store into (based on granularity and year) based on sdoh table
    tablename = source + "_" + str(version) + "_" + granularity
    with psycopg2.connect(f"host={DBHOST} dbname={DBNAME} user={DBUSER} password={DBPASS}") as conn:
        with conn.cursor() as cur:
            # need to create a new table for each source because postgres only allows 1600 per table
            col_sql = ", ".join(list(map(lambda s: s + " varchar(255)", vars)))
            cur.execute(
                f"CREATE TABLE sdoh.{tablename} ({granularity.upper() + 'FIPS'} bigint PRIMARY KEY, {col_sql});")

            entities = [[e] + [s[v] for v in vars] for e, s in df.iterrows()]
            sql = "INSERT INTO sdoh." + tablename + " (" + granularity.upper() + "FIPS," + ",".join(vars)
            sql += ") VALUES (%s, " + ", ".join(["%s" for v in vars]) + ")"
            psycopg2.extras.execute_batch(cur, sql, entities)


def download_data(name=""):
    try:
        if not os.path.isdir("tmp"):
            os.mkdir("tmp")
        if not os.path.isdir("data"):
            os.mkdir("data")

        if name == "places":
            URL = "https://chronicdata.cdc.gov/api/views/cwsq-ngmh/rows.csv?accessType=DOWNLOAD"

            fn, _ = urllib.request.urlretrieve(URL, "tmp/data.csv")

            places_df = pd.read_csv("tmp/data.csv")
            places_df["variable"] = places_df["MeasureId"]
            places_df["description"] = places_df["Category"] + ": " + places_df["Measure"]
            places_df["TRACTFIPS"] = places_df["LocationID"]

            places_df[["variable", "description"]].drop_duplicates().to_csv("data/places_2022_desc.csv")

            pd.pivot(places_df[["variable", "Data_Value", "TRACTFIPS"]], index="TRACTFIPS", columns="variable",
                     values="Data_Value").to_csv("data/places_2022.csv")

            print(load_sdoh_desc("data/places_2022_desc.csv", source="places", version="2022", desc="PLACES: Local Data for Better Health",
                           census_year=2010, granularity="tract", url="https://chronicdata.cdc.gov/500-Cities-Places/PLACES-Local-Data-for-Better-Health-Census-Tract-D/cwsq-ngmh"))

            load_sdoh_data("data/places_2022.csv", index_col="TRACTFIPS", source="places", version="2022", granularity="tract")
        elif name == "ahrq":
            URL = "https://www.ahrq.gov/downloads/sdoh/sdoh_2020_tract_1_0.xlsx"

            fn, _ = urllib.request.urlretrieve(URL, "tmp/data.xlsx")
            var_df = pd.read_excel("tmp/data.xlsx")[['name', 'label']].rename(columns={'name': 'variable', 'label': 'description'}).set_index('variable')
            var_df = var_df.iloc[8:]

            data_df = pd.read_excel("tmp/data.xlsx", sheet_name=1).drop(columns=['YEAR', 'COUNTYFIPS', 'STATEFIPS', 'STATE', 'COUNTY', 'REGION', 'TERRITORY']).set_index('TRACTFIPS')

            var_df.to_csv('data/ahrq_2020_desc.csv')
            data_df.to_csv('data/ahrq_2020.csv')

            print(load_sdoh_desc("data/ahrq_2020_desc.csv", source="ahrq", version="2020",
                                 desc="Agency for Healthcare Research and Quality Social Determinants of Health Database",
                           census_year=2020, granularity="tract", url="https://www.ahrq.gov/sdoh/data-analytics/sdoh-data.html"))

            load_sdoh_data("data/ahrq_2020.csv", index_col="TRACTFIPS", source="ahrq", version="2020", granularity="tract")
        elif name == "svi":
            URL = "https://svi.cdc.gov/Documents/Data/2020_SVI_Data/CSV/SVI2020_US.csv"

            fn, _ = urllib.request.urlretrieve(URL, "tmp/data.csv")

            data_df = pd.read_csv("tmp/data.csv").drop(columns=["ST", "STATE", "ST_ABBR", "STCNTY", "COUNTY", "LOCATION"]).set_index('FIPS')
            var_df = pd.DataFrame(index=data_df.columns)

            # no offical descriptions in table form -- just leave blank
            var_df.index = var_df.index.rename('variable')
            var_df['description'] = ""

            var_df.to_csv('data/svi_2020_desc.csv')
            data_df.to_csv('data/svi_2020.csv')

            print(load_sdoh_desc("data/svi_2020_desc.csv", source="svi", version="2020", desc="CDC/ATSDR Social Vulnerability Index",
                           census_year=2020, granularity="tract", url="https://www.atsdr.cdc.gov/placeandhealth/svi/index.html"))
            load_sdoh_data("data/svi_2020.csv", index_col="FIPS", source="svi", version="2020", granularity="tract")
        elif name == "fea":
            URL = "https://www.ers.usda.gov/webdocs/DataFiles/80526/FoodEnvironmentAtlas.xls?v=7756.1"
            fn, _ = urllib.request.urlretrieve(URL, "tmp/data.xls")

            var_df = pd.read_excel("tmp/data.xls", sheet_name=1)[["Variable Name", "Variable Code"]].rename(columns={"Variable Name": "description", "Variable Code": "variable"}).set_index("variable")

            dfs = []
            for i in range(9):
                dfs.append(pd.read_excel("tmp/data.xls", sheet_name=i+4).drop(columns=["State", "County"]).set_index("FIPS"))

            data_df = pd.concat(dfs, axis=1, verify_integrity=True).dropna()

            var_df.to_csv('data/fea_2020_desc.csv')
            data_df.to_csv('data/fea_2020.csv')


            print(load_sdoh_desc("data/fea_2020_desc.csv", source="fea", version="2020", desc="Food Environment Atlas",
                           census_year=2010, granularity="county", url="https://www.ers.usda.gov/data-products/food-environment-atlas/"))
            load_sdoh_data("data/fea_2020.csv", index_col="FIPS", source="fea", version="2020", granularity="county")
        elif name == "cre":
            URL = "https://www2.census.gov/programs-surveys/demo/datasets/community-resilience/2019/CRE_19_Tract.csv"

            fn, _ = urllib.request.urlretrieve(URL, "tmp/data.csv")

            data_df = pd.read_csv("tmp/data.csv").drop(columns=["STATE", "COUNTY", "TRACT"]).drop(columns=["NAME"]).\
                set_index("GEO_ID").rename(index=lambda s: s.split("US")[1])

            var_df = pd.DataFrame(index=data_df.columns)

            # no offical descriptions in table form -- just leave blank
            var_df.index = var_df.index.rename('variable')
            var_df['description'] = ""

            var_df.to_csv('data/cre_2019_desc.csv')
            data_df.to_csv('data/cre_2019.csv')

            print(load_sdoh_desc("data/cre_2019_desc.csv", source="cre", version="2019",
                                 desc="Community Resilience Estimates",
                           census_year=2010, granularity="tract", url="https://www.census.gov/programs-surveys/community-resilience-estimates.html"))
            load_sdoh_data("data/cre_2019.csv", index_col="GEO_ID", source="cre", version="2019", granularity="tract")
        elif name == "ruca":
            URL = "https://www.ers.usda.gov/webdocs/DataFiles/53241/ruca2010revised.xlsx?v=7852.7"

            fn, _ = urllib.request.urlretrieve(URL, "tmp/data.xlsx")

            data_df = pd.read_excel("tmp/data.xlsx", skiprows=1)[["State-County-Tract FIPS Code (lookup by address at http://www.ffiec.gov/Geocode/)", "Primary RUCA Code 2010", "Secondary RUCA Code, 2010 (see errata)"]]
            data_df = data_df.rename(columns={"State-County-Tract FIPS Code (lookup by address at http://www.ffiec.gov/Geocode/)": "FIPS", "Primary RUCA Code 2010": "Primary_RUCA_Code_2010", "Secondary RUCA Code, 2010 (see errata)":"Secondary_RUCA_Code_2010"}).dropna().set_index("FIPS")

            var_df = pd.DataFrame(index=data_df.columns)
            # no offical descriptions in table form -- just leave blank
            var_df.index = var_df.index.rename('variable')
            var_df['description'] = ""

            var_df.to_csv('data/ruca_2019_desc.csv')
            data_df.to_csv('data/ruca_2019.csv')

            print(load_sdoh_desc("data/ruca_2019_desc.csv", source="ruca", version="2019", desc="Rural-Urban Commuting Area Codes",
                           census_year=2010, granularity="tract", url="https://www.ers.usda.gov/data-products/rural-urban-commuting-area-codes.aspx"))
            load_sdoh_data("data/ruca_2019.csv", index_col="FIPS", source="ruca", version="2019", granularity="tract")
        elif name == "hl":
            URL = "http://healthliteracymap.unc.edu/download/national_hl_scores.xlsx"

            fn, _ = urllib.request.urlretrieve(URL, "tmp/data.xlsx")

            data_df = pd.read_excel("tmp/data.xlsx").rename(columns={"Census block group ID": "ID"}).set_index("ID")

            var_df = pd.DataFrame(index=data_df.columns)
            var_df.index = var_df.index.rename('variable')
            var_df['description'] = ""

            var_df.to_csv('data/hl_2003_desc.csv')
            data_df.to_csv('data/hl_2003.csv')

            print(load_sdoh_desc("data/hl_2003_desc.csv", source="hl", version="2003", desc="National Health Literacy Map",
                           census_year=2010, granularity="blockgroup", url="http://healthliteracymap.unc.edu/#understanding_the_data"))
            load_sdoh_data("data/hl_2003.csv", index_col="ID", source="hl", version="2003", granularity="blockgroup")
        else:
            print(name, "is not a valid SDoH database that can be downloaded.")
    finally:
        shutil.rmtree("tmp")


download_data("ahrq")
download_data("places")
download_data("svi")
download_data('fea')
download_data('cre')
download_data('ruca')
download_data('hl')