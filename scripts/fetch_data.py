"""
PRISM — Prosperity Risk Intelligence & Scoring Model
Step 1: Fetch World Bank data → store in MySQL (star schema)

Requirements:
    pip install requests pandas mysql-connector-python
    MySQL server must be running locally or on a server.
    Update DB_CONFIG below before running.
"""

import requests
import pandas as pd
import mysql.connector
import os
import time

# ─── UPDATE THESE CREDENTIALS ─────────────────────────────────────────────────
DB_CONFIG = {
    "host":     "localhost",
    "port":     3306,
    "user":     "root",           # your MySQL username
    "password": "yourpassword",   # your MySQL password
    "database": "prism_db",
}

YEARS    = list(range(2013, 2023))
BASE_URL = "https://api.worldbank.org/v2"

INDICATORS = {
    "gdp_per_capita_usd":     "NY.GDP.PCAP.CD",
    "gdp_growth_pct":         "NY.GDP.MKTP.KD.ZG",
    "life_expectancy":        "SP.DYN.LE00.IN",
    "school_enrollment_pct":  "SE.PRM.NENR",
    "control_of_corruption":  "CC.EST",
    "trade_pct_gdp":          "NE.TRD.GNFS.ZS",
}


# ─── DATABASE SETUP ───────────────────────────────────────────────────────────
def get_conn(with_db=True):
    cfg = DB_CONFIG.copy()
    if not with_db:
        cfg.pop("database")
    return mysql.connector.connect(**cfg)


def create_database():
    conn = get_conn(with_db=False)
    cur  = conn.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']} "
                f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;")
    print(f"  Database '{DB_CONFIG['database']}' ready.")
    cur.close(); conn.close()


def create_tables():
    conn = get_conn()
    cur  = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_country (
            code         VARCHAR(10)  NOT NULL PRIMARY KEY,
            name         VARCHAR(150) NOT NULL,
            region       VARCHAR(100),
            income_level VARCHAR(80)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_year (
            year INT NOT NULL PRIMARY KEY
        ) ENGINE=InnoDB;
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS fact_economic (
            id                    INT AUTO_INCREMENT PRIMARY KEY,
            country_code          VARCHAR(10),
            country_name          VARCHAR(150),
            year                  INT,
            gdp_per_capita_usd    DOUBLE,
            gdp_growth_pct        DOUBLE,
            life_expectancy       DOUBLE,
            school_enrollment_pct DOUBLE,
            control_of_corruption DOUBLE,
            trade_pct_gdp         DOUBLE,
            FOREIGN KEY (country_code) REFERENCES dim_country(code),
            FOREIGN KEY (year)         REFERENCES dim_year(year),
            UNIQUE KEY uq_country_year (country_code, year)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

    conn.commit(); cur.close(); conn.close()
    print("  Tables created: dim_country, dim_year, fact_economic")


# ─── WORLD BANK API ───────────────────────────────────────────────────────────
def get_countries():
    r = requests.get(f"{BASE_URL}/country?format=json&per_page=300", timeout=30)
    r.raise_for_status()
    countries = [
        {"code": c["id"], "name": c["name"],
         "region": c["region"]["value"], "income_level": c["incomeLevel"]["value"]}
        for c in r.json()[1]
        if c["region"]["id"] != "NA" and c["id"] != "WLD"
    ]
    print(f"  {len(countries)} countries fetched")
    return countries


def fetch_indicator(wb_code):
    url = (f"{BASE_URL}/country/all/indicator/{wb_code}"
           f"?format=json&per_page=20000&date=2013:2022")
    r = requests.get(url, timeout=60); r.raise_for_status()
    payload = r.json()
    if len(payload) < 2 or payload[1] is None:
        return pd.DataFrame()
    records = [
        {"country_code": i["countryiso3code"],
         "country_name": i["country"]["value"],
         "year":  int(i["date"]),
         "value": float(i["value"])}
        for i in payload[1]
        if i["value"] is not None and i["countryiso3code"]
    ]
    return pd.DataFrame(records)


def build_wide_table():
    frames = []
    for col, code in INDICATORS.items():
        print(f"    Fetching {col} ({code}) …")
        df = fetch_indicator(code)
        if df.empty:
            print(f"    ⚠ No data for {col}")
            continue
        frames.append(df.rename(columns={"value": col})
                        [["country_code", "country_name", "year", col]])
        time.sleep(0.3)

    merged = frames[0]
    for f in frames[1:]:
        merged = pd.merge(merged, f[["country_code", "year", f.columns[-1]]],
                          on=["country_code", "year"], how="outer")
    return merged.sort_values(["country_code", "year"]).reset_index(drop=True)


# ─── INSERT INTO MYSQL ────────────────────────────────────────────────────────
def insert_dim_country(countries):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SET FOREIGN_KEY_CHECKS=0;")
    cur.execute("TRUNCATE TABLE fact_economic;")
    cur.execute("DELETE FROM dim_country;")
    cur.execute("SET FOREIGN_KEY_CHECKS=1;")
    sql  = ("INSERT INTO dim_country (code, name, region, income_level) "
            "VALUES (%s, %s, %s, %s)")
    rows = [(c["code"], c["name"], c["region"], c["income_level"]) for c in countries]
    cur.executemany(sql, rows)
    conn.commit(); cur.close(); conn.close()
    print(f"  dim_country: {len(rows)} rows")


def insert_dim_year():
    conn = get_conn(); cur = conn.cursor()
    cur.execute("DELETE FROM dim_year;")
    cur.executemany("INSERT INTO dim_year (year) VALUES (%s)", [(y,) for y in YEARS])
    conn.commit(); cur.close(); conn.close()
    print(f"  dim_year: {len(YEARS)} rows")


def insert_fact(df):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("TRUNCATE TABLE fact_economic;")
    sql = """
        INSERT INTO fact_economic
            (country_code, country_name, year,
             gdp_per_capita_usd, gdp_growth_pct, life_expectancy,
             school_enrollment_pct, control_of_corruption, trade_pct_gdp)
        VALUES (%s,%s,%s, %s,%s,%s, %s,%s,%s)
        ON DUPLICATE KEY UPDATE
            gdp_per_capita_usd    = VALUES(gdp_per_capita_usd),
            gdp_growth_pct        = VALUES(gdp_growth_pct),
            life_expectancy       = VALUES(life_expectancy),
            school_enrollment_pct = VALUES(school_enrollment_pct),
            control_of_corruption = VALUES(control_of_corruption),
            trade_pct_gdp         = VALUES(trade_pct_gdp)
    """
    def v(x): return None if pd.isna(x) else float(x)
    rows = [
        (r.country_code, r.country_name, int(r.year),
         v(r.gdp_per_capita_usd), v(r.gdp_growth_pct), v(r.life_expectancy),
         v(r.school_enrollment_pct), v(r.control_of_corruption), v(r.trade_pct_gdp))
        for r in df.itertuples(index=False)
    ]
    cur.executemany(sql, rows)
    conn.commit(); cur.close(); conn.close()
    print(f"  fact_economic: {len(rows)} rows")


# ─── MAIN ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n[1/4] Setting up MySQL database & tables …")
    create_database()
    create_tables()

    print("\n[2/4] Fetching country list from World Bank …")
    countries = get_countries()

    print("\n[3/4] Fetching 6 economic indicators …")
    wide_df = build_wide_table()
    os.makedirs("data", exist_ok=True)
    wide_df.to_csv("data/raw_economic_data.csv", index=False)
    print(f"  Shape: {wide_df.shape} | CSV saved → data/raw_economic_data.csv")

    print("\n[4/4] Inserting into MySQL …")
    insert_dim_country(countries)
    insert_dim_year()
    insert_fact(wide_df)

    print("\n✅ Done. MySQL prism_db is populated and ready for Power BI.")
