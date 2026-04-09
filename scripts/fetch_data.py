"""
PRISM - Complete pipeline in one file.
Run this once. It fetches data, scores, clusters, saves everything to MySQL.
"""

import requests, time, warnings
import pandas as pd
import numpy as np
import mysql.connector
warnings.filterwarnings("ignore")

from sklearn.preprocessing import MinMaxScaler
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
from sklearn.impute import SimpleImputer

# ── CHANGE ONLY THIS ──────────────────────────────────────────────────────────
DB = dict(host="localhost", port=3306, user="root", password="root", database="prism_db")
# ─────────────────────────────────────────────────────────────────────────────

INDICATORS = {
    "gdp_per_capita_usd":    "NY.GDP.PCAP.CD",
    "gdp_growth_pct":        "NY.GDP.MKTP.KD.ZG",
    "life_expectancy":       "SP.DYN.LE00.IN",
    "school_enrollment_pct": "SE.PRM.NENR",
    "control_of_corruption": "CC.EST",
    "trade_pct_gdp":         "NE.TRD.GNFS.ZS",
}
FEATURES = list(INDICATORS.keys())
CLUSTER_NAMES = {0:"Fragile States", 1:"Developing Nations",
                 2:"Transition Economies", 3:"Emerging Economies", 4:"High-Income Stable"}


def setup_db():
    c = mysql.connector.connect(host=DB["host"], port=DB["port"],
                                user=DB["user"], password=DB["root"])
    cur = c.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {DB['database']}")
    c.commit(); cur.close(); c.close()

    conn = mysql.connector.connect(**DB)
    cur  = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS most_improved")
    cur.execute("DROP TABLE IF EXISTS fact_prism_scores")
    cur.execute("DROP TABLE IF EXISTS fact_economic")
    cur.execute("DROP TABLE IF EXISTS dim_year")
    cur.execute("DROP TABLE IF EXISTS dim_country")

    cur.execute("""CREATE TABLE dim_country (
        code VARCHAR(10) PRIMARY KEY, name VARCHAR(150),
        region VARCHAR(100), income_level VARCHAR(80)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")

    cur.execute("""CREATE TABLE dim_year (year INT PRIMARY KEY) ENGINE=InnoDB""")

    cur.execute("""CREATE TABLE fact_economic (
        id INT AUTO_INCREMENT PRIMARY KEY,
        country_code VARCHAR(10), country_name VARCHAR(150), year INT,
        gdp_per_capita_usd DOUBLE, gdp_growth_pct DOUBLE, life_expectancy DOUBLE,
        school_enrollment_pct DOUBLE, control_of_corruption DOUBLE, trade_pct_gdp DOUBLE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")

    cur.execute("""CREATE TABLE fact_prism_scores (
        id INT AUTO_INCREMENT PRIMARY KEY,
        country_code VARCHAR(10), country_name VARCHAR(150),
        region VARCHAR(100), income_level VARCHAR(80), year INT,
        gdp_per_capita_usd DOUBLE, gdp_growth_pct DOUBLE, life_expectancy DOUBLE,
        school_enrollment_pct DOUBLE, control_of_corruption DOUBLE, trade_pct_gdp DOUBLE,
        prism_score DOUBLE, cluster_id INT, cluster_name VARCHAR(60)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")

    cur.execute("""CREATE TABLE most_improved (
        id INT AUTO_INCREMENT PRIMARY KEY,
        country_code VARCHAR(10), country_name VARCHAR(150),
        score_start DOUBLE, score_end DOUBLE, improvement DOUBLE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")

    conn.commit(); cur.close(); conn.close()
    print("  Database & tables ready")


def fetch_countries():
    r = requests.get("https://api.worldbank.org/v2/country?format=json&per_page=300", timeout=30)
    return [{"code":c["id"],"name":c["name"],"region":c["region"]["value"],
             "income_level":c["incomeLevel"]["value"]}
            for c in r.json()[1] if c["region"]["id"] != "NA" and c["id"] != "WLD"]


def fetch_indicator(wb_code):
    url = f"https://api.worldbank.org/v2/country/all/indicator/{wb_code}?format=json&per_page=20000&date=2013:2022"
    r = requests.get(url, timeout=60); r.raise_for_status()
    p = r.json()
    if len(p) < 2 or p[1] is None: return pd.DataFrame()
    return pd.DataFrame([
        {"country_code": i["countryiso3code"], "country_name": i["country"]["value"],
         "year": int(i["date"]), "value": float(i["value"])}
        for i in p[1] if i["value"] is not None and i["countryiso3code"]
    ])


def build_dataset():
    frames = []
    for col, code in INDICATORS.items():
        print(f"    Fetching {col} ...")
        df = fetch_indicator(code)
        if not df.empty:
            frames.append(df.rename(columns={"value":col})[["country_code","country_name","year",col]])
        time.sleep(0.3)
    merged = frames[0]
    for f in frames[1:]:
        merged = pd.merge(merged, f[["country_code","year",f.columns[-1]]],
                          on=["country_code","year"], how="outer")
    for col in FEATURES:
        if col not in merged.columns:
            merged[col] = None
            print(f"    ⚠ {col} not available from API — set to NULL")
    return merged.sort_values(["country_code","year"]).reset_index(drop=True)


def insert_raw(countries, df):
    conn = mysql.connector.connect(**DB); cur = conn.cursor()
    cur.executemany("INSERT IGNORE INTO dim_country VALUES (%s,%s,%s,%s)",
                    [(c["code"],c["name"],c["region"],c["income_level"]) for c in countries])
    cur.executemany("INSERT IGNORE INTO dim_year VALUES (%s)", [(y,) for y in range(2013,2023)])

    def v(x):
        try: return None if pd.isna(x) else float(x)
        except: return None

    rows = [(r.country_code, r.country_name, int(r.year),
             v(getattr(r,"gdp_per_capita_usd",None)), v(getattr(r,"gdp_growth_pct",None)),
             v(getattr(r,"life_expectancy",None)),    v(getattr(r,"school_enrollment_pct",None)),
             v(getattr(r,"control_of_corruption",None)), v(getattr(r,"trade_pct_gdp",None)))
            for r in df.itertuples(index=False)]

    cur.executemany("""INSERT INTO fact_economic
        (country_code,country_name,year,gdp_per_capita_usd,gdp_growth_pct,
         life_expectancy,school_enrollment_pct,control_of_corruption,trade_pct_gdp)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""", rows)

    conn.commit(); cur.close(); conn.close()
    print(f"  fact_economic: {len(rows)} rows inserted")


def score_and_cluster(df, countries):
    dim = pd.DataFrame(countries)
    df  = df.merge(dim[["code","region","income_level"]], left_on="country_code",
                   right_on="code", how="left").drop(columns="code")

    available = [f for f in FEATURES if f in df.columns and df[f].notna().sum() > 0]
    df = df.dropna(subset=available, how="all").copy()
    for col in available:
        df[col] = df.groupby("country_code")[col].transform(lambda x: x.fillna(x.median()))
    df[available] = SimpleImputer(strategy="median").fit_transform(df[available])

    scaler   = MinMaxScaler()
    X_scaled = scaler.fit_transform(df[available])
    pca      = PCA(n_components=len(available))
    pca.fit(X_scaled)
    raw     = np.abs(pca.components_).T @ pca.explained_variance_ratio_
    weights = raw / raw.sum()
    print("  PCA weights:")
    for f,w in zip(available, weights): print(f"    {f:<30} {w:.4f}")

    X2 = pd.DataFrame(scaler.transform(df[available]), columns=available, index=df.index)
    df["prism_score"] = (sum(X2[f]*weights[i] for i,f in enumerate(available))*100).round(2)

    latest = df[df["year"]==df["year"].max()].copy()
    km = KMeans(n_clusters=5, random_state=42, n_init=10)
    latest["cluster_id"] = km.fit_predict(latest[available+["prism_score"]].values)
    order = latest.groupby("cluster_id")["prism_score"].mean().sort_values().index.tolist()
    remap = {o:n for n,o in enumerate(order)}
    latest["cluster_id"]   = latest["cluster_id"].map(remap)
    latest["cluster_name"] = latest["cluster_id"].map(CLUSTER_NAMES)
    cmap = latest.set_index("country_code")[["cluster_id","cluster_name"]].to_dict("index")
    df["cluster_id"]   = df["country_code"].map(lambda c: cmap.get(c,{}).get("cluster_id"))
    df["cluster_name"] = df["country_code"].map(lambda c: cmap.get(c,{}).get("cluster_name"))
    return df


def insert_scores(df):
    conn = mysql.connector.connect(**DB); cur = conn.cursor()

    def v(x):
        try: return None if pd.isna(x) else float(x)
        except: return None

    rows = [(r.country_code, r.country_name,
             getattr(r,"region",None), getattr(r,"income_level",None), int(r.year),
             v(getattr(r,"gdp_per_capita_usd",None)), v(getattr(r,"gdp_growth_pct",None)),
             v(getattr(r,"life_expectancy",None)),    v(getattr(r,"school_enrollment_pct",None)),
             v(getattr(r,"control_of_corruption",None)), v(getattr(r,"trade_pct_gdp",None)),
             v(r.prism_score),
             int(r.cluster_id) if r.cluster_id is not None and not pd.isna(r.cluster_id) else None,
             r.cluster_name)
            for r in df.itertuples(index=False)]

    cur.executemany("""INSERT INTO fact_prism_scores
        (country_code,country_name,region,income_level,year,
         gdp_per_capita_usd,gdp_growth_pct,life_expectancy,
         school_enrollment_pct,control_of_corruption,trade_pct_gdp,
         prism_score,cluster_id,cluster_name)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", rows)

    pivot = df.groupby(["country_code","country_name","year"])["prism_score"].mean().reset_index()
    start = pivot[pivot["year"]==pivot["year"].min()][["country_code","country_name","prism_score"]].rename(columns={"prism_score":"s"})
    end   = pivot[pivot["year"]==pivot["year"].max()][["country_code","prism_score"]].rename(columns={"prism_score":"e"})
    delta = start.merge(end, on="country_code").nlargest(10, "e")
    delta["imp"] = (delta["e"] - delta["s"]).round(2)
    cur.executemany("INSERT INTO most_improved (country_code,country_name,score_start,score_end,improvement) VALUES (%s,%s,%s,%s,%s)",
                    [(r.country_code,r.country_name,float(r.s),float(r.e),float(r.imp)) for r in delta.itertuples()])

    conn.commit(); cur.close(); conn.close()
    print(f"  fact_prism_scores: {len(rows)} rows")
    print(f"  most_improved: {len(delta)} rows")
    return df


if __name__ == "__main__":
    print("\n[1/5] Setting up MySQL ...")
    setup_db()

    print("\n[2/5] Fetching countries ...")
    countries = fetch_countries()
    print(f"  {len(countries)} countries")

    print("\n[3/5] Fetching World Bank indicators ...")
    import os; os.makedirs("data", exist_ok=True)
    df = build_dataset()
    df.to_csv("data/raw_economic_data.csv", index=False)
    print(f"  {len(df)} rows fetched")

    print("\n[4/5] Inserting raw data into MySQL ...")
    insert_raw(countries, df)

    print("\n[5/5] Scoring, clustering, saving results ...")
    df = score_and_cluster(df, countries)
    df = insert_scores(df)
    df.to_csv("data/prism_scores.csv", index=False)

    print("\n✅ ALL DONE. MySQL is ready. Now run: python scripts/eda_visualize.py")
