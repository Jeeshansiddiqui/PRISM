"""
PRISM — Prosperity Risk Intelligence & Scoring Model
Step 2: PCA weighting → PRISM Score → K-Means clustering → write back to MySQL

Creates two new MySQL tables:
    fact_prism_scores   — every country-year with PRISM score + cluster
    most_improved       — top 10 countries by decade improvement
"""

import pandas as pd
import numpy as np
import mysql.connector
import warnings
warnings.filterwarnings("ignore")

from sklearn.preprocessing import MinMaxScaler
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
from sklearn.impute import SimpleImputer

# ── same credentials as fetch_data.py ─────────────────────────────────────────
DB_CONFIG = {
    "host":     "localhost",
    "port":     3306,
    "user":     "root",
    "password": "yourpassword",
    "database": "prism_db",
}

FEATURES = [
    "gdp_per_capita_usd", "gdp_growth_pct", "life_expectancy",
    "school_enrollment_pct", "control_of_corruption", "trade_pct_gdp",
]

CLUSTER_NAMES = {
    0: "Fragile States",
    1: "Developing Nations",
    2: "Transition Economies",
    3: "Emerging Economies",
    4: "High-Income Stable",
}


def get_conn():
    return mysql.connector.connect(**DB_CONFIG)


# ─── CREATE OUTPUT TABLES ─────────────────────────────────────────────────────
def create_output_tables():
    conn = get_conn(); cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS fact_prism_scores (
            id                    INT AUTO_INCREMENT PRIMARY KEY,
            country_code          VARCHAR(10),
            country_name          VARCHAR(150),
            region                VARCHAR(100),
            income_level          VARCHAR(80),
            year                  INT,
            gdp_per_capita_usd    DOUBLE,
            gdp_growth_pct        DOUBLE,
            life_expectancy       DOUBLE,
            school_enrollment_pct DOUBLE,
            control_of_corruption DOUBLE,
            trade_pct_gdp         DOUBLE,
            prism_score           DOUBLE,
            cluster_id            INT,
            cluster_name          VARCHAR(60),
            UNIQUE KEY uq_score (country_code, year)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS most_improved (
            id            INT AUTO_INCREMENT PRIMARY KEY,
            country_code  VARCHAR(10),
            country_name  VARCHAR(150),
            score_start   DOUBLE,
            score_end     DOUBLE,
            improvement   DOUBLE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

    conn.commit(); cur.close(); conn.close()
    print("  Output tables ready: fact_prism_scores, most_improved")


# ─── LOAD RAW DATA ────────────────────────────────────────────────────────────
def load_data():
    conn = get_conn()
    df   = pd.read_sql("""
        SELECT f.*, d.region, d.income_level
        FROM fact_economic f
        LEFT JOIN dim_country d ON f.country_code = d.code
    """, conn)
    conn.close()
    print(f"  Loaded {len(df)} rows, {df['country_code'].nunique()} countries")
    return df


# ─── CLEAN ────────────────────────────────────────────────────────────────────
def clean(df):
    df = df.dropna(subset=FEATURES, how="all").copy()
    for col in FEATURES:
        df[col] = df.groupby("country_code")[col].transform(
            lambda x: x.fillna(x.median())
        )
    imputer = SimpleImputer(strategy="median")
    df[FEATURES] = imputer.fit_transform(df[FEATURES])
    print(f"  After cleaning: {len(df)} rows")
    return df


# ─── PCA WEIGHTS ──────────────────────────────────────────────────────────────
def pca_weights(df):
    scaler   = MinMaxScaler()
    X_scaled = scaler.fit_transform(df[FEATURES])
    pca      = PCA(n_components=len(FEATURES))
    pca.fit(X_scaled)
    loadings     = np.abs(pca.components_)
    ev           = pca.explained_variance_ratio_
    raw          = loadings.T @ ev
    weights      = raw / raw.sum()
    print("  PCA weights:")
    for f, w in zip(FEATURES, weights):
        print(f"    {f:<30} {w:.4f}")
    return weights, scaler


# ─── PRISM SCORE ──────────────────────────────────────────────────────────────
def compute_score(df, weights, scaler):
    X = pd.DataFrame(scaler.transform(df[FEATURES]), columns=FEATURES, index=df.index)
    df = df.copy()
    df["prism_score"] = (sum(X[f] * weights[i] for i, f in enumerate(FEATURES)) * 100).round(2)
    return df


# ─── K-MEANS ──────────────────────────────────────────────────────────────────
def cluster(df):
    latest = df[df["year"] == df["year"].max()].copy()
    X = latest[FEATURES + ["prism_score"]].values
    km = KMeans(n_clusters=5, random_state=42, n_init=10)
    latest["cluster_id"] = km.fit_predict(X)

    order = (latest.groupby("cluster_id")["prism_score"]
             .mean().sort_values().index.tolist())
    remap = {old: new for new, old in enumerate(order)}
    latest["cluster_id"]   = latest["cluster_id"].map(remap)
    latest["cluster_name"] = latest["cluster_id"].map(CLUSTER_NAMES)

    cmap = latest.set_index("country_code")[["cluster_id", "cluster_name"]].to_dict("index")
    df = df.copy()
    df["cluster_id"]   = df["country_code"].map(lambda c: cmap.get(c, {}).get("cluster_id"))
    df["cluster_name"] = df["country_code"].map(lambda c: cmap.get(c, {}).get("cluster_name"))
    return df


# ─── MOST IMPROVED ────────────────────────────────────────────────────────────
def most_improved(df, n=10):
    pivot  = df.groupby(["country_code", "country_name", "year"])["prism_score"].mean().reset_index()
    y_min, y_max = pivot["year"].min(), pivot["year"].max()
    start  = pivot[pivot["year"] == y_min][["country_code","country_name","prism_score"]].rename(columns={"prism_score":"score_start"})
    end    = pivot[pivot["year"] == y_max][["country_code","prism_score"]].rename(columns={"prism_score":"score_end"})
    delta  = start.merge(end, on="country_code")
    delta["improvement"] = (delta["score_end"] - delta["score_start"]).round(2)
    top = delta.nlargest(n, "improvement")
    print(f"\n  Top {n} Most Improved ({y_min}→{y_max}):")
    print(top[["country_name","score_start","score_end","improvement"]].to_string(index=False))
    return top


# ─── WRITE BACK TO MYSQL ──────────────────────────────────────────────────────
def write_scores(df):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("TRUNCATE TABLE fact_prism_scores;")

    sql = """
        INSERT INTO fact_prism_scores
            (country_code, country_name, region, income_level, year,
             gdp_per_capita_usd, gdp_growth_pct, life_expectancy,
             school_enrollment_pct, control_of_corruption, trade_pct_gdp,
             prism_score, cluster_id, cluster_name)
        VALUES (%s,%s,%s,%s,%s, %s,%s,%s, %s,%s,%s, %s,%s,%s)
    """
    def v(x): return None if pd.isna(x) else float(x)

    rows = [
        (r.country_code, r.country_name, r.region, r.income_level, int(r.year),
         v(r.gdp_per_capita_usd), v(r.gdp_growth_pct), v(r.life_expectancy),
         v(r.school_enrollment_pct), v(r.control_of_corruption), v(r.trade_pct_gdp),
         v(r.prism_score), int(r.cluster_id) if not pd.isna(r.cluster_id) else None, r.cluster_name)
        for r in df.itertuples(index=False)
    ]
    cur.executemany(sql, rows)
    conn.commit(); cur.close(); conn.close()
    print(f"  fact_prism_scores: {len(rows)} rows written")


def write_most_improved(top):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("TRUNCATE TABLE most_improved;")
    sql = ("INSERT INTO most_improved (country_code, country_name, score_start, score_end, improvement) "
           "VALUES (%s,%s,%s,%s,%s)")
    rows = [(r.country_code, r.country_name, float(r.score_start), float(r.score_end), float(r.improvement))
            for r in top.itertuples(index=False)]
    cur.executemany(sql, rows)
    conn.commit(); cur.close(); conn.close()
    print(f"  most_improved: {len(rows)} rows written")


# ─── MAIN ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n[1/6] Creating output tables …")
    create_output_tables()

    print("\n[2/6] Loading data from MySQL …")
    df = load_data()

    print("\n[3/6] Cleaning & imputing …")
    df = clean(df)

    print("\n[4/6] PCA-informed weighting …")
    weights, scaler = pca_weights(df)

    print("\n[5/6] Computing PRISM Score & clustering …")
    df = compute_score(df, weights, scaler)
    df = cluster(df)
    top = most_improved(df)

    print("\n[6/6] Writing results back to MySQL …")
    write_scores(df)
    write_most_improved(top)

    # Also save CSV for Power BI CSV option
    import os; os.makedirs("data", exist_ok=True)
    df.to_csv("data/prism_scores.csv", index=False)
    top.to_csv("data/most_improved.csv", index=False)
    print("  CSVs saved to data/")

    print("\n✅ PRISM scoring complete. MySQL is updated.")
