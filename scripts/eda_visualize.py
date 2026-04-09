"""
PRISM — Prosperity Risk Intelligence & Scoring Model
Step 3: EDA & Visualizations — reads directly from MySQL
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import seaborn as sns
import mysql.connector
import numpy as np
import os

DB_CONFIG = {
    "host":     "localhost",
    "port":     3306,
    "user":     "root",
    "password": "yourpassword",
    "database": "prism_db",
}

OUT_DIR  = "outputs"
os.makedirs(OUT_DIR, exist_ok=True)
sns.set_theme(style="whitegrid", palette="muted")

FEATURES = [
    "gdp_per_capita_usd", "gdp_growth_pct", "life_expectancy",
    "school_enrollment_pct", "control_of_corruption", "trade_pct_gdp",
]
LABELS = {
    "gdp_per_capita_usd":    "GDP per Capita",
    "gdp_growth_pct":        "GDP Growth %",
    "life_expectancy":       "Life Expectancy",
    "school_enrollment_pct": "School Enrollment",
    "control_of_corruption": "Control of Corruption",
    "trade_pct_gdp":         "Trade % GDP",
}


def get_conn():
    return mysql.connector.connect(**DB_CONFIG)


def load():
    conn = get_conn()
    df  = pd.read_sql("SELECT * FROM fact_prism_scores", conn)
    top = pd.read_sql("SELECT * FROM most_improved ORDER BY improvement DESC LIMIT 10", conn)
    conn.close()
    return df, top


def save(fig, name):
    path = os.path.join(OUT_DIR, name)
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {path}")


def chart1_distribution(df):
    latest = df[df["year"] == df["year"].max()]
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.hist(latest["prism_score"].dropna(), bins=30, color="#4C72B0",
            edgecolor="white", alpha=0.85)
    med = latest["prism_score"].median()
    ax.axvline(med, color="crimson", linestyle="--", linewidth=1.8, label=f"Median: {med:.1f}")
    ax.set_title("PRISM Score Distribution — Latest Year", fontsize=14, fontweight="bold")
    ax.set_xlabel("PRISM Score (0–100)"); ax.set_ylabel("Countries")
    ax.legend(); save(fig, "chart1_distribution.png")


def chart2_cluster_bar(df):
    latest = df[df["year"] == df["year"].max()]
    order  = (latest.groupby("cluster_name")["prism_score"].mean()
              .sort_values(ascending=False).index)
    fig, ax = plt.subplots(figsize=(10, 5))
    sns.barplot(data=latest, x="cluster_name", y="prism_score",
                order=order, palette="Blues_d", ax=ax, errorbar=None)
    ax.set_title("Average PRISM Score by Cluster", fontsize=14, fontweight="bold")
    ax.set_xlabel("Cluster"); ax.set_ylabel("Avg PRISM Score")
    ax.set_xticklabels(ax.get_xticklabels(), rotation=15, ha="right")
    save(fig, "chart2_cluster_scores.png")


def chart3_most_improved(top):
    top = top.sort_values("improvement", ascending=True)
    fig, ax = plt.subplots(figsize=(10, 6))
    colors = sns.color_palette("RdYlGn", len(top))
    ax.barh(top["country_name"], top["improvement"], color=colors)
    ax.set_title("Top 10 Most Improved Economies (2013–2022)",
                 fontsize=14, fontweight="bold")
    ax.set_xlabel("PRISM Score Improvement")
    save(fig, "chart3_most_improved.png")


def chart4_global_trend(df):
    trend = df.groupby("year")["prism_score"].mean().reset_index()
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(trend["year"], trend["prism_score"], marker="o",
            color="#4C72B0", linewidth=2.5)
    ax.fill_between(trend["year"], trend["prism_score"], alpha=0.12, color="#4C72B0")
    ax.set_title("Global Average PRISM Score (2013–2022)",
                 fontsize=14, fontweight="bold")
    ax.set_xlabel("Year"); ax.set_ylabel("Avg PRISM Score")
    ax.xaxis.set_major_locator(mtick.MaxNLocator(integer=True))
    save(fig, "chart4_global_trend.png")


def chart5_heatmap(df):
    renamed = df[FEATURES + ["prism_score"]].rename(
        columns={**LABELS, "prism_score": "PRISM Score"}
    )
    corr = renamed.corr()
    mask = np.triu(np.ones_like(corr, dtype=bool), k=1)
    fig, ax = plt.subplots(figsize=(10, 8))
    sns.heatmap(corr, mask=mask, annot=True, fmt=".2f", cmap="coolwarm",
                vmin=-1, vmax=1, linewidths=0.5, ax=ax)
    ax.set_title("Feature Correlation Heatmap", fontsize=14, fontweight="bold")
    save(fig, "chart5_correlation.png")


def chart6_pie(df):
    latest = df[df["year"] == df["year"].max()]
    counts = latest["cluster_name"].value_counts()
    fig, ax = plt.subplots(figsize=(8, 8))
    ax.pie(counts, labels=counts.index, autopct="%1.1f%%",
           colors=sns.color_palette("Set2", len(counts)), startangle=140,
           wedgeprops=dict(edgecolor="white", linewidth=1.5))
    ax.set_title("Country Distribution by Cluster", fontsize=14, fontweight="bold")
    save(fig, "chart6_cluster_pie.png")


if __name__ == "__main__":
    print("\nLoading from MySQL …")
    df, top = load()
    print(f"  {len(df)} rows | {df['country_code'].nunique()} countries\n")

    print("Generating charts …")
    chart1_distribution(df)
    chart2_cluster_bar(df)
    chart3_most_improved(top)
    chart4_global_trend(df)
    chart5_heatmap(df)
    chart6_pie(df)
    print("\n✅ All 6 charts saved to outputs/")
