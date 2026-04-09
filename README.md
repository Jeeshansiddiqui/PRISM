# PRISM — Prosperity Risk Intelligence & Scoring Model

> **End-to-end global data analytics system analyzing 195 countries across six economic dimensions.**

---

## 📌 Overview

PRISM is a full-stack data analytics pipeline that:
- Pulls **10 years of real-world economic data** (2013–2022) from the **World Bank REST API** for 195 countries
- Stores data in a **MySQL database** with a star schema and analytical SQL queries
- Computes a composite **PRISM Score (0–100)** using **PCA-informed feature weighting**
- Applies **K-Means clustering** to segment countries into 5 economic profiles
- Identifies the **most-improved economies** of the decade
- Visualizes insights with **Matplotlib & Seaborn charts**
- Delivers an interactive **Power BI dashboard** connected live to MySQL

---

## 🏗️ Architecture

```
World Bank REST API
(195 countries × 6 indicators × 10 years)
        │
        ▼
  fetch_data.py
  (requests + pandas)
        │
        ▼
  MySQL — prism_db          ← Star Schema
  ├── dim_country
  ├── dim_year
  └── fact_economic
        │
        ▼
  score_and_cluster.py
  (PCA weighting + PRISM Score + K-Means)
        │
        ▼
  MySQL — prism_db
  ├── fact_prism_scores
  └── most_improved
        │
   ┌────┴─────────────────┐
   ▼                       ▼
eda_visualize.py      Power BI Dashboard
(Matplotlib/Seaborn)  (live MySQL connection)
```

---

## 📊 Six Economic Dimensions

| Dimension | Indicator | World Bank Code |
|---|---|---|
| Economic Output | GDP per Capita (USD) | NY.GDP.PCAP.CD |
| Growth | GDP Growth Rate (%) | NY.GDP.MKTP.KD.ZG |
| Social Development | Life Expectancy (years) | SP.DYN.LE00.IN |
| Education | Primary School Enrollment (%) | SE.PRM.NENR |
| Governance | Control of Corruption | CC.EST |
| Trade / Openness | Trade as % of GDP | NE.TRD.GNFS.ZS |

---

## 🧮 PRISM Score Methodology

1. **Fetch** — World Bank API → raw data → MySQL `fact_economic`
2. **Clean** — Country-level median imputation, drop all-null rows
3. **Normalize** — MinMaxScaler to [0, 1]
4. **PCA Weighting** — Run PCA on all 6 features; weight = loading magnitude × explained variance ratio (normalized to sum = 1)
5. **Score** — Weighted sum × 100 = PRISM Score (0–100)
6. **Cluster** — K-Means (k=5), clusters ordered by mean PRISM score:

| Cluster | Profile |
|---|---|
| 🔴 Fragile States | Lowest scoring, high instability |
| 🟠 Developing Nations | Below-average across dimensions |
| 🟡 Transition Economies | Mid-range, improving trajectory |
| 🟢 Emerging Economies | Above-average, high growth |
| 🔵 High-Income Stable | Highest scoring, stable governance |

---

## 🚀 Quick Start

### Prerequisites
- Python 3.9+
- **MySQL Server** running locally → https://dev.mysql.com/downloads/mysql/

```bash
git clone https://github.com/YOUR_USERNAME/PRISM.git
cd PRISM
pip install -r requirements.txt
```

### Update DB credentials in all 3 scripts
```python
DB_CONFIG = {
    "host":     "localhost",
    "user":     "root",
    "password": "YOUR_PASSWORD",   # ← change this
    "database": "prism_db",
}
```

### Run the pipeline
```bash
# Step 1 — Fetch & load into MySQL
python scripts/fetch_data.py

# Step 2 — Score, cluster & write back to MySQL
python scripts/score_and_cluster.py

# Step 3 — Generate charts
python scripts/eda_visualize.py
```

### Power BI
Open Power BI Desktop → Get Data → MySQL database → `localhost` / `prism_db`
Full setup guide: `powerbi_notes/dashboard_setup.md`

---

## 📁 Project Structure

```
PRISM/
├── data/
│   ├── raw_economic_data.csv
│   ├── prism_scores.csv
│   └── most_improved.csv
├── sql/
│   └── analytical_queries.sql
├── scripts/
│   ├── fetch_data.py
│   ├── score_and_cluster.py
│   └── eda_visualize.py
├── powerbi_notes/
│   └── dashboard_setup.md
├── outputs/             ← generated charts saved here
├── requirements.txt
└── README.md
```

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Data Ingestion | Python · Requests · World Bank REST API |
| Database | **MySQL** (star schema, 5 tables) |
| Data Processing | Pandas · NumPy |
| ML / Statistics | scikit-learn (PCA · KMeans · MinMaxScaler) |
| Visualization | Matplotlib · Seaborn |
| BI Dashboard | **Power BI** (live MySQL · DirectQuery · DAX) |
| Version Control | Git / GitHub |

---

## 👤 Author

**Jeeshan Siddiqui** — Data Analyst  
jeeshansiddiqui4396@gmail.com | [LinkedIn](https://linkedin.com/in/jeeshan-siddiqui-439095263)
