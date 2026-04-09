# Power BI Dashboard — PRISM (MySQL Live Connection)

## Step 1: Install MySQL ODBC Driver (one-time setup)

1. Download **MySQL Connector/ODBC** from:  
   https://dev.mysql.com/downloads/connector/odbc/
2. Install it (64-bit version for Power BI Desktop)
3. Restart Power BI Desktop after installation

---

## Step 2: Connect Power BI to MySQL

1. Open **Power BI Desktop**
2. Click **Home → Get Data → More**
3. Search for **MySQL database** → Click **Connect**
4. Enter:
   - **Server:** `localhost` (or your MySQL server IP)
   - **Database:** `prism_db`
5. Click **OK** → Enter your MySQL username & password
6. In the Navigator, select these tables:
   - ✅ `fact_prism_scores`
   - ✅ `dim_country`
   - ✅ `most_improved`
7. Click **Load**

> 💡 **For real-time / live query mode:** Click **Transform Data → Advanced → DirectQuery** instead of Import. This means Power BI always queries MySQL live — charts refresh when data changes.

---

## Step 3: Data Model (Star Schema in Power BI)

Go to **Model view** and verify this relationship:
```
dim_country [code]  ──(1:Many)──►  fact_prism_scores [country_code]
```
If not auto-detected, drag `dim_country.code` onto `fact_prism_scores.country_code`.

---

## Step 4: Dashboard Pages & Visuals

### Page 1 — 🌍 Global Overview
| Visual | Type | Config |
|---|---|---|
| World Choropleth | Filled Map | Location = `country_name`, Values = `prism_score` (avg) |
| Global Avg KPI | Card | Measure → `Global Avg PRISM Score` |
| Countries Count | Card | Measure → `Countries Tracked` |
| Score Histogram | Clustered Bar | X = `prism_score` (binned), Y = count |
| Year Slicer | Slicer | Field = `year` |

### Page 2 — 🔵 Cluster Analysis
| Visual | Type | Config |
|---|---|---|
| Cluster Donut | Donut Chart | Legend = `cluster_name`, Values = count of `country_code` |
| Cluster Avg Bar | Bar Chart | Axis = `cluster_name`, Value = avg `prism_score` |
| GDP vs Score | Scatter Plot | X = `gdp_per_capita_usd`, Y = `prism_score`, Legend = `cluster_name` |
| Income Level Bar | Stacked Bar | Axis = `income_level`, Legend = `cluster_name` |

### Page 3 — 📈 Most Improved Economies
| Visual | Type | Config |
|---|---|---|
| Top 10 Horizontal Bar | Bar Chart | Axis = `country_name`, Value = `improvement` (sorted desc) |
| Before vs After | Grouped Bar | Values = `score_start` & `score_end`, Axis = `country_name` |
| Gain KPI | Card | MAX(`improvement`) |

### Page 4 — 📅 Time Trends
| Visual | Type | Config |
|---|---|---|
| Global Trend Line | Line Chart | X = `year`, Y = avg `prism_score` |
| Country Trend | Multi-line | X = `year`, Y = `prism_score`, Legend = `country_name` |
| Country Slicer | Slicer | Field = `country_name` (search enabled) |
| Region Slicer | Slicer | Field = `region` |

---

## Step 5: DAX Measures

Create these in the **fact_prism_scores** table:

```dax
Global Avg PRISM Score =
ROUND(AVERAGE(fact_prism_scores[prism_score]), 2)

Countries Tracked =
DISTINCTCOUNT(fact_prism_scores[country_code])

YoY Score Change =
VAR CY = MAX(fact_prism_scores[year])
VAR PY = CY - 1
VAR AvgCY = CALCULATE(AVERAGE(fact_prism_scores[prism_score]), fact_prism_scores[year] = CY)
VAR AvgPY = CALCULATE(AVERAGE(fact_prism_scores[prism_score]), fact_prism_scores[year] = PY)
RETURN ROUND(AvgCY - AvgPY, 2)

Top Country =
CALCULATE(
    FIRSTNONBLANK(fact_prism_scores[country_name], 1),
    TOPN(1, fact_prism_scores, fact_prism_scores[prism_score], DESC),
    fact_prism_scores[year] = MAX(fact_prism_scores[year])
)
```

---

## Step 6: Cluster Color Coding

In **Format → Data colors**, manually assign:
| Cluster | Color |
|---|---|
| High-Income Stable | `#1565C0` (Dark Blue) |
| Emerging Economies | `#00897B` (Teal) |
| Transition Economies | `#F9A825` (Amber) |
| Developing Nations | `#EF6C00` (Orange) |
| Fragile States | `#B71C1C` (Red) |

---

## Step 7: Enable Auto-Refresh (optional)

For scheduled refresh from MySQL:
- **File → Publish to Power BI Service**
- In Power BI Service → Dataset → **Schedule Refresh**
- Set frequency (daily / hourly)
- Install **On-premises data gateway** if MySQL is on your local machine

---

## Step 8: Save & Export

- Save as: `PRISM_Dashboard.pbix` in the `powerbi_notes/` folder
- Export snapshot: **File → Export → Export to PDF** → add to GitHub as `powerbi_notes/PRISM_Dashboard_preview.pdf`
