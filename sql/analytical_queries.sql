-- ============================================================
-- PRISM — Prosperity Risk Intelligence & Scoring Model
-- Analytical SQL Queries  |  Database: prism_db  (MySQL)
-- ============================================================
-- Tables:
--   dim_country        — country dimension
--   dim_year           — year dimension
--   fact_economic      — raw World Bank indicators
--   fact_prism_scores  — scored + clustered data
--   most_improved      — top 10 most improved countries
-- ============================================================


-- ── Q1: Latest PRISM scores for all countries (global ranking) ───────────────
SELECT
    f.country_code,
    f.country_name,
    f.region,
    f.income_level,
    ROUND(f.prism_score, 2)                                      AS prism_score,
    f.cluster_name,
    RANK() OVER (ORDER BY f.prism_score DESC)                    AS global_rank
FROM fact_prism_scores f
WHERE f.year = (SELECT MAX(year) FROM fact_prism_scores)
ORDER BY global_rank;


-- ── Q2: Average PRISM score by region ────────────────────────────────────────
SELECT
    region,
    ROUND(AVG(prism_score), 2)        AS avg_prism_score,
    ROUND(MIN(prism_score), 2)        AS min_score,
    ROUND(MAX(prism_score), 2)        AS max_score,
    COUNT(DISTINCT country_code)      AS num_countries
FROM fact_prism_scores
WHERE year = (SELECT MAX(year) FROM fact_prism_scores)
GROUP BY region
ORDER BY avg_prism_score DESC;


-- ── Q3: Global average PRISM score trend (year over year) ────────────────────
SELECT
    year,
    ROUND(AVG(prism_score), 2)        AS global_avg_prism,
    COUNT(DISTINCT country_code)      AS countries_tracked
FROM fact_prism_scores
GROUP BY year
ORDER BY year;


-- ── Q4: Cluster summary — count, avg, min, max ───────────────────────────────
SELECT
    cluster_name,
    COUNT(DISTINCT country_code)      AS num_countries,
    ROUND(AVG(prism_score), 2)        AS avg_score,
    ROUND(MIN(prism_score), 2)        AS min_score,
    ROUND(MAX(prism_score), 2)        AS max_score
FROM fact_prism_scores
WHERE year = (SELECT MAX(year) FROM fact_prism_scores)
GROUP BY cluster_name
ORDER BY avg_score DESC;


-- ── Q5: Top 10 most improved economies ───────────────────────────────────────
SELECT
    country_name,
    ROUND(score_start, 2)             AS score_2013,
    ROUND(score_end,   2)             AS score_2022,
    ROUND(improvement, 2)             AS prism_improvement
FROM most_improved
ORDER BY prism_improvement DESC
LIMIT 10;


-- ── Q6: Countries in bottom quartile (fragile / lowest scores) ───────────────
SELECT
    country_name,
    ROUND(prism_score, 2)             AS prism_score,
    cluster_name,
    region
FROM fact_prism_scores
WHERE year = (SELECT MAX(year) FROM fact_prism_scores)
  AND prism_score <= (
        SELECT PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY prism_score)
        OVER ()
        FROM fact_prism_scores
        WHERE year = (SELECT MAX(year) FROM fact_prism_scores)
        LIMIT 1
      )
ORDER BY prism_score;


-- ── Q7: GDP per capita vs PRISM score (top 30 by GDP) ────────────────────────
SELECT
    country_name,
    ROUND(gdp_per_capita_usd, 0)      AS gdp_per_capita_usd,
    ROUND(prism_score, 2)             AS prism_score,
    cluster_name
FROM fact_prism_scores
WHERE year = (SELECT MAX(year) FROM fact_prism_scores)
  AND gdp_per_capita_usd IS NOT NULL
ORDER BY gdp_per_capita_usd DESC
LIMIT 30;


-- ── Q8: Data completeness — missing values per indicator per year ─────────────
SELECT
    year,
    COUNT(*)                                                              AS total_rows,
    SUM(gdp_per_capita_usd    IS NULL)                                    AS missing_gdp_pc,
    SUM(gdp_growth_pct        IS NULL)                                    AS missing_gdp_growth,
    SUM(life_expectancy       IS NULL)                                    AS missing_life_exp,
    SUM(school_enrollment_pct IS NULL)                                    AS missing_school,
    SUM(control_of_corruption IS NULL)                                    AS missing_corruption,
    SUM(trade_pct_gdp         IS NULL)                                    AS missing_trade
FROM fact_economic
GROUP BY year
ORDER BY year;
