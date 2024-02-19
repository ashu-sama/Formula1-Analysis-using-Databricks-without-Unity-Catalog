-- Databricks notebook source
SELECT
  driver_name,
  COUNT(1) AS total_races,
  SUM(calculated_points) AS total_points,
  ROUND(AVG(calculated_points), 3) AS avg_points
FROM
  f1_presentation.calculated_race_results
GROUP BY
  driver_name
HAVING
  COUNT(1) >= 50
ORDER BY
  avg_points DESC

-- COMMAND ----------

SELECT
  driver_name,
  COUNT(1) AS total_races,
  SUM(calculated_points) AS total_points,
  ROUND(AVG(calculated_points), 3) AS avg_points
FROM
  f1_presentation.calculated_race_results
WHERE
  race_year BETWEEN 2011
  AND 2020
GROUP BY
  driver_name
HAVING
  COUNT(1) >= 50
ORDER BY
  avg_points DESC;