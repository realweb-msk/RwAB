-- EXAMPLE QUERY WITH GOOGLE BQ USING STANDARD SQL AND GoogleAnalytics 360 (UA) STREAMING DATA
-- https://support.google.com/analytics/answer/3437719
DECLARE start_date, end_date, experiment_id STRING;
SET start_date = '{start_date}'; SET end_date = '{end_date}'; SET experiment_id = '{experiment_id}';

WITH main AS(
SELECT 
-- DIMENSIONS
TIMESTAMP(PARSE_DATE('%Y%m%d', date)) AS date,
clientid AS client_id,
device.deviceCategory AS device,
geoNetwork.region AS region,

-- METRICS
MAX(experimentVariant) AS experimentVariant,
IF(totals.newVisits = 1, 'new_visitor', 'returning_visitor') AS visitor_type,
CONCAT(fullvisitorid, CAST(visitstarttime AS STRING)) AS session_id,
COUNT(DISTINCT(transaction.transactionId)) AS transactions,
SUM(transaction.transactionRevenue / 1000000) AS transactionRevenue,
MAX(hits.time) / 1e3 AS duration,
COUNTIF(hits.type = 'PAGE') AS pageviews,

FROM `projectID.datasetID.ga_sessions_*` ga, UNNEST(hits) AS hits, UNNEST(hits.experiment)
WHERE _TABLE_SUFFIX BETWEEN start_date AND end_date
AND experimentId = experiment_id
GROUP BY date, client_id, device, visitor_type, session_id, geoNetwork.region
)

SELECT * FROM main
