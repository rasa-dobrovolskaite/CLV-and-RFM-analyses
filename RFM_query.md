```sql
WITH rfm_values AS (
  SELECT 
  DISTINCT(rfm.CustomerID), 
  DATE_DIFF('2011-12-01', DATE(MAX(rfm.InvoiceDate)), DAY) AS recency,
  COUNT(DISTINCT rfm.InvoiceNo) AS frequency,
  ROUND(SUM(rfm.UnitPrice * rfm.Quantity),2) AS monetary,
  FROM `turing_data_analytics.rfm` rfm
  WHERE rfm.InvoiceDate >= '2010-12-01' AND rfm.InvoiceDate <= '2011-12-01'
  AND rfm.Quantity > 0
  AND rfm.UnitPrice > 0 
  AND rfm.CustomerID IS NOT NULL
  GROUP BY CustomerID
),  
-- rfm_values CTE returns recency, frequence and monetary values for each customers. 
-- Additionally, it filters data to only include relevant results.

rfm_quartiles AS (
  SELECT
    APPROX_QUANTILES(rfm_values.monetary, 4)[OFFSET(1)] AS m25,
    APPROX_QUANTILES(rfm_values.monetary, 4)[OFFSET(2)] AS m50,
    APPROX_QUANTILES(rfm_values.monetary, 4)[OFFSET(3)] AS m75,
    APPROX_QUANTILES(rfm_values.frequency, 4)[OFFSET(1)] AS f25,
    APPROX_QUANTILES(rfm_values.frequency, 4)[OFFSET(2)] AS f50,
    APPROX_QUANTILES(rfm_values.frequency, 4)[OFFSET(3)] AS f75,
    APPROX_QUANTILES(rfm_values.recency, 4)[OFFSET(1)] AS r25,
    APPROX_QUANTILES(rfm_values.recency, 4)[OFFSET(2)] AS r50,
    APPROX_QUANTILES(rfm_values.recency, 4)[OFFSET(3)] AS r75
  FROM rfm_values
),
-- rfm_quartiles CTE is used to set quartiles for monetary, frequency and recency values. It divides values into 4 parts.

rfm_scores AS (
  SELECT
    rfm_values.*,
    CASE WHEN rfm_values.recency <= rfm_quartiles.r25 THEN 4 WHEN rfm_values.recency <= rfm_quartiles.r50 THEN 3 WHEN rfm_values.recency <= rfm_quartiles.r75 THEN 2 ELSE 1 END AS R,
    CASE WHEN rfm_values.frequency <= rfm_quartiles.f25 THEN 1 WHEN rfm_values.frequency <= rfm_quartiles.f50 THEN 2 WHEN rfm_values.frequency <= rfm_quartiles.f75 THEN 3 ELSE 4 END AS F,
    CASE WHEN rfm_values.monetary <= rfm_quartiles.m25 THEN 1 WHEN rfm_values.monetary <= rfm_quartiles.m50 THEN 2 WHEN rfm_values.monetary <= rfm_quartiles.m75 THEN 3 ELSE 4 END AS M,
  FROM rfm_values
  JOIN rfm_quartiles ON TRUE
  ORDER BY CustomerID
),
-- rfm_scores CTE is used to assign values in each quartile a rank from 1 to 4 using a CASE WHEN function. Set rankings will be used to return a common RFM score.

rfm_concat AS (
  SELECT
  CustomerID,
  R,
  F,
  M,
  recency,
  frequency,
  monetary,
  CAST(CONCAT(R, F, M) AS INT64) AS rfm_score
  FROM rfm_scores
  GROUP BY CustomerID, rfm_score, R, F, M, frequency, recency, monetary
),
-- rfm_concat connects separate R, F, M values into a common RFM score.

segments AS (
  SELECT "Best Customers" AS Segment, ARRAY<INT64>[444, 443, 434, 344] AS `RFM_scores` UNION ALL
  SELECT "Loyal Customers", ARRAY<INT64>[343, 342, 442, 334, 433] UNION ALL 
  SELECT "Potential Loyalist", ARRAY<INT64>[324, 332, 422, 333, 323, 423, 424, 432] UNION ALL
  SELECT "Recent Customers", ARRAY<INT64>[414, 413, 412, 411, 314, 313, 312, 311] UNION ALL 
  SELECT "Promising Customers", ARRAY<INT64>[331, 321, 322, 341, 421, 431, 441] UNION ALL
  SELECT "Customers Needing Attention", ARRAY<INT64>[221, 222, 223, 231, 232, 233, 241, 242] UNION ALL 
  SELECT "At Risk", ARRAY<INT64>[113, 114, 123, 124, 133, 141, 142, 213, 214] UNION ALL
  SELECT "Can't Lose Them", ARRAY<INT64>[134, 143, 144, 224, 234, 243, 244] UNION ALL
  SELECT "Hibernating", ARRAY<INT64>[122, 131, 132, 211, 212] UNION ALL
  SELECT "Lost Customers", ARRAY<INT64>[111, 112, 121] 
)
-- This CTE defines all segments and the exact rfm score that belongs to the segment. 

SELECT
  CustomerID,
  recency,
  frequency,
  monetary,
  R,
  F,
  M,
  rfm_concat.rfm_score,
  segments.Segment
FROM rfm_concat
LEFT JOIN segments ON rfm_concat.rfm_score IN UNNEST(segments.RFM_scores)
ORDER BY CustomerID
 
-- Lastly, final table is pulled with needed data for RFM analysis.
-- To assign each customer a segment, UNNEST function is used when joining tables.
