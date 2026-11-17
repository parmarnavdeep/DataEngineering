WITH jobs AS (
  SELECT
    JSON_VALUE(job, '$.phase') AS phase,
    JSON_VALUE(job, '$.pipeline_id') AS pipeline_id,
    NULLIF(JSON_VALUE(job, '$.source_system'), '') AS source_system,
    JSON_VALUE(job, '$.job_name') AS job_name,
    JSON_VALUE(job, '$.job_id') AS job_id,
    TIMESTAMP_SECONDS(CAST(JSON_VALUE(job, '$.start') AS INT64)) AS starts_at,
    TIMESTAMP_SECONDS(CAST(JSON_VALUE(job, '$.end') AS INT64)) AS ends_at
  FROM UNNEST(JSON_QUERY_ARRAY(@jobs)) AS job
),
export_rows AS (
  SELECT
    resource.name AS resource_name,
    ARRAY_REVERSE(SPLIT(resource.global_name, '/'))[SAFE_OFFSET(0)] AS resource_id,
    (SELECT l.value FROM UNNEST(labels) AS l WHERE l.key = 'pipeline_id' LIMIT 1) AS billed_pipeline_id,
    (SELECT l.value FROM UNNEST(labels) AS l WHERE l.key = 'source_system' LIMIT 1) AS billed_source_system,
    CAST(cost AS NUMERIC) AS gross_cost,
    CAST(cost AS NUMERIC) + COALESCE((
      SELECT SUM(CAST(c.amount AS NUMERIC)) FROM UNNEST(credits) AS c
    ), 0) AS net_cost,
    currency,
    export_time
  FROM `BILLING_EXPORT_TABLE`
  WHERE project.id = @project_id
    AND service.description = 'Dataflow'
    AND cost_type = 'regular'
    AND usage_start_time >= TIMESTAMP_SUB((SELECT MIN(starts_at) FROM jobs), INTERVAL 1 HOUR)
    AND usage_start_time < (SELECT MAX(ends_at) FROM jobs)
),
attributed AS (
  SELECT
    COALESCE(by_label.job_name, by_id.job_name, by_name.job_name) AS matched_job_name,
    e.*
  FROM export_rows AS e
  LEFT JOIN jobs AS by_label
    ON by_label.phase = 'after'
   AND e.billed_pipeline_id = CONCAT(by_label.pipeline_id, '_', by_label.job_name)
  LEFT JOIN jobs AS by_id ON by_id.job_id = e.resource_id
  LEFT JOIN jobs AS by_name ON by_name.job_name = e.resource_name
)
SELECT
  j.phase,
  j.pipeline_id,
  j.source_system,
  j.job_name AS run_id,
  IF(j.phase = 'after', CONCAT(j.pipeline_id, '_', j.job_name), NULL) AS expected_billing_pipeline_id,
  j.job_id AS dataflow_job_id,
  j.starts_at,
  j.ends_at,
  a.currency,
  COUNT(a.matched_job_name) AS billing_rows,
  ARRAY_AGG(DISTINCT a.billed_pipeline_id IGNORE NULLS) AS billing_pipeline_ids,
  ARRAY_AGG(DISTINCT a.billed_source_system IGNORE NULLS) AS billing_source_systems,
  COUNTIF(a.billed_pipeline_id = CONCAT(j.pipeline_id, '_', j.job_name)) AS expected_pipeline_id_rows,
  SUM(a.gross_cost) AS gross_cost,
  SUM(a.net_cost) AS net_cost,
  MAX(a.export_time) AS last_export_time,
  CASE
    WHEN COUNT(a.matched_job_name) = 0 THEN 'PENDING OR NO MATCHING JOB RESOURCE'
    WHEN j.phase = 'before' AND COUNTIF(a.billed_pipeline_id IS NOT NULL OR a.billed_source_system IS NOT NULL) = 0
      THEN 'UNLABELED BASELINE OBSERVED'
    WHEN j.phase = 'after' AND COUNTIF(
      a.billed_pipeline_id = CONCAT(j.pipeline_id, '_', j.job_name)
      AND (j.source_system IS NULL OR a.billed_source_system = j.source_system)
    ) > 0
      THEN 'LABELED USAGE OBSERVED'
    ELSE 'CHECK LABELS'
  END AS billing_status
FROM jobs AS j
LEFT JOIN attributed AS a ON a.matched_job_name = j.job_name
GROUP BY j.phase, j.pipeline_id, j.source_system, j.job_name, j.job_id, j.starts_at, j.ends_at, a.currency
ORDER BY j.starts_at, j.job_name, a.currency;
