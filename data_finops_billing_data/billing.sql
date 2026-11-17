-- BILLING_EXPORT_TABLE is replaced by poc.sh. Run parameters come from the experiment log.
WITH windows AS (
  SELECT 'dataflow' AS workload, 'before' AS phase,
    TIMESTAMP_SECONDS(@dataflow_before_start) AS starts_at,
    TIMESTAMP_SECONDS(@dataflow_before_end) AS ends_at
  UNION ALL SELECT 'dataflow', 'after',
    TIMESTAMP_SECONDS(@dataflow_after_start), TIMESTAMP_SECONDS(@dataflow_after_end)
  UNION ALL SELECT 'gke', 'before',
    TIMESTAMP_SECONDS(@gke_before_start), TIMESTAMP_SECONDS(@gke_before_end)
  UNION ALL SELECT 'gke', 'after',
    TIMESTAMP_SECONDS(@gke_after_start), TIMESTAMP_SECONDS(@gke_after_end)
),
export_rows AS (
  SELECT
    usage_start_time,
    usage_end_time,
    service.description AS service,
    CASE service.description
      WHEN 'Dataflow' THEN 'dataflow'
      WHEN 'Compute Engine' THEN 'gke'
      WHEN 'Kubernetes Engine' THEN 'gke'
      WHEN 'Google Kubernetes Engine' THEN 'gke'
    END AS workload_family,
    sku.description AS sku,
    currency,
    export_time,
    CAST(cost AS NUMERIC) AS gross_cost,
    CAST(cost AS NUMERIC) + COALESCE((
      SELECT SUM(CAST(credit.amount AS NUMERIC)) FROM UNNEST(credits) AS credit
    ), 0) AS net_cost,
    (SELECT label.value FROM UNNEST(labels) AS label
     WHERE label.key = 'finops_cost_center' LIMIT 1) AS dataflow_label,
    (SELECT label.value FROM UNNEST(labels) AS label
     WHERE label.key = 'finops_gke_cluster' LIMIT 1) AS gke_resource_label,
    (SELECT label.value FROM UNNEST(labels) AS label
     WHERE label.key = 'k8s-label/finops_cost_center' LIMIT 1) AS gke_pod_label,
    (SELECT label.value FROM UNNEST(labels) AS label
     WHERE label.key = 'goog-k8s-cluster-name' LIMIT 1) AS gke_cluster,
    (SELECT label.value FROM UNNEST(labels) AS label
     WHERE label.key = 'k8s-namespace' LIMIT 1) AS gke_namespace,
    (SELECT label.value FROM UNNEST(labels) AS label
     WHERE label.key = 'k8s-workload-name' LIMIT 1) AS gke_workload
  FROM `BILLING_EXPORT_TABLE`
  WHERE project.id = @project_id
    AND cost_type = 'regular'
    AND service.description IN ('Dataflow', 'Compute Engine', 'Kubernetes Engine', 'Google Kubernetes Engine')
    AND usage_start_time >= TIMESTAMP_SUB(TIMESTAMP_SECONDS(@dataflow_before_start), INTERVAL 1 HOUR)
    AND usage_start_time < TIMESTAMP_SECONDS(@dataflow_after_end)
),
matched AS (
  SELECT
    w.workload,
    w.phase,
    w.starts_at,
    w.ends_at,
    e.*,
    CASE WHEN w.workload = 'dataflow' THEN e.dataflow_label ELSE e.gke_pod_label END AS observed_label,
    CASE WHEN w.workload = 'dataflow' THEN e.dataflow_label = 'data_engineering'
      ELSE e.gke_pod_label = 'data_engineering' AND e.gke_cluster = @cluster_name
        AND e.gke_namespace = 'finops' AND e.gke_workload = 'billing-demo'
    END AS expected_label_match
  FROM windows AS w
  LEFT JOIN export_rows AS e
    ON e.workload_family = w.workload
   AND e.usage_start_time < w.ends_at
   AND CASE w.workload
         WHEN 'dataflow' THEN e.usage_end_time > w.starts_at
         ELSE e.usage_start_time >= w.starts_at
       END
)
SELECT
  IF(GROUPING(service) = 1, 'summary', 'sku_detail') AS row_type,
  workload,
  phase,
  starts_at,
  ends_at,
  currency,
  service,
  sku,
  observed_label,
  gke_resource_label,
  gke_cluster,
  gke_namespace,
  gke_workload,
  COUNT(service) AS billing_rows,
  COUNTIF(observed_label IS NOT NULL) AS custom_label_rows,
  COUNTIF(expected_label_match) AS expected_label_rows,
  COUNTIF(gke_cluster = @cluster_name AND gke_namespace = 'finops') AS gke_namespace_rows,
  COUNTIF(gke_resource_label = 'data_engineering') AS gke_resource_label_rows,
  SUM(gross_cost) AS gross_cost,
  SUM(net_cost) AS net_cost,
  SUM(IF(expected_label_match, net_cost, 0)) AS expected_label_net_cost,
  MAX(export_time) AS last_export_time,
  CASE
    WHEN GROUPING(service) = 0 THEN NULL
    WHEN COUNT(service) = 0 THEN 'PENDING'
    WHEN phase = 'before' AND COUNTIF(observed_label IS NOT NULL OR gke_resource_label IS NOT NULL) = 0
      AND COUNTIF(gke_cluster = @cluster_name AND gke_namespace = 'finops') = 0 THEN 'BASELINE OBSERVED'
    WHEN phase = 'before' THEN 'CHECK BASELINE LABELS'
    WHEN COUNTIF(expected_label_match) = 0 THEN 'EXPECTED LABEL PENDING OR MISSING'
    WHEN workload = 'gke' AND COUNTIF(gke_resource_label = 'data_engineering') = 0
      THEN 'POD LABEL OBSERVED; GKE RESOURCE LABEL PENDING OR MISSING'
    ELSE 'EXPECTED LABEL OBSERVED'
  END AS evidence_status
FROM matched
GROUP BY GROUPING SETS (
  (workload, phase, starts_at, ends_at, currency),
  (workload, phase, starts_at, ends_at, currency, service, sku, observed_label,
   gke_resource_label, gke_cluster, gke_namespace, gke_workload)
)
HAVING COUNT(service) > 0 OR GROUPING(service) = 1
ORDER BY row_type DESC, workload, starts_at, currency, service, sku, gke_namespace;
