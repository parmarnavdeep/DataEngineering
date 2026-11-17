#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

project=${PROJECT_ID:-data-finops-billing-data}
region=us-central1
zone=us-central1-a
dataset=finops_workload
network=finops-poc
subnet=finops-poc-central
cluster=finops-billing-poc
worker="finops-dataflow@${project}.iam.gserviceaccount.com"
node="finops-gke@${project}.iam.gserviceaccount.com"
display_name=data-finops-billing-data

die() { printf '%s\n' "$*" >&2; exit 1; }
[[ "$project" =~ ^[a-z][a-z0-9-]{4,28}[a-z0-9]$ ]] || die "Invalid PROJECT_ID"
command -v gcloud >/dev/null || die "Google Cloud CLI is required."
command -v bq >/dev/null || die "The bq CLI is required."

gc() { gcloud --quiet --project="$project" "$@"; }
bq_poc() { bq --project_id="$project" --location="$region" "$@"; }

setup() {
  : "${BILLING_ACCOUNT_ID:?Set BILLING_ACCOUNT_ID before setup}"
  if ! gcloud projects describe "$project" --format='value(name)' >/dev/null 2>&1; then
    local parent=(--quiet)
    if [[ -n "${FOLDER_ID:-}" ]]; then
      parent+=(--folder="$FOLDER_ID")
    elif [[ -n "${ORGANIZATION_ID:-}" ]]; then
      parent+=(--organization="$ORGANIZATION_ID")
    fi
    gcloud projects create "$project" --name="$display_name" "${parent[@]}"
  fi
  check_project
  local linked
  linked=$(gc billing projects describe "$project" --format='value(billingAccountName)')
  [[ -z "$linked" || "$linked" == "billingAccounts/$BILLING_ACCOUNT_ID" ]] ||
    die "Project already linked to another billing account."
  if [[ -z "$linked" ]]; then
    gc billing projects link "$project" --billing-account="$BILLING_ACCOUNT_ID"
  fi
  gc services enable compute.googleapis.com container.googleapis.com dataflow.googleapis.com \
    bigquery.googleapis.com storage.googleapis.com iam.googleapis.com

  gc compute networks describe "$network" >/dev/null 2>&1 ||
    gc compute networks create "$network" --subnet-mode=custom
  gc compute networks subnets describe "$subnet" --region="$region" >/dev/null 2>&1 ||
    gc compute networks subnets create "$subnet" --region="$region" --network="$network" --range=10.90.0.0/24

  gc iam service-accounts describe "$worker" >/dev/null 2>&1 ||
    gc iam service-accounts create finops-dataflow
  gc iam service-accounts describe "$node" >/dev/null 2>&1 ||
    gc iam service-accounts create finops-gke
  for role in roles/dataflow.worker roles/bigquery.jobUser; do
    gc projects add-iam-policy-binding "$project" --member="serviceAccount:$worker" --role="$role" --condition=None >/dev/null
  done
  gc projects add-iam-policy-binding "$project" --member="serviceAccount:$node" \
    --role=roles/container.defaultNodeServiceAccount --condition=None >/dev/null

  bucket_name
  gc storage buckets describe "$bucket" >/dev/null 2>&1 ||
    gc storage buckets create "$bucket" --location="$region" --uniform-bucket-level-access
  gc storage buckets add-iam-policy-binding "$bucket" --member="serviceAccount:$worker" \
    --role=roles/storage.objectAdmin >/dev/null
  gc storage cp mock.csv oracle.csv gcs.csv sap.csv schema.json transform.js "$bucket/input/"

  bq_poc show "$project:$dataset" >/dev/null 2>&1 || bq_poc mk --dataset "$project:$dataset"
  bq_poc query --use_legacy_sql=false \
    "GRANT \`roles/bigquery.dataEditor\` ON SCHEMA \`$project.$dataset\` TO 'serviceAccount:$worker'"
}

check_project() {
  local actual_name
  actual_name=$(gcloud projects describe "$project" --format='value(name)')
  [[ -n "${PROJECT_ID:-}" || "$actual_name" == "$display_name" ]] ||
    die "Set PROJECT_ID explicitly to use the existing project $project."
}

bucket_name() {
  bucket="gs://finops-poc-$(gc projects describe "$project" --format='value(projectNumber)')"
}

need_export_table() {
  : "${BILLING_TABLE:?Set BILLING_TABLE to the project.dataset.table for the detailed billing export}"
  [[ "$BILLING_TABLE" =~ ^[a-z][a-z0-9-]+\.[a-zA-Z0-9_]+\.gcp_billing_export_resource_v1_[a-zA-Z0-9_]+$ ]] ||
    die "Expected a fully qualified detailed usage export table."
}

run_dataflow() {
  local phase=$1 iteration=${2:-} start end launch job state deadline options=(--num-workers=1 --max-workers=1)
  if [[ "$phase" == after ]]; then
    options+=(--additional-user-labels=finops_cost_center=data_engineering)
  fi
  local suffix="$run_id${iteration:+-$iteration}"
  local job_name="finops-$phase-$suffix"
  local output="orders_${phase}_${suffix//-/_}"
  local source_system=${3:-}
  local input_file=${source_system:-mock}.csv
  if [[ "$phase" == after && -n "$iteration" ]]; then
    options=(--num-workers=1 --max-workers=1 "--additional-user-labels=pipeline_id=${pipeline_id}_${job_name},source_system=$source_system")
  fi
  start=$(date -u +%s)
  launch=$(gc dataflow flex-template run "$job_name" \
    --region="$region" --template-file-gcs-location="gs://dataflow-templates/${TEMPLATE_VERSION:-latest}/flex/GCS_Text_to_BigQuery_Flex" \
    --network="$network" --subnetwork="regions/$region/subnetworks/$subnet" \
    --service-account-email="$worker" \
    --temp-location="$bucket/temp/$suffix/$phase" \
    --parameters="inputFilePattern=$bucket/input/$input_file,JSONPath=$bucket/input/schema.json,javascriptTextTransformGcsPath=$bucket/input/transform.js,javascriptTextTransformFunctionName=transformOrder,outputTable=$project:$dataset.$output,bigQueryLoadingTemporaryDirectory=$bucket/load/$suffix/$phase" \
    "${options[@]}" --format='value(job.id,id)')
  read -r job _ <<< "$launch"
  [[ -n "$job" ]] || die "Dataflow launch returned no job ID. Inspect the Dataflow Jobs page."
  printf 'Dataflow %s job %s\n' "$phase" "$job"
  deadline=$((start + 2700))
  while true; do
    state=$(gc dataflow jobs describe "$job" --region="$region" --format='value(currentState)')
    case "$state" in
      JOB_STATE_DONE) break ;;
      JOB_STATE_FAILED|JOB_STATE_CANCELLED|JOB_STATE_DRAINED|JOB_STATE_UPDATED)
        die "Dataflow $job ended in $state" ;;
    esac
    if (( $(date -u +%s) >= deadline )); then
      gc dataflow jobs cancel "$job" --region="$region"
      die "Dataflow job timed out: $job"
    fi
    sleep 20
  done
  end=$(date -u +%s)
  if [[ -n "$iteration" ]]; then
    local validation_status=0
    bq_poc query --use_legacy_sql=false "
      ASSERT (SELECT COUNT(*) = 5 AND COUNT(DISTINCT order_id) = 5 AND SUM(amount_cents) = 10798
        FROM \`$project.$dataset.$output\`) AS 'Unexpected mock orders';" || validation_status=$?
    bq_poc rm -f -t "$project:$dataset.$output"
    (( validation_status == 0 )) || return "$validation_status"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$phase" "$pipeline_id" "$job_name" "$job" "$start" "$((end + 1))" "$source_system" >> "$windows"
  else
    printf 'dataflow\t%s\t%s\t%s\t%s\n' "$phase" "$start" "$((end + 1))" "$job" >> "$windows"
  fi
}

delete_cluster() {
  gc container clusters delete "$cluster" --zone="$zone"
}

measure_gke() {
  local phase=$1 now start end
  kubectl --namespace=finops rollout status deployment/billing-demo --timeout=10m
  now=$(date -u +%s)
  start=$(( (now / 3600 + 1) * 3600 ))
  end=$((start + 3600))
  printf 'GKE %s UTC measurement starts at epoch %s and ends at %s.\n' "$phase" "$start" "$end"
  sleep "$((end - now))"
  kubectl --namespace=finops rollout status deployment/billing-demo --timeout=5m
  printf 'gke\t%s\t%s\t%s\t%s\n' "$phase" "$start" "$end" "$cluster" >> "$windows"
}

run() {
  need_export_table
  check_project
  bucket_name
  command -v kubectl >/dev/null || die "kubectl is required."
  command -v gke-gcloud-auth-plugin >/dev/null || die "gke-gcloud-auth-plugin is required."
  run_id=$(date -u +%Y%m%d%H%M%S)
  windows="$PWD/windows-$run_id.tsv"
  (set -o noclobber; : > "$windows")
  printf 'Run ID %s. Evidence file %s\n' "$run_id" "$windows"

  run_dataflow before
  export KUBECONFIG
  KUBECONFIG=$(mktemp)
  trap 'rm -f "$KUBECONFIG"' EXIT
  gc container clusters create "$cluster" --zone="$zone" --network="$network" --subnetwork="$subnet" \
    --enable-ip-alias --cluster-ipv4-cidr=10.92.0.0/20 --services-ipv4-cidr=10.93.0.0/24 \
    --num-nodes=1 --machine-type=e2-standard-2 --disk-size=30 --disk-type=pd-standard \
    --service-account="$node"
  trap 'delete_cluster || printf "Cluster cleanup failed. Run ./poc.sh stop-gke.\n" >&2; rm -f "$KUBECONFIG"' EXIT
  trap 'exit 130' INT
  trap 'exit 143' TERM
  gc container clusters get-credentials "$cluster" --zone="$zone"

  local enabled
  enabled=$(gc container clusters describe "$cluster" --zone="$zone" --format='value(costManagementConfig.enabled)')
  case "$enabled" in
    True|true) die "Baseline requires cost allocation to be disabled." ;;
  esac
  kubectl create namespace finops
  kubectl --namespace=finops apply -f workload.yaml
  measure_gke before

  gc container clusters update "$cluster" --zone="$zone" --enable-cost-allocation
  gc container clusters update "$cluster" --zone="$zone" --update-labels=finops_gke_cluster=data_engineering
  enabled=$(gc container clusters describe "$cluster" --zone="$zone" --format='value(costManagementConfig.enabled)')
  [[ "$enabled" == True || "$enabled" == true ]] || die "GKE cost allocation was not enabled."
  kubectl --namespace=finops patch deployment billing-demo --type=merge \
    --patch '{"spec":{"template":{"metadata":{"labels":{"finops_cost_center":"data_engineering"}}}}}'
  measure_gke after
  kubectl --namespace=finops get pods -l finops_cost_center=data_engineering
  delete_cluster
  trap 'rm -f "$KUBECONFIG"' EXIT

  run_dataflow after
  bq_poc query --use_legacy_sql=false "
    ASSERT (SELECT COUNT(*) = 5 AND COUNT(DISTINCT order_id) = 5 AND SUM(amount_cents) = 10798
      FROM \`$project.$dataset.orders_before_$run_id\`) AS 'Unexpected baseline orders';
    ASSERT (SELECT COUNT(*) = 5 AND COUNT(DISTINCT order_id) = 5 AND SUM(amount_cents) = 10798
      FROM \`$project.$dataset.orders_after_$run_id\`) AS 'Unexpected labeled orders';
    ASSERT (NOT EXISTS (
      SELECT * FROM \`$project.$dataset.orders_before_$run_id\`
      EXCEPT DISTINCT
      SELECT * FROM \`$project.$dataset.orders_after_$run_id\`
    )) AS 'Orders differ between runs';"
  printf 'Run ./poc.sh billing %s after usage appears in the detailed export.\n' "$run_id"
}

run_dataflow_ten() {
  check_project
  bucket_name
  run_id=$(date -u +%Y%m%d%H%M%S)
  windows="$PWD/dataflow-jobs-$run_id.tsv"
  (set -o noclobber; : > "$windows")
  printf 'Dataflow run ID %s. Job evidence file %s\n' "$run_id" "$windows"
  local iteration pipeline_id=${PIPELINE_ID:-mock_orders_gcs_to_bq}
  [[ "$pipeline_id" =~ ^[a-z][a-z0-9_-]{0,62}$ ]] || die "Invalid PIPELINE_ID label value."
  local longest_label="${pipeline_id}_finops-after-${run_id}-10"
  (( ${#longest_label} <= 63 )) || die "PIPELINE_ID plus the run ID must fit in a 63-character label value."
  local sources=(oracle gcs sap)
  run_dataflow before 01 gcs
  for iteration in 01 02 03 04 05 06 07 08 09 10; do
    run_dataflow after "$iteration" "${sources[$(( (10#$iteration - 1) % 3 ))]}"
  done
  printf 'Run ./poc.sh dataflow-billing %s after usage appears in the detailed export.\n' "$run_id"
}

dataflow_billing() {
  need_export_table
  local id=${1:-} phase pipeline_id job_name job_id start end source_system
  [[ "$id" =~ ^[0-9]{14}$ ]] || die "Pass the Dataflow run ID."
  local evidence="dataflow-jobs-$id.tsv" rows="" count=0
  [[ -s "$evidence" ]] || die "Missing $evidence"
  while IFS=$'\t' read -r phase pipeline_id job_name job_id start end source_system; do
    [[ "$phase" =~ ^(before|after)$ && "$pipeline_id" =~ ^[a-z][a-z0-9_-]{0,62}$ && "$job_name" =~ ^[a-z0-9-]+$ &&
       "$job_id" =~ ^[a-zA-Z0-9_-]+$ && "$start" =~ ^[0-9]{10}$ && "$end" =~ ^[0-9]{10}$ ]] ||
      die "Invalid job record in $evidence"
    [[ -z "$source_system" || "$source_system" =~ ^(oracle|gcs|sap)$ ]] || die "Invalid source system in $evidence"
    rows+="${rows:+,}{\"source_system\":\"$source_system\",\"phase\":\"$phase\",\"pipeline_id\":\"$pipeline_id\",\"job_name\":\"$job_name\",\"job_id\":\"$job_id\",\"start\":$start,\"end\":$end}"
    count=$((count + 1))
  done < "$evidence"
  [[ "$count" == 11 ]] || printf 'Reporting %s of 11 recorded jobs.\n' "$count" >&2
  local location=${BILLING_LOCATION:-US}
  local evidence_dataset="$project:finops_billing_evidence"
  local before="$project.finops_billing_evidence.dataflow_before_$id"
  local after="$project.finops_billing_evidence.dataflow_after_$id"
  ensure_billing_dataset "$location" "$evidence_dataset"
  {
    printf 'CREATE TEMP TABLE comparison AS\n'
    sed "s|BILLING_EXPORT_TABLE|$BILLING_TABLE|g" dataflow_billing.sql
    printf 'CREATE OR REPLACE TABLE `%s` AS SELECT * FROM comparison WHERE phase = "before";\n' "$before"
    printf 'CREATE OR REPLACE TABLE `%s` AS SELECT * FROM comparison WHERE phase = "after";\n' "$after"
  } | bq --project_id="$project" --location="$location" query --use_legacy_sql=false \
    --parameter="project_id:STRING:$project" --parameter="jobs:STRING:[$rows]"
  printf 'Saved before billing to %s\nSaved after billing to %s\n' "$before" "$after"
}

billing() {
  need_export_table
  run_id=${1:-}
  [[ "$run_id" =~ ^[0-9]{14}$ ]] || die "Pass the run ID printed by ./poc.sh run."
  local windows_file="windows-$run_id.tsv" workload phase start end resource
  [[ -f "$windows_file" ]] || die "Missing $windows_file"
  local args=() key seen=" "
  while IFS=$'\t' read -r workload phase start end resource; do
    key="${workload}_${phase}"
    case "$key" in
      dataflow_before|dataflow_after|gke_before|gke_after) ;;
      *) die "Unexpected window in $windows_file" ;;
    esac
    [[ "$seen" != *" $key "* && "$start" =~ ^[0-9]{10}$ && "$end" =~ ^[0-9]{10}$ && "$end" -gt "$start" ]] ||
      die "Invalid or duplicate window in $windows_file"
    seen+="$key "
    args+=("--parameter=${key}_start:INT64:$start" "--parameter=${key}_end:INT64:$end")
  done < "$windows_file"
  [[ "${#args[@]}" == 8 ]] || die "The run did not record all four measurement windows."
  save_billing_snapshot billing.sql "comparison_$run_id" --parameter="cluster_name:STRING:$cluster" "${args[@]}"
}

save_billing_snapshot() {
  local sql_file=$1 prefix=$2
  shift 2
  local location=${BILLING_LOCATION:-US}
  local evidence_dataset="$project:finops_billing_evidence"
  local snapshot="${prefix}_$(date -u +%Y%m%d%H%M%S)"
  ensure_billing_dataset "$location" "$evidence_dataset"
  sed "s|BILLING_EXPORT_TABLE|$BILLING_TABLE|g" "$sql_file" |
    bq --project_id="$project" --location="$location" query --use_legacy_sql=false --max_rows=200 \
      --destination_table="$evidence_dataset.$snapshot" \
      --parameter="project_id:STRING:$project" "$@"
  printf 'Saved billing snapshot to %s.%s\n' "$evidence_dataset" "$snapshot"
}

ensure_billing_dataset() {
  local location=$1 evidence_dataset=$2
  bq --project_id="$project" --location="$location" show "$evidence_dataset" >/dev/null 2>&1 ||
    bq --project_id="$project" --location="$location" mk --dataset "$evidence_dataset"
}

case "${1:-}" in
  setup) setup ;;
  run) run ;;
  dataflow-ten) run_dataflow_ten ;;
  dataflow-billing) dataflow_billing "${2:-}" ;;
  billing) billing "${2:-}" ;;
  stop-gke) check_project; delete_cluster ;;
  *) die "Usage: ./poc.sh {setup|run|billing RUN_ID|dataflow-ten|dataflow-billing RUN_ID|stop-gke}" ;;
esac
