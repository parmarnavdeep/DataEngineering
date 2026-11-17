# Dataflow and GKE billing PoC runbook

The local project is `data_finops_billing_data`. For this execution, use the existing Google Cloud project `data-foundations-495802`, linked to the Data Engineering billing account. Set `PROJECT_ID` explicitly. The script defaults to `data-finops-billing-data` when no project is supplied. Google Cloud project names and IDs use different naming rules, and both exclude underscores. [Project naming rules](https://docs.cloud.google.com/resource-manager/docs/creating-managing-projects)

| Workload | Before | After | Billing evidence |
| --- | --- | --- | --- |
| Dataflow run-level comparison | One batch job without a custom label | Ten batch jobs, each with `pipeline_id=<pipeline_name>_<run_id>` | The exact label value appears on Dataflow billing rows and maps to the recorded job ID |
| GKE | One zonal Standard cluster, one node, two identical Pods, and cost allocation disabled | Same cluster and workload with cost allocation enabled, cluster label `finops_gke_cluster=data_engineering` and Pod label `finops_cost_center=data_engineering` | `labels` contains `finops_gke_cluster` on eligible GKE resources and `k8s-label/finops_cost_center` on eligible Pod cost rows |

## Set up the project

Use [Google Cloud Shell](https://shell.cloud.google.com/) or an authenticated workstation with Bash, the Google Cloud CLI, `bq`, `kubectl`, and `gke-gcloud-auth-plugin`. Copy or clone this folder into that environment. Use a principal authorized to create the project and its resources, enable APIs, grant service account access, act as both service accounts, and link the billing account. Set `FOLDER_ID` or `ORGANIZATION_ID` if your organization requires a project parent.

```bash
cd data_finops_billing_data
export PROJECT_ID=data-foundations-495802
export BILLING_ACCOUNT_ID=YOUR_BILLING_ACCOUNT_ID
./poc.sh setup
```

Setup creates the project, enables APIs, creates a VPC and subnet, grants workload service account permissions, uploads mock orders and transformation files to Cloud Storage, and creates the BigQuery dataset `finops_workload` in `us-central1`.

In Cloud Billing, check **Billing export → BigQuery export → Detailed usage cost**. If detailed export already exists, reuse its destination and obtain read access to the table. If none exists, create a separate BigQuery dataset such as `finops_billing_export` in the PoC project and select it in **Edit settings** for detailed usage cost. The export covers the linked billing account, so the query filters to the PoC project. [Configure detailed billing export](https://docs.cloud.google.com/billing/docs/how-to/export-data-bigquery-setup)

```bash
export BILLING_TABLE=EXPORT_PROJECT.EXPORT_DATASET.gcp_billing_export_resource_v1_ACCOUNT_SUFFIX
export BILLING_LOCATION=US  # Set to the actual export dataset location.
```

Use the actual export table name. The account suffix replaces hyphens in the billing account ID with underscores. A newly enabled export can take time to create the table.

## Run the Dataflow billing comparison

Run **one unlabeled baseline followed by ten labeled jobs**. Each job reads one of three mock feeds from Cloud Storage, writes to an intermediate BigQuery table, verifies that all five orders arrived and total 10,798 cents, then removes that intermediate table.

```bash
export PIPELINE_ID=mock_orders_gcs_to_bq
./poc.sh dataflow-ten
```

Save the printed experiment ID and the corresponding `dataflow-jobs-EXPERIMENT_ID.tsv` in the project directory. The log maps each per-execution run ID to its Dataflow-assigned job ID and records the usage window. The experiment ID identifies the batch of eleven jobs.

### Mock sources

The three files simulate Oracle (`oracle.csv`), Google Cloud Storage (`gcs.csv`), and SAP (`sap.csv`). Setup stages all three files in Cloud Storage. Each contains five distinct synthetic orders totaling 10,798 cents. The unlabeled baseline uses GCS. The ten labeled jobs rotate through Oracle, GCS, and SAP, giving four Oracle runs, three GCS runs, and three SAP runs.

### Label format and DAG integration

The labeled jobs submit a custom label with key `pipeline_id` and value `<pipeline_name>_<run_id>`. For example:

```text
pipeline_id=mock_orders_gcs_to_bq_finops-after-20261117003722-01
```

Each labeled job also submits `source_system=oracle`, `source_system=gcs`, or `source_system=sap`. Cloud Billing label values use lowercase. The baseline omits both labels. The ten labeled jobs share the pipeline name `mock_orders_gcs_to_bq` and use different run IDs. The complete label value must fit within 63 characters.

The PoC runner uses each Dataflow job name as its per-execution run ID. For a DAG that launches Dataflow directly, set the same `pipeline_id` key in the Dataflow launch task's `additionalUserLabels`. If the report needs the DAG's native run ID, have the DAG produce a unique label-safe value for each launch and record the original DAG run ID, label value, and Dataflow job ID together. When one DAG run launches all ten jobs, include an execution index so the ten label values remain distinct.

### Run from Airflow or Cloud Composer

Use the Cloud Composer environment `data-finops-engineering` in `us-east4`. The DAG supports Airflow 2.11 with the Google provider, or Airflow 3 with the Google and Standard providers. Enable a triggerer for the driver waits. Add [`dags/dataflow_billing_poc.py`](dags/dataflow_billing_poc.py) to the environment's DAGs folder and unpause both PoC DAGs. The `google_cloud_default` connection must authenticate as a principal that can launch and inspect Dataflow jobs in the PoC project, act as the Dataflow worker service account, query and delete intermediate tables in the output dataset, and write to the PoC bucket's `evidence/dataflow/` prefix.

Trigger **`finops_dataflow_billing_experiment`** once from the Airflow UI using this configuration. Use a fresh UTC timestamp from `date -u +%Y%m%d%H%M%S` and the bucket created by `./poc.sh setup`.

```json
{
  "experiment_id": "20261117003722",
  "project_id": "data-foundations-495802",
  "bucket_name": "finops-poc-487325807206",
  "pipeline_id": "mock_orders_gcs_to_bq",
  "template_version": "latest"
}
```

The driver triggers **`finops_dataflow_job`** eleven times. It waits for each DAG run to succeed, including the output check and evidence write, before triggering the next. The first run, `finops-before-20261117003722-00`, launches without a custom label. The following ten use their actual Airflow run IDs in the combined `pipeline_id` value. The driver stops on a failed run.

Download the evidence into the PoC project folder before querying billing:

```bash
EXPERIMENT_ID=20261117003722
POC_BUCKET=finops-poc-487325807206
gcloud storage cat "gs://$POC_BUCKET/evidence/dataflow/$EXPERIMENT_ID/*.tsv" \
  > "dataflow-jobs-$EXPERIMENT_ID.tsv"
./poc.sh dataflow-billing "$EXPERIMENT_ID"
```

### Verify captured billing rows

```bash
./poc.sh dataflow-billing EXPERIMENT_ID
```

Use the experiment ID supplied to the driver DAG or printed by `dataflow-ten`. The command queries the detailed Cloud Billing export and writes two permanent tables in `finops_billing_evidence`.

- `dataflow_before_<experiment_id>` contains the unlabeled baseline.
- `dataflow_after_<experiment_id>` contains all ten labeled runs.

The report attributes Dataflow service rows by the combined label or exported job resource identity.

Check these columns in the two saved BigQuery tables:

| Column | What to verify |
| --- | --- |
| `phase` | One distinct `before` job and ten distinct `after` jobs |
| `run_id` and `dataflow_job_id` | Each reported run maps to its recorded Dataflow job |
| `expected_billing_pipeline_id` | Empty for the baseline and unique for each labeled run |
| `billing_pipeline_ids` | Empty for the baseline and contains the exact expected value for each labeled run |
| `source_system` and `billing_source_systems` | The configured feed and the observed label values. The baseline uses GCS and has an empty observed label array. |
| `billing_status` | `UNLABELED BASELINE OBSERVED` for the baseline and `LABELED USAGE OBSERVED` for each labeled run |

A job with no attributable rows is marked pending. Rerun the billing command as usage arrives. Each query refreshes the same two tables for that experiment. A completed Dataflow job and a verified BigQuery output establish that the workload ran. The billing tables establish whether its label appeared in the export. [Detailed billing schema](https://docs.cloud.google.com/billing/docs/how-to/export-data-bigquery-tables/detailed-usage)

## Optional combined GKE and Dataflow experiment

```bash
./poc.sh run
```

Keep the shell session running for the experiment. The two GKE measurements each cover a full UTC billing hour after rollout, with additional time for startup and label propagation. During these windows, run only this PoC in the project. The baseline Compute Engine rows are scoped by isolation and time because they precede Pod allocation. The GKE cluster is deleted after its second window and before the second Dataflow job.

The Dataflow runs use Google's Cloud Storage Text to BigQuery Flex Template and write to separate `orders_before_<run-id>` and `orders_after_<run-id>` tables. Both should contain the same five synthetic orders totaling 10,798 cents. This combined experiment uses `finops_cost_center=data_engineering` as its Dataflow label. Set `TEMPLATE_VERSION` to a specific dated release if required for reproducibility. [Dataflow template](https://docs.cloud.google.com/dataflow/docs/guides/templates/provided/cloud-storage-to-bigquery)

Save the printed run ID and corresponding `windows-<run-id>.tsv`. It records UTC measurement boundaries and the Dataflow job IDs and GKE cluster name.

### Verify the combined experiment

```bash
./poc.sh billing RUN_ID
```

Each query saves a timestamped BigQuery snapshot in `finops_billing_evidence` in the PoC project. This dataset is created in the detailed export's location so BigQuery can read the source and write the snapshot in one query. Each snapshot contains the before and after summaries and their SKU-level evidence. Rerunning after additional billing rows arrive creates another snapshot, so you can retain the earlier results. The original billed usage timestamps determine the phase, even when a row is exported after labels are enabled.

Read the four summary rows by phase, then inspect SKU detail to see the exact label values and the corresponding gross and net costs. The GKE summary includes cluster, node, and overhead costs within the measured windows. Pod label evidence requires the expected cluster, namespace, workload name, and Pod label to appear together. Cluster resource label visibility is counted independently. The Dataflow comparison uses time overlap with each isolated run and includes the Dataflow service SKUs. Cloud Storage and BigQuery charges are outside that comparison.

A successful run shows billing rows in both baselines with no new custom labels, Dataflow rows after labeling with the expected job label, and GKE rows after enablement with both cluster resource and Pod label evidence. Missing billing rows show as pending. GKE allocation data can take up to three days to appear. Rerun the query after export catches up. Use SKU-level rows to inspect system overhead, unallocated capacity, and unsupported SKUs. Net cost includes credits. Label evidence may appear on a row with zero net cost. [GKE cost allocation](https://docs.cloud.google.com/kubernetes-engine/docs/how-to/cost-allocations) · [Detailed billing schema](https://docs.cloud.google.com/billing/docs/how-to/export-data-bigquery-tables/detailed-usage)

## Cleanup

The runner attempts to delete the cluster when it completes or exits after provisioning. If the shell disconnects before cleanup, use `./poc.sh stop-gke`. If the Dataflow launch was interrupted, inspect **Dataflow → Jobs** and cancel any active PoC job. The project, bucket, and BigQuery tables remain available for validation. In an existing project, remove only PoC resources after saving the evidence. For a dedicated disposable project, project deletion is also an option. If this project hosts the billing account's export, preserve that dataset and export destination while the account needs them.
