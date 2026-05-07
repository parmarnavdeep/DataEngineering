import sys
import yaml
from google.cloud import bigquery_datatransfer_v1
from google.protobuf.struct_pb2 import Struct


def to_struct(data: dict) -> Struct:
    s = Struct()
    s.update(data)
    return s


def deploy_scheduled_query(config_path: str) -> None:
    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f)

    transform = cfg.get("transform", {})

    if not transform.get("enabled", False):
        print("Transform disabled, skipping scheduled query")
        return

    project_id = cfg["project_id"]
    location = cfg["location"]

    client = bigquery_datatransfer_v1.DataTransferServiceClient()
    parent = f"projects/{project_id}/locations/{location}"

    transfer_config = bigquery_datatransfer_v1.TransferConfig(
        display_name=f"{cfg['job_name']}_transform",
        data_source_id="scheduled_query",
        schedule=cfg["transfer"]["schedule"],
        params=to_struct(
            {
                "query": transform["query"],
                "destination_table_name_template": transform.get(
                    "destination_table", ""
                ),
                "write_disposition": transform.get(
                    "write_disposition", "WRITE_TRUNCATE"
                ),
            }
        ),
    )

    created = client.create_transfer_config(
        request={
            "parent": parent,
            "transfer_config": transfer_config,
        }
    )

    print(f"Created scheduled query: {created.name}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit(
            "Usage: python scripts/deploy_scheduled_query.py configs/customers.yaml"
        )

    deploy_scheduled_query(sys.argv[1])