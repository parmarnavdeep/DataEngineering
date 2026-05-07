import sys
import yaml
from google.cloud import secretmanager
from google.cloud import bigquery_datatransfer_v1
from google.protobuf import field_mask_pb2
from google.protobuf.struct_pb2 import Struct


def read_secret(project_id: str, secret_id: str) -> str:
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("utf-8")


def to_struct(data: dict) -> Struct:
    s = Struct()
    s.update(data)
    return s


def find_existing_config(client, parent: str, display_name: str):
    configs = client.list_transfer_configs(parent=parent)
    for cfg in configs:
        if cfg.display_name == display_name:
            return cfg
    return None


def build_params(cfg: dict) -> dict:
    project_id = cfg["project_id"]
    source = cfg["source"]
    transfer = cfg["transfer"]

    username = read_secret(project_id, source["username_secret"])
    password = read_secret(project_id, source["password_secret"])

    assets = [a["table"] for a in cfg["assets"]]

    params = {
        "assets": assets,
        "connector.database": source["database"],
        "connector.endpoint.host": source["host"],
        "connector.endpoint.port": source["port"],
        "connector.authentication.username": username,
        "connector.authentication.password": password,
        "connector.legacyMapping": source["legacy_mapping"],
        "connector.tls.mode": source["tls_mode"],
        "ingestionType": transfer["ingestion_type"],
    }

    if source.get("network_attachment"):
        params["connector.networkAttachment"] = source["network_attachment"]

    if source.get("trusted_server_certificate"):
        params["connector.tls.trustedServerCertificate"] = source[
            "trusted_server_certificate"
        ]

    if transfer["ingestion_type"] == "incremental":
        params["writeMode"] = transfer["write_mode"]
        params["watermarkColumns"] = [
            a["watermark_column"] for a in cfg["assets"]
        ]

        if transfer["write_mode"] == "WRITE_MODE_UPSERT":
            params["primaryKeys"] = [
                a["primary_keys"] for a in cfg["assets"]
            ]

    return params


def create_or_update_transfer(config_path: str) -> None:
    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f)

    if not cfg.get("enabled", True):
        print(f"Config disabled, skipping: {config_path}")
        return

    project_id = cfg["project_id"]
    location = cfg["location"]
    display_name = cfg["job_name"]
    dataset = cfg["target_dataset"]

    client = bigquery_datatransfer_v1.DataTransferServiceClient()
    parent = f"projects/{project_id}/locations/{location}"

    params = build_params(cfg)

    transfer_config = bigquery_datatransfer_v1.TransferConfig(
        display_name=display_name,
        data_source_id="sqlserver",
        destination_dataset_id=dataset,
        schedule=cfg["transfer"]["schedule"],
        params=to_struct(params),
    )

    existing = find_existing_config(client, parent, display_name)

    if existing:
        transfer_config.name = existing.name

        update_mask = field_mask_pb2.FieldMask(
            paths=[
                "display_name",
                "destination_dataset_id",
                "schedule",
                "params",
            ]
        )

        updated = client.update_transfer_config(
            request={
                "transfer_config": transfer_config,
                "update_mask": update_mask,
            }
        )

        print(f"Updated DTS transfer: {updated.name}")

    else:
        created = client.create_transfer_config(
            request={
                "parent": parent,
                "transfer_config": transfer_config,
            }
        )

        print(f"Created DTS transfer: {created.name}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit(
            "Usage: python scripts/deploy_dts_transfer.py configs/customers.yaml"
        )

    create_or_update_transfer(sys.argv[1])