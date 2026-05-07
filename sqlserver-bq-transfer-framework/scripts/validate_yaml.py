import sys
import yaml


REQUIRED_TOP_LEVEL = [
    "job_name",
    "enabled",
    "project_id",
    "location",
    "target_dataset",
    "source",
    "transfer",
    "assets",
]


def validate_config(path: str) -> None:
    with open(path, "r") as f:
        cfg = yaml.safe_load(f)

    for field in REQUIRED_TOP_LEVEL:
        if field not in cfg:
            raise ValueError(f"Missing required field: {field}")

    source_required = [
        "host",
        "port",
        "database",
        "username_secret",
        "password_secret",
        "tls_mode",
        "legacy_mapping",
    ]

    for field in source_required:
        if field not in cfg["source"]:
            raise ValueError(f"Missing source field: source.{field}")

    transfer_required = ["ingestion_type", "write_mode", "schedule"]

    for field in transfer_required:
        if field not in cfg["transfer"]:
            raise ValueError(f"Missing transfer field: transfer.{field}")

    if not cfg["assets"]:
        raise ValueError("At least one asset is required")

    ingestion_type = cfg["transfer"]["ingestion_type"]

    for asset in cfg["assets"]:
        if "table" not in asset:
            raise ValueError("Each asset requires table")

        if ingestion_type == "incremental":
            if "watermark_column" not in asset:
                raise ValueError(f"Missing watermark_column for {asset['table']}")
            if cfg["transfer"]["write_mode"] == "WRITE_MODE_UPSERT":
                if "primary_keys" not in asset or not asset["primary_keys"]:
                    raise ValueError(f"Missing primary_keys for {asset['table']}")

    print(f"YAML validation passed: {path}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python scripts/validate_yaml.py configs/customers.yaml")

    validate_config(sys.argv[1])