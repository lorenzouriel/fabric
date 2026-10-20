import pytest


def test_all_config_rows_validate(common, config_module):
    configs = [common.IngestionConfig.from_dict(row) for row in config_module.INGESTION_CONFIG]
    datasets = [c.dataset for c in configs]
    assert len(datasets) == len(set(datasets)), "duplicate source_system.entity"


def test_landing_path_matches_dataset(common, config_module):
    for row in config_module.INGESTION_CONFIG:
        cfg = common.IngestionConfig.from_dict(row)
        assert cfg.landing_path == f"{cfg.source_system}/{cfg.entity}"


def test_round_trip_through_table_row(common, config_module):
    for row in config_module.INGESTION_CONFIG:
        cfg = common.IngestionConfig.from_dict(row)
        assert common.IngestionConfig.from_dict(cfg.to_row()) == cfg


def test_table_schema_covers_every_config_field(common):
    from dataclasses import fields

    ddl = common.CONTROL_TABLE_SCHEMAS[common.TABLE_CONFIG]
    columns = {part.strip().split(" ")[0] for part in ddl.split(",")}
    assert {f.name for f in fields(common.IngestionConfig)} <= columns


@pytest.mark.parametrize(
    "override, message",
    [
        ({"load_type": "incremental", "request_strategy": "single"}, "date_window"),
        ({"source_type": "FTP"}, "source_type"),
        ({"base_url": "http://insecure"}, "https"),
        ({"landing_path": "../escape"}, "landing_path"),
        ({"authentication_type": "api_key_header"}, "auth_header"),
    ],
)
def test_invalid_config_is_rejected(common, override, message):
    row = {
        "source_system": "s", "entity": "e", "source_type": "REST",
        "base_url": "https://x", "endpoint": "/e", "load_type": "full", "landing_path": "s/e",
    }
    row.update(override)
    with pytest.raises(common.ConfigError, match=message):
        common.IngestionConfig.from_dict(row)
