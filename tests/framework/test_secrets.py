"""Secret providers: Key Vault (production) and the lakehouse-file fallback (dev)."""

import pytest


class FakeReader:
    workspace = "ws"
    lakehouse = "lh_senado_control"

    def __init__(self, files):
        self.files = files
        self.reads = []

    def read_bytes(self, path):
        self.reads.append(path)
        if path not in self.files:
            raise FileNotFoundError(path)
        return self.files[path]


def _path(common, name):
    return common.onelake_path("ws", "lh_senado_control", f"Files/_secrets/{name}")


def test_lakehouse_secret_is_read_and_stripped(common):
    reader = FakeReader({_path(common, "transparencia-api-key"): b"\xef\xbb\xbfabc123\r\n"})
    assert common.lakehouse_secret_provider(reader)("transparencia-api-key") == "abc123"


def test_lakehouse_secret_is_read_lazily(common):
    reader = FakeReader({})
    common.lakehouse_secret_provider(reader)
    assert reader.reads == []


def test_missing_lakehouse_secret_says_where_to_upload(common):
    provider = common.lakehouse_secret_provider(FakeReader({}))
    with pytest.raises(common.ConfigError, match="Files/_secrets/transparencia-api-key"):
        provider("transparencia-api-key")


def test_empty_lakehouse_secret_fails(common):
    provider = common.lakehouse_secret_provider(FakeReader({_path(common, "k"): b"  \n"}))
    with pytest.raises(common.ConfigError, match="empty"):
        provider("k")


def test_datasets_without_auth_never_read_a_secret(common, config_module):
    # sources mixing public and keyed datasets must not need the secret for the public ones
    for row in config_module.INGESTION_CONFIG:
        cfg = common.IngestionConfig.from_dict(row)
        if cfg.authentication_type == "none":
            assert common._auth_headers(cfg, lambda name: pytest.fail(f"{cfg.dataset} read a secret")) == {}
