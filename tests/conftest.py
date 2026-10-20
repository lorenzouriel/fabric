import importlib.util
import sys
from pathlib import Path

import pytest

# Importing notebook-content.py must not leave __pycache__ inside Fabric item folders:
# fabric-cicd uploads every file in the folder and Fabric rejects the .pyc parts.
sys.dont_write_bytecode = True

REPO_ROOT = Path(__file__).resolve().parents[1]


def load_notebook(relative_dir: str, module_name: str):
    """Import a Fabric notebook-content.py as a module. Only for notebooks with plain-Python
    top-level code (no `%run` magics, no Spark calls): nb_framework_common, nb_framework_config."""
    path = REPO_ROOT / relative_dir / "notebook-content.py"
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="session")
def common():
    return load_notebook("notebooks/framework/nb_framework_common.Notebook", "nb_framework_common")


@pytest.fixture(scope="session")
def config_module():
    return load_notebook("notebooks/framework/nb_framework_config.Notebook", "nb_framework_config")
