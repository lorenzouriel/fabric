"""Structural checks on Fabric item definitions so a bad edit fails locally, not at deploy."""

import json
import re

from conftest import REPO_ROOT

GUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
MARKER = re.compile(r"^# (CELL|PARAMETERS CELL|MARKDOWN|METADATA) \*{20}$", re.M)


def _platform_files():
    return [p for p in REPO_ROOT.rglob(".platform") if ".terraform" not in p.parts]


def _platforms():
    return {p.parent: json.loads(p.read_text(encoding="utf-8")) for p in _platform_files()}


def test_platform_files_are_valid_and_unique():
    platforms = _platforms()
    assert platforms, "no Fabric items found"
    logical_ids, names = set(), set()
    for folder, data in platforms.items():
        item_type = data["metadata"]["type"]
        name = data["metadata"]["displayName"]
        assert folder.name == f"{name}.{item_type}", f"{folder} must be named <displayName>.<type>"
        assert GUID.match(data["config"]["logicalId"]), folder
        assert data["config"]["logicalId"] not in logical_ids, f"duplicate logicalId in {folder}"
        assert name not in names, f"duplicate displayName {name}"
        logical_ids.add(data["config"]["logicalId"])
        names.add(name)


ALLOWED_FILES = {
    "Notebook": {".platform", "notebook-content.py"},
    "DataPipeline": {".platform", "pipeline-content.json"},
}


def test_item_folders_contain_only_definition_files():
    for folder, data in _platforms().items():
        allowed = ALLOWED_FILES[data["metadata"]["type"]]
        present = {str(p.relative_to(folder)) for p in folder.rglob("*") if p.is_file()}
        assert present <= allowed, f"{folder}: unexpected files {sorted(present - allowed)} would be uploaded by fabric-cicd"


def test_notebook_names_follow_convention():
    for folder, data in _platforms().items():
        if data["metadata"]["type"] == "Notebook":
            name = data["metadata"]["displayName"]
            layer = folder.parent.name
            assert name.startswith(f"nb_{layer}_"), f"{name} lives in notebooks/{layer} but is not nb_{layer}_*"


def test_notebook_source_format():
    for folder, data in _platforms().items():
        if data["metadata"]["type"] != "Notebook":
            continue
        text = (folder / "notebook-content.py").read_text(encoding="utf-8")
        assert text.startswith("# Fabric notebook source\n"), folder
        markers = MARKER.findall(text)
        assert markers[0] == "METADATA", f"{folder}: first block must be kernel METADATA"
        for i, marker in enumerate(markers):
            if marker in ("CELL", "PARAMETERS CELL"):
                assert i + 1 < len(markers) and markers[i + 1] == "METADATA", (
                    f"{folder}: every code cell must be followed by a METADATA block"
                )
        assert markers.count("PARAMETERS CELL") <= 1, f"{folder}: at most one parameters cell"
        # Fabric stores magics as bare lines (verified via getDefinition); a "# MAGIC" prefix
        # is mis-parsed into a duplicated command such as "%run x%run x".
        assert "# MAGIC" not in text, f"{folder}: write magics as bare lines, e.g. '%run nb_framework_common'"


def test_pipeline_notebook_references_resolve():
    platforms = _platforms()
    notebooks = {d["config"]["logicalId"]: d["metadata"]["displayName"] for d in platforms.values() if d["metadata"]["type"] == "Notebook"}
    for folder, data in platforms.items():
        if data["metadata"]["type"] != "DataPipeline":
            continue
        content = (folder / "pipeline-content.json").read_text(encoding="utf-8")
        json.loads(content)
        refs = re.findall(r'"notebookId":\s*"([^"]+)"', content)
        assert refs, f"{folder}: no notebook activities"
        for ref in refs:
            assert ref in notebooks, f"{folder}: notebookId {ref} is not a notebook logicalId in this repo"
        for ws in re.findall(r'"workspaceId":\s*"([^"]+)"', content):
            assert ws == "00000000-0000-0000-0000-000000000000", f"{folder}: use the default workspace id so fabric-cicd re-points it"
