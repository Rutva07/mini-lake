from pathlib import Path

import pytest

from minilake.config import PipelineConfig
from minilake.errors import ConfigurationError


def test_config_rejects_unknown_partition(tmp_path: Path) -> None:
    source = tmp_path / "events.csv"
    source.write_text("event_id\n", encoding="utf-8")
    config = PipelineConfig(source, tmp_path / "out", partition_by=("unknown",))
    with pytest.raises(ConfigurationError, match="Unsupported partition"):
        config.validate()


def test_config_rejects_missing_input(tmp_path: Path) -> None:
    with pytest.raises(ConfigurationError, match="does not exist"):
        PipelineConfig(tmp_path / "missing", tmp_path / "out").validate()

