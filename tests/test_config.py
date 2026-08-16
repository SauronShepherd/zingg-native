import pytest

from zingg_native import NativeConfig


def test_config_validates_mode_and_timeout():
    assert NativeConfig().mode == "SAFE"
    with pytest.raises(ValueError):
        NativeConfig(mode="unsafe")
    with pytest.raises(ValueError):
        NativeConfig(operation_timeout_seconds=0)
