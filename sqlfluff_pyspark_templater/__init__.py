"""Defines the hook endpoints for the pyspark templater plugin."""

from typing import Any
from sqlfluff.core.plugin import hookimpl
from sqlfluff.core.config import load_config_resource
from sqlfluff_pyspark_templater.templater import PySparkTemplater


@hookimpl
def get_templaters():
    """Get templaters."""
    return [PySparkTemplater]


@hookimpl
def get_rules():
    """Get plugin rules."""
    return []


@hookimpl
def load_default_config() -> dict[str, Any]:
    """Loads the default configuration for the plugin."""
    return {}


@hookimpl
def get_configs_info() -> dict[str, dict[str, Any]]:
    """Get rule config validations and descriptions."""
    return {}