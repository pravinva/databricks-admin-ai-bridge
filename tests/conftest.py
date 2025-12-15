"""
Pytest configuration and fixtures.

This file provides test configuration including a ToolSpec compatibility shim
for databricks.agents which may not be available or may not have ToolSpec in all versions.
"""

import sys
from typing import Callable, Any, Dict, Optional


class ToolSpec:
    """
    Mock/shim for databricks.agents.ToolSpec.

    This provides compatibility when ToolSpec is not available in the installed
    version of databricks-agents.
    """

    def __init__(
        self,
        name: Optional[str] = None,
        description: Optional[str] = None,
        func: Optional[Callable] = None,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        self.name = name
        self.description = description
        self.func = func
        self.parameters = parameters or {}

    @classmethod
    def python(
        cls,
        func: Callable,
        name: str,
        description: str,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> "ToolSpec":
        """
        Create a ToolSpec from a Python function.

        This is the primary factory method used by the agent tools.
        """
        return cls(name=name, description=description, func=func, parameters=parameters or {})


def pytest_configure(config):
    """
    Pytest hook to configure test environment.

    Injects ToolSpec into databricks.agents if it's not already present.
    """
    try:
        import databricks.agents

        # Check if ToolSpec already exists
        if not hasattr(databricks.agents, 'ToolSpec'):
            # Inject our shim
            databricks.agents.ToolSpec = ToolSpec
            print("INFO: Injected ToolSpec shim into databricks.agents")
    except ImportError:
        # databricks.agents not installed, tests that need it will skip
        pass
