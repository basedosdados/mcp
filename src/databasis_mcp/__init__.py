"""MCP server exposing the Base dos Dados API as tools for AI agents."""

from importlib.metadata import version

__version__ = version("databasis-mcp")

__all__ = ["__version__", "mcp"]


def __getattr__(name: str):
    # Lazy so `import databasis_mcp` stays cheap; accessing `mcp` imports
    # the server module, which registers every tool.
    if name == "mcp":
        from .server import mcp

        return mcp
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
