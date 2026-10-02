# Enables `python -m databasis_mcp`, equivalent to the `databasis-mcp` script.
# Useful when the environment's scripts directory is not on PATH.
from .server import main

main()
