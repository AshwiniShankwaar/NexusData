"""
nexus_data/cli.py
Entry point for the `nexusdata` console command.
Delegates to nexus_cli.run_cli() so both `python nexus_cli.py` and
the installed `nexusdata` command behave identically.
"""
from __future__ import annotations


def main() -> None:  # pragma: no cover
    # Import here so startup errors surface with a clean traceback
    from nexus_cli import run_cli
    run_cli()


if __name__ == "__main__":
    main()
