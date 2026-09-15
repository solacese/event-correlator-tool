"""Solace Agent Mesh event correlator toolset."""


def cli() -> None:
    from .__main__ import cli as run

    run()


__all__ = ["cli"]
