"""Command-line interface for the AAS-UNS Bridge."""

from pathlib import Path
from typing import Annotated, Optional

import typer

from aas_uns_bridge import __version__
from aas_uns_bridge.config import BridgeSettings, load_config

app = typer.Typer(
    name="aas-uns-bridge",
    help="AAS-to-UNS Bridge: Ingest AAS content and publish to UNS + Sparkplug B",
    no_args_is_help=True,
)


@app.callback()
def callback() -> None:
    """AAS-UNS Bridge CLI."""
    pass


@app.command()
def run(
    config: Annotated[
        Optional[Path],
        typer.Option("--config", "-c", help="Path to config.yaml"),
    ] = None,
    mappings: Annotated[
        Optional[Path],
        typer.Option("--mappings", "-m", help="Path to mappings.yaml"),
    ] = None,
) -> None:
    """Run the AAS-UNS Bridge daemon."""
    from aas_uns_bridge.daemon import run_daemon

    settings = BridgeSettings()
    if config:
        settings = BridgeSettings(config_file=config, mappings_file=settings.mappings_file)
    if mappings:
        settings = BridgeSettings(config_file=settings.config_file, mappings_file=mappings)

    cfg = load_config(settings)
    run_daemon(cfg, settings.mappings_file)


@app.command()
def validate(
    config: Annotated[
        Optional[Path],
        typer.Option("--config", "-c", help="Path to config.yaml"),
    ] = None,
    mappings: Annotated[
        Optional[Path],
        typer.Option("--mappings", "-m", help="Path to mappings.yaml"),
    ] = None,
) -> None:
    """Validate configuration files without starting the daemon."""
    settings = BridgeSettings()
    if config:
        settings = BridgeSettings(config_file=config, mappings_file=settings.mappings_file)
    if mappings:
        settings = BridgeSettings(config_file=settings.config_file, mappings_file=mappings)

    try:
        cfg = load_config(settings)
        typer.echo(f"Configuration valid: {settings.config_file}")
        typer.echo(f"  MQTT: {cfg.mqtt.host}:{cfg.mqtt.port}")
        typer.echo(f"  UNS enabled: {cfg.uns.enabled}")
        typer.echo(f"  Sparkplug enabled: {cfg.sparkplug.enabled}")
        typer.echo(f"  File watcher: {cfg.file_watcher.enabled}")
        typer.echo(f"  Repo client: {cfg.repo_client.enabled}")
    except Exception as e:
        typer.echo(f"Configuration error: {e}", err=True)
        raise typer.Exit(1)


@app.command()
def version() -> None:
    """Show version information."""
    typer.echo(f"aas-uns-bridge {__version__}")


@app.command()
def status() -> None:
    """Check the status of a running bridge instance."""
    import httpx

    settings = BridgeSettings()
    cfg = load_config(settings)

    try:
        resp = httpx.get(f"http://localhost:{cfg.observability.health_port}/health", timeout=5.0)
        if resp.status_code == 200:
            data = resp.json()
            typer.echo(f"Status: {data.get('status', 'unknown')}")
            typer.echo(f"MQTT connected: {data.get('mqtt_connected', False)}")
        else:
            typer.echo(f"Health check returned {resp.status_code}", err=True)
            raise typer.Exit(1)
    except httpx.ConnectError:
        typer.echo("Bridge is not running or health endpoint unreachable", err=True)
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
