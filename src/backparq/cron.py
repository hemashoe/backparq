import shutil
import subprocess
from pathlib import Path

from backparq.config import BackparqConfig


def install_cron(config: BackparqConfig, config_path: Path) -> None:
    if not config.cron.enabled:
        return

    if not config.cron.schedule:
        raise RuntimeError("Cron enabled but schedule missing.")

    if shutil.which("crontab") is None:
        raise RuntimeError("crontab command not available to install cron entry.")

    command = config.cron.command or f"backparq apply --config {config_path}"
    marker = f"# backparq:{config_path}"
    new_entry = f"{config.cron.schedule} {command} {marker}"

    existing = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
    current = existing.stdout if existing.returncode == 0 else ""
    lines = [line for line in current.splitlines() if marker not in line]
    lines.append(new_entry)

    updated = "\n".join(lines).strip() + "\n"
    subprocess.run(["crontab", "-"], input=updated, text=True, check=True)
