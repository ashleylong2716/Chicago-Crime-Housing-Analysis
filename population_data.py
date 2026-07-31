"""Compatibility wrapper for the canonical population fetch script."""

from pathlib import Path
import runpy
import subprocess
import sys


def ensure_dependency(module_name: str, package_name: str) -> None:
    try:
        __import__(module_name)
    except ModuleNotFoundError:
        print(f"Installing missing Python package: {package_name}")
        subprocess.check_call([sys.executable, "-m", "pip", "install", package_name])


ensure_dependency("pandas", "pandas")
ensure_dependency("requests", "requests")


if __name__ == "__main__":
    script_path = Path(__file__).with_name("fetch_population_data.py")
    runpy.run_path(str(script_path), run_name="__main__")
