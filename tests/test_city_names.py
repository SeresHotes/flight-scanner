"""core/city_names.json — имена городов выдачи (scripts/build_city_names.py). Файл
обязан быть в git: `*.json` в .gitignore однажды молча не пустил его в образ, и
выдача осталась без справочника (PR #108)."""
import json
import subprocess
from pathlib import Path

from core.segments import make_city_lookup

ROOT = Path(__file__).resolve().parent.parent


def test_city_names_file_is_tracked_by_git():
    subprocess.run(["git", "ls-files", "--error-unmatch", "core/city_names.json"], cwd=ROOT,
                   check=True, capture_output=True)


def test_lookup_uses_city_names_file():
    names = json.loads((ROOT / "core" / "city_names.json").read_text(encoding="utf-8"))
    code, (name, country) = next((c, v) for c, v in names.items() if v[1])
    info = make_city_lookup()(code)
    assert info["city"] == name and info["country"] == country and info["flag"]


def test_city_names_loaded_once_per_process():
    from core import segments
    segments.load_city_names()
    assert segments._city_names_network() is segments._city_names_network()
