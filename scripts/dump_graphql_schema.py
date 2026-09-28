#!/usr/bin/env python3
"""Снимает схему GraphQL Data API Travelpayouts introspection-запросом и печатает SDL.

    poetry run python scripts/dump_graphql_schema.py > docs/travelpayouts/graphql-schema.graphql

Нужен TRAVELPAYOUTS_TOKEN (без токена источник отвечает 401). Один запрос.
"""
import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core import graphql_api  # noqa: E402

INTROSPECTION = """query { __schema { types { kind name description
  fields(includeDeprecated: true) { name description isDeprecated deprecationReason
    args { name description type { ...T } defaultValue } type { ...T } }
  inputFields { name description type { ...T } defaultValue }
  enumValues(includeDeprecated: true) { name description }
} } }
fragment T on __Type { kind name ofType { kind name ofType { kind name ofType { kind name ofType { kind name } } } } }"""

BUILTIN_SCALARS = {"Boolean", "String", "Int", "Float", "ID"}
KEYWORDS = {"OBJECT": "type", "INPUT_OBJECT": "input", "INTERFACE": "interface", "UNION": "union"}


def type_ref(t):
    if t["kind"] == "NON_NULL":
        return type_ref(t["ofType"]) + "!"
    if t["kind"] == "LIST":
        return "[" + type_ref(t["ofType"]) + "]"
    return t["name"]


def doc(text, indent=""):
    return f'{indent}"""{text}"""\n' if text else ""


def field_line(f):
    args = ""
    if f.get("args"):
        args = "(" + ", ".join(
            a["name"] + ": " + type_ref(a["type"]) + (f" = {a['defaultValue']}" if a["defaultValue"] else "")
            for a in f["args"]) + ")"
    default = f" = {f['defaultValue']}" if f.get("defaultValue") else ""
    deprecated = f' @deprecated(reason: "{f["deprecationReason"]}")' if f.get("isDeprecated") else ""
    return doc(f["description"], "  ") + f"  {f['name']}{args}: {type_ref(f['type'])}{default}{deprecated}\n"


def to_sdl(types):
    out = []
    for t in sorted(types, key=lambda x: (x["name"] != "Query", x["name"])):
        name, kind = t["name"], t["kind"]
        if name.startswith("__") or (kind == "SCALAR" and name in BUILTIN_SCALARS):
            continue
        head = doc(t["description"])
        if kind == "SCALAR":
            out.append(head + f"scalar {name}\n")
        elif kind == "ENUM":
            values = "".join(doc(v["description"], "  ") + f"  {v['name']}\n" for v in t["enumValues"])
            out.append(head + f"enum {name} {{\n{values}}}\n")
        else:
            fields = t["inputFields"] if kind == "INPUT_OBJECT" else t["fields"]
            body = "".join(field_line(f) for f in fields or [])
            out.append(head + f"{KEYWORDS[kind]} {name} {{\n{body}}}\n")
    return "\n".join(out)


def main():
    resp = requests.post(graphql_api.GRAPHQL_URL, json={"query": INTROSPECTION},
                         headers={"X-Access-Token": graphql_api.require_token()},
                         timeout=graphql_api.REQUEST_TIMEOUT)
    resp.raise_for_status()
    print(to_sdl(resp.json()["data"]["__schema"]["types"]), end="")


if __name__ == "__main__":
    main()
