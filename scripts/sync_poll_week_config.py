"""
Sync config/poll_week_windows.json into the two places that keep their own
generated copy of it: build_polls_games_joined.py's _DEFAULT_POLL_WEEK_WINDOWS
fallback dict, and ANALYTICS_TABLE_README.md's Poll Week Reference table.

Usage:
    python scripts/sync_poll_week_config.py          # regenerate both files
    python scripts/sync_poll_week_config.py --check  # exit 1 if either is
                                                       # out of sync (CI uses
                                                       # this -- it never
                                                       # auto-commits code or
                                                       # doc changes, only
                                                       # data/, so drift has
                                                       # to fail loudly here
                                                       # instead)

Each target file marks its generated block with a matching pair of
"BEGIN GENERATED: <name>" / "END GENERATED: <name>" comments; only the text
between them is touched.

Note: config/poll_week_windows.json has no field for the short narrative
description some weeks had before this script existed (e.g. '3/9' was
"Conference tournaments begin", '3/16' was "NCAA Tournament Round 1/2").
Those aren't derivable from the JSON, so the generated "Represents" column
is deliberately simpler: the first window is "Preseason poll", the last is
"Season-end poll", everything else is "Weekly poll".
"""
import argparse
import json
import re
import sys
from pathlib import Path

CONFIG_PATH = Path("config") / "poll_week_windows.json"

BUILD_SCRIPT_PATH = Path("scripts") / "process" / "build_polls_games_joined.py"
BUILD_SCRIPT_MARKER = "poll_week_windows"

README_PATH = Path("ANALYTICS_TABLE_README.md")
README_MARKER = "poll_week_reference"


def load_windows() -> dict:
    with open(CONFIG_PATH) as f:
        config = json.load(f)
    windows = config.get("windows")
    if not windows:
        raise KeyError(f"'windows' key missing or empty in {CONFIG_PATH}")
    return windows


def render_default_windows_dict(windows: dict) -> str:
    labels = list(windows.keys())
    field_width = max(len(f"'{label}':") for label in labels) + 1

    lines = ["_DEFAULT_POLL_WEEK_WINDOWS = {"]
    for label in labels:
        start, end = windows[label]
        field = f"'{label}':".ljust(field_width)
        lines.append(f"    {field}('{start}', '{end}'),")
    lines.append("}")
    return "\n".join(lines)


def render_readme_table(windows: dict) -> str:
    labels = list(windows.keys())
    last_idx = len(labels) - 1

    lines = [
        "| `poll_week` | `week_number` | Represents | Game date window |",
        "|-------------|----------------|------------|-------------------|",
    ]
    for i, label in enumerate(labels):
        start, end = windows[label]
        if i == 0:
            represents = "Preseason poll"
        elif i == last_idx:
            represents = "Season-end poll"
        else:
            represents = "Weekly poll"
        lines.append(f"| `{label}` | {i} | {represents} | {start} – {end} |")
    return "\n".join(lines)


def replace_marked_block(text: str, marker: str, new_block: str, comment_style: str) -> str:
    if comment_style == "python":
        begin = f"# BEGIN GENERATED: {marker}"
        end = f"# END GENERATED: {marker}"
    elif comment_style == "markdown":
        begin = f"<!-- BEGIN GENERATED: {marker} -->"
        end = f"<!-- END GENERATED: {marker} -->"
    else:
        raise ValueError(comment_style)

    pattern = re.compile(
        re.escape(begin) + r"\n.*?\n" + re.escape(end), re.DOTALL
    )
    if not pattern.search(text):
        raise ValueError(
            f"Could not find '{begin}' ... '{end}' markers to replace. "
            f"Has the file been edited to remove them?"
        )
    replacement = f"{begin}\n{new_block}\n{end}"
    return pattern.sub(replacement, text, count=1)


def sync_file(path: Path, marker: str, new_block: str, comment_style: str, check: bool) -> bool:
    """Returns True if the file was (or, in --check mode, would be) changed."""
    original = path.read_text()
    updated = replace_marked_block(original, marker, new_block, comment_style)

    if updated == original:
        print(f"  OK: {path} already in sync")
        return False

    if check:
        print(f"  OUT OF SYNC: {path} does not match {CONFIG_PATH}")
        return True

    path.write_text(updated)
    print(f"  UPDATED: {path}")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check", action="store_true",
        help="Don't write anything; exit 1 if either generated file is out of sync.",
    )
    args = parser.parse_args()

    windows = load_windows()
    default_windows_block = render_default_windows_dict(windows)
    readme_table_block = render_readme_table(windows)

    print(f"Syncing generated content from {CONFIG_PATH}...")
    changed = False
    changed |= sync_file(
        BUILD_SCRIPT_PATH, BUILD_SCRIPT_MARKER, default_windows_block, "python", args.check
    )
    changed |= sync_file(
        README_PATH, README_MARKER, readme_table_block, "markdown", args.check
    )

    if args.check and changed:
        print(
            f"\n{CONFIG_PATH} has changed since these files were last generated. "
            f"Run 'python scripts/sync_poll_week_config.py' (no --check) and commit the result."
        )
        return 1

    print("Done." if changed else "Nothing to do -- already in sync.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
