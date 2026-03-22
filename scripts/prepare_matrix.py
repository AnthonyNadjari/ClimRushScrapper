"""Prepare the GitHub Actions matrix from segments.json."""

import base64
import json
import os
import sys


def main():
    # Check for workflow_dispatch config input
    config_b64 = os.environ.get("INPUT_CONFIG", "")
    if config_b64:
        try:
            config = json.loads(base64.b64decode(config_b64))
        except Exception:
            config = json.loads(config_b64)
    else:
        config_path = os.environ.get("CONFIG_PATH", "config/segments.json")
        with open(config_path, "r", encoding="utf-8") as f:
            config = json.load(f)

    segments = config.get("segments", [])
    enabled = [s for s in segments if s.get("enabled", True)]

    matrix = {"include": [{"index": i, "name": s["name"]} for i, s in enumerate(enabled)]}

    # Output for GitHub Actions
    output_file = os.environ.get("GITHUB_OUTPUT", "")
    if output_file:
        with open(output_file, "a") as f:
            f.write(f"matrix={json.dumps(matrix)}\n")
            f.write(f"count={len(enabled)}\n")
    else:
        # Local testing
        print(json.dumps(matrix, indent=2))


if __name__ == "__main__":
    main()
