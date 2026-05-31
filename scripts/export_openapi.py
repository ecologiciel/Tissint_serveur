import json
import os
import sys
from pathlib import Path

os.environ.setdefault("TINSSIT_SKIP_MODEL_LOAD", "1")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from main import app


def main():
    output_path = Path(__file__).resolve().parents[1] / "docs" / "openapi.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(app.openapi(), indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(f"OpenAPI schema exported to {output_path}")


if __name__ == "__main__":
    main()
