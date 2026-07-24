"""Import a local image directory into an expert dataset through the admin API.

Usage:
  python scripts/import_expert_dataset.py --base-url http://127.0.0.1:8000
    --dataset-id DATASET_ID --token ACCESS_TOKEN --api-key API_KEY
    --directory ./raw_images --metadata-csv ./metadata.csv

The mobile application is intentionally not used for bulk import. The script keeps
the original filename and optional CSV metadata attached to every dataset item.
"""

import argparse
import csv
import mimetypes
from pathlib import Path

import httpx


SUPPORTED_SUFFIXES = {".jpg", ".jpeg", ".png", ".webp", ".tif", ".tiff", ".heic", ".heif"}


def read_metadata(path: Path | None) -> dict[str, dict[str, str]]:
    if not path:
        return {}
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = csv.DictReader(handle)
        return {
            str(row.get("filename") or "").strip(): row
            for row in rows
            if str(row.get("filename") or "").strip()
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import massif du dataset expert")
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--directory", required=True, type=Path)
    parser.add_argument("--metadata-csv", type=Path)
    parser.add_argument("--token", required=True, help="Bearer token d’un compte admin")
    parser.add_argument("--api-key", required=True)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--continue-on-error", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.directory.is_dir():
        raise SystemExit(f"Dossier introuvable: {args.directory}")

    metadata = read_metadata(args.metadata_csv)
    files = sorted(
        path for path in args.directory.rglob("*")
        if path.is_file() and path.suffix.lower() in SUPPORTED_SUFFIXES
    )
    if not files:
        raise SystemExit("Aucune image supportée trouvée.")

    headers = {
        "Authorization": f"Bearer {args.token}",
        "X-API-Key": args.api_key,
    }
    endpoint = f"{args.base_url.rstrip('/')}/api/v1/expert/datasets/{args.dataset_id}/images"
    imported = 0
    failed = 0

    with httpx.Client(headers=headers, timeout=args.timeout) as client:
        for index, image_path in enumerate(files, start=1):
            row = metadata.get(image_path.name, {})
            content_type = mimetypes.guess_type(image_path.name)[0] or "image/jpeg"
            form = {
                key: value
                for key, value in {
                    "specimen_id": row.get("specimen_id"),
                    "source_type": row.get("source_type"),
                    "origin": row.get("origin"),
                    "capture_type": row.get("capture_type"),
                    "has_interior_cut": row.get("has_interior_cut"),
                }.items()
                if value
            }
            try:
                response = client.post(
                    endpoint,
                    data=form,
                    files={"image": (image_path.name, image_path.read_bytes(), content_type)},
                )
                response.raise_for_status()
                imported += 1
                print(f"[{index}/{len(files)}] importée: {image_path.name}")
            except Exception as exc:
                failed += 1
                print(f"[{index}/{len(files)}] échec: {image_path.name} — {exc}")
                if not args.continue_on_error:
                    raise

    print(f"Import terminé: {imported} succès, {failed} échecs.")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
