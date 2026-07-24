"""Durable worker for dataset normalization and Vision Trio inference.

The API can still run a small background task for local development. In staging and
production this process is the durable consumer: work remains represented by the
dataset item status and can be retried after a process restart.
"""

import argparse
import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone

from sqlalchemy import select, update

from database import AsyncSessionLocal, DatasetItemModel
from main import _infer_dataset_item, vision_pipeline


LOGGER = logging.getLogger("tissint.expert_worker")


async def claimable_item_ids(limit: int) -> list[str]:
    async with AsyncSessionLocal() as db:
        stale_before = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(
            seconds=float(os.getenv("DATASET_WORKER_STALE_SECONDS", "900"))
        )
        await db.execute(
            update(DatasetItemModel)
            .where(
                DatasetItemModel.status == "processing_inference",
                DatasetItemModel.updated_at < stale_before,
            )
            .values(status="inference_pending", updated_at=datetime.now(timezone.utc).replace(tzinfo=None))
        )
        await db.commit()
        result = await db.execute(
            select(DatasetItemModel.id)
            .where(DatasetItemModel.status.in_({"inference_pending", "imported"}))
            .order_by(DatasetItemModel.created_at.asc())
            .limit(limit)
        )
        return list(result.scalars().all())


async def run_once(batch_size: int) -> int:
    item_ids = await claimable_item_ids(batch_size)
    for item_id in item_ids:
        try:
            await _infer_dataset_item(item_id)
            LOGGER.info("dataset item processed: %s", item_id)
        except Exception:
            LOGGER.exception("dataset item failed: %s", item_id)
    return len(item_ids)


async def run_worker(batch_size: int, interval_seconds: float, once: bool) -> None:
    if vision_pipeline is None:
        raise RuntimeError("Vision Trio est désactivé; retirez TINSSIT_SKIP_MODEL_LOAD=1.")
    while True:
        processed = await run_once(batch_size)
        if once:
            return
        if processed == 0:
            await asyncio.sleep(interval_seconds)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Worker dataset expert Vision Trio")
    parser.add_argument("--batch-size", type=int, default=int(os.getenv("DATASET_WORKER_BATCH_SIZE", "4")))
    parser.add_argument("--interval", type=float, default=float(os.getenv("DATASET_WORKER_INTERVAL_SECONDS", "2")))
    parser.add_argument("--once", action="store_true", help="Traiter la file disponible puis quitter")
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    arguments = parse_args()
    asyncio.run(run_worker(arguments.batch_size, arguments.interval, arguments.once))
