import uuid
import asyncio
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy import update, select
from src.models.pipeline import Pipeline, PipelineRun
from src.config import settings
from src.pipeline.watermark import get_watermark, advance_watermark
from src.utils.logging import get_logger

logger = get_logger(__name__)

engine = create_async_engine(settings.database_url)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False)

# BUG DF-09: Spark checkpoint directory is /tmp.
# Spark uses checkpoints to recover streaming state after a failure or restart.
# /tmp is ephemeral on containers — it is wiped on every restart, scale-down,
# or deploy. When the container restarts, Spark has no checkpoint to resume from
# and reprocesses the entire Kafka topic from the beginning (or from the
# auto.offset.reset position). For a topic with 30 days of retention this means
# reprocessing billions of records, taking hours and writing massive duplicates.
# Fix: set checkpoint to a persistent volume or S3 path.

# BUG DF-15: No distributed lock on pipeline execution.
# The scheduler triggers run_pipeline() every 15 minutes. If the previous run
# takes longer than 15 minutes (large batch, slow Spark job), a second run starts
# concurrently. Both runs read the same watermark, process the same records, and
# write duplicate rows to the warehouse. The is_running flag in the DB is checked
# but the check-then-set is not atomic — two concurrent API requests or scheduler
# ticks can both read is_running=False before either sets it to True.
# Fix: use a database-level advisory lock (SELECT pg_try_advisory_lock(pipeline_id))
# or a Redis distributed lock (SET lock NX EX ttl) before starting a run.

CHECKPOINT_DIR = settings.checkpoint_dir

async def run_pipeline(pipeline_id: str) -> str:
    async with SessionLocal() as session:
        result = await session.execute(
            select(Pipeline).where(Pipeline.id == pipeline_id)
        )
        pipeline = result.scalar_one_or_none()
        if not pipeline:
            raise ValueError(f"Pipeline {pipeline_id} not found")

        # BUG DF-15: non-atomic check-then-set — race condition between two concurrent callers
        if pipeline.is_running:
            logger.warning("pipeline_already_running", pipeline_id=pipeline_id)
            return "already_running"

        # BUG DF-15: window between this read and the next write allows two runners through
        await session.execute(
            update(Pipeline)
            .where(Pipeline.id == pipeline_id)
            .values(is_running=True, last_run_at=datetime.utcnow())
        )
        await session.commit()

    run_id = str(uuid.uuid4())
    async with SessionLocal() as session:
        run = PipelineRun(
            id=run_id,
            pipeline_id=pipeline_id,
            started_at=datetime.utcnow(),
            status="running",
        )
        session.add(run)
        await session.commit()

    try:
        watermark = get_watermark(pipeline_id)
        logger.info("pipeline_run_started", pipeline_id=pipeline_id, run_id=run_id, watermark=watermark.isoformat())

        # Spark job execution (simplified — real impl calls run_transactions_job)
        await asyncio.sleep(0)

        new_watermark = advance_watermark(pipeline_id)

        async with SessionLocal() as session:
            await session.execute(
                update(PipelineRun)
                .where(PipelineRun.id == run_id)
                .values(
                    status="completed",
                    finished_at=datetime.utcnow(),
                )
            )
            await session.commit()

        logger.info("pipeline_run_completed", pipeline_id=pipeline_id, run_id=run_id)
        return run_id

    except Exception as e:
        logger.error("pipeline_run_failed", pipeline_id=pipeline_id, run_id=run_id, error=str(e))
        async with SessionLocal() as session:
            await session.execute(
                update(PipelineRun)
                .where(PipelineRun.id == run_id)
                .values(status="failed", error_message=str(e), finished_at=datetime.utcnow())
            )
            await session.commit()
        raise

    finally:
        async with SessionLocal() as session:
            await session.execute(
                update(Pipeline)
                .where(Pipeline.id == pipeline_id)
                .values(is_running=False)
            )
            await session.commit()
