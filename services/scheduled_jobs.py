import asyncio
from datetime import datetime, timezone, timedelta
from typing import Optional

from sqlalchemy import select, func

from config.database_config import get_db_session
from config.logger import logger
from models import PriceHistoryIntraday, PriceHistoryDaily
from services.data.data_creation import DataCreationService
from features.exchange.exchange_service import get_all_active_exchanges


class ScheduledJobs:
    """Manages periodic background jobs."""

    def __init__(self):
        self._running = False
        self._tasks: dict = {}
        self._data_resolver = None

    def set_resolver(self, resolver):
        """Set the data resolver instance."""
        self._data_resolver = resolver

    async def start(self):
        """Start all scheduled jobs."""
        if self._running:
            return

        self._running = True
        logger.info("Starting scheduled jobs...")

        # A4: Periodic gap detection every 60 minutes
        self._tasks["gap_detection"] = asyncio.create_task(
            self._run_periodic(self.periodic_gap_detection, interval_minutes=60)
        )

        # # A2: Alert for exceeded retry threshold every 30 minutes
        # self._tasks["retry_alert"] = asyncio.create_task(
        #     self._run_periodic(self.check_exceeded_retries, interval_minutes=30)
        # )

        # B3: Volume reconciliation every hour
        self._tasks["volume_reconciliation"] = asyncio.create_task(
            self._run_periodic(self.reconcile_daily_volume, interval_minutes=60)
        )

        # C1: Daily data creation for the next day at 12 AM
        self._tasks["daily_data_creation"] = asyncio.create_task(
            self._run_daily_at(0, 0, self.run_daily_data_creation)
        )

        logger.info(f"Started {len(self._tasks)} scheduled jobs")

    async def stop(self):
        """Stop all scheduled jobs."""
        self._running = False
        for name, task in self._tasks.items():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._tasks.clear()
        logger.info("Stopped all scheduled jobs")

    async def _run_periodic(self, job_func, interval_minutes: int):
        """Run a job periodically."""
        while self._running:
            try:
                await job_func()
            except Exception as e:
                logger.error(f"Error in {job_func.__name__}: {e}", exc_info=True)

            await asyncio.sleep(interval_minutes * 60)

    async def _run_daily_at(self, hour: int, minute: int, job_func):
        """Run a job daily at a specific time."""
        while self._running:
            try:
                now = datetime.now()
                target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
                if target <= now:
                    target += timedelta(days=1)

                wait_seconds = (target - now).total_seconds()
                logger.info(
                    f"Scheduling {job_func.__name__} for {target} (in {wait_seconds:.2f}s)"
                )

                await asyncio.sleep(wait_seconds)

                if self._running:
                    await job_func()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(
                    f"Error in scheduled job {job_func.__name__}: {e}", exc_info=True
                )
                # Avoid tight loop in case of error
                await asyncio.sleep(60)

    async def run_daily_data_creation(self):
        """Creates data records for the next day for all active exchanges."""
        logger.info("📅 Running daily data creation for tomorrow...")
        try:
            # We want to create data for tomorrow
            # If running on 19th, target is 20th
            target_date = datetime.now().date() + timedelta(days=1)

            async for session in get_db_session():
                try:
                    exchange_data = await get_all_active_exchanges(session=session)

                    if not exchange_data:
                        logger.warning("No active exchanges found for data creation.")
                        return

                    data_creation = DataCreationService(session=session)
                    for exchange in exchange_data:
                        data_creation.add_exchange(exchange=exchange)

                    await data_creation.start_data_creation(target_date=target_date)
                    logger.info(
                        f"✅ successfully created future records for {target_date}"
                    )
                finally:
                    # Async context manager mostly handles close, but explicit check if needed.
                    # With 'async for', session is yielded then closed.
                    pass

                # We only need one session pass
                break

        except Exception as e:
            logger.error(f"Error creating future records: {e}", exc_info=True)

    async def periodic_gap_detection(self):
        """A4: Periodically check for gaps and trigger resolution."""
        logger.info("🔄 Running periodic gap detection...")

        if not self._data_resolver:
            logger.warning("Data resolver not set, skipping gap detection")
            return

        try:
            await self._data_resolver.check_and_fill_gaps()
            logger.info("✅ Periodic gap detection completed")
        except Exception as e:
            logger.error(f"Gap detection failed: {e}")

    # async def check_exceeded_retries(self):
    #     """A2: Check for records that exceeded retry threshold and alert."""
    #     logger.info("🔍 Checking for records exceeding retry threshold...")
    #
    #     try:
    #         async for session in get_db_session():
    #             now = datetime.now(timezone.utc)
    #             one_day_ago = now - timedelta(days=1)
    #
    #             # Count intraday records with exceeded retries
    #             stmt = select(
    #                 func.count(PriceHistoryIntraday.id),
    #                 func.min(PriceHistoryIntraday.datetime),
    #                 func.max(PriceHistoryIntraday.datetime),
    #             ).where(
    #                 PriceHistoryIntraday.datetime >= one_day_ago,
    #                 PriceHistoryIntraday.resolve_required == True,
    #                 PriceHistoryIntraday.resolve_tries >= 3,
    #             )
    #             result = await session.execute(stmt)
    #             row = result.one()
    #             count, min_dt, max_dt = row
    #
    #             if count > 0:
    #                 logger.warning(
    #                     f"⚠️ ALERT: {count} intraday records exceeded retry limit! "
    #                     f"Time range: {min_dt} to {max_dt}"
    #                 )
    #
    #                 # Get sample of affected instruments
    #                 sample_stmt = (
    #                     select(
    #                         PriceHistoryIntraday.instrument_id,
    #                         func.count(PriceHistoryIntraday.id).label("cnt"),
    #                     )
    #                     .where(
    #                         PriceHistoryIntraday.datetime >= one_day_ago,
    #                         PriceHistoryIntraday.resolve_required == True,
    #                         PriceHistoryIntraday.resolve_tries >= 3,
    #                     )
    #                     .group_by(PriceHistoryIntraday.instrument_id)
    #                     .order_by(func.count(PriceHistoryIntraday.id).desc())
    #                     .limit(5)
    #                 )
    #
    #                 sample_result = await session.execute(sample_stmt)
    #                 top_affected = sample_result.all()
    #
    #                 logger.warning(
    #                     f"Top affected instruments: {[(r[0], r[1]) for r in top_affected]}"
    #                 )
    #             else:
    #                 logger.info("✅ No records exceeding retry threshold")
    #
    #             return count
    #
    #     except Exception as e:
    #         logger.error(f"Error checking exceeded retries: {e}")
    #         return 0

    async def reconcile_daily_volume(self):
        """B3: Reconcile daily volume by summing intraday volumes."""
        logger.info("🔄 Running daily volume reconciliation...")

        try:
            async for session in get_db_session():
                today = datetime.now(timezone.utc).replace(
                    hour=0, minute=0, second=0, microsecond=0
                )
                yesterday = today - timedelta(days=1)

                # Get daily records for yesterday
                daily_stmt = select(PriceHistoryDaily).where(
                    PriceHistoryDaily.datetime >= yesterday,
                    PriceHistoryDaily.datetime < today,
                )
                daily_result = await session.execute(daily_stmt)
                daily_records = daily_result.scalars().all()

                updated_count = 0
                for daily in daily_records:
                    # Sum intraday volumes for this instrument on this day
                    day_start = daily.datetime
                    day_end = day_start + timedelta(days=1)

                    vol_stmt = select(func.sum(PriceHistoryIntraday.volume)).where(
                        PriceHistoryIntraday.instrument_id == daily.instrument_id,
                        PriceHistoryIntraday.datetime >= day_start,
                        PriceHistoryIntraday.datetime < day_end,
                        PriceHistoryIntraday.volume.isnot(None),
                    )
                    vol_result = await session.execute(vol_stmt)
                    intraday_sum = vol_result.scalar() or 0

                    if daily.volume != intraday_sum and intraday_sum > 0:
                        logger.info(
                            f"Volume mismatch for instrument {daily.instrument_id}: "
                            f"daily={daily.volume}, intraday_sum={intraday_sum}"
                        )
                        daily.volume = intraday_sum
                        updated_count += 1

                if updated_count > 0:
                    await session.commit()
                    logger.info(
                        f"✅ Reconciled volume for {updated_count} daily records"
                    )
                else:
                    logger.info("✅ All daily volumes are consistent")

        except Exception as e:
            logger.error(f"Volume reconciliation failed: {e}")


# Singleton
_scheduled_jobs: Optional[ScheduledJobs] = None


def get_scheduled_jobs() -> ScheduledJobs:
    global _scheduled_jobs
    if _scheduled_jobs is None:
        _scheduled_jobs = ScheduledJobs()
    return _scheduled_jobs
