import asyncio

from utils.common_constants import DataIngestionFormat


class DataIngestionQueue:

    def __init__(self):
        self.queue = asyncio.Queue()
        self._loop = None

    def _ensure_loop(self):
        if self._loop is None:
            try:
                self._loop = asyncio.get_running_loop()
            except RuntimeError:
                pass
        return self._loop

    async def add_data(self, data_format: DataIngestionFormat):
        self._ensure_loop()
        await self.queue.put(data_format)

    def add_data_threadsafe(self, data_format: DataIngestionFormat):
        """Helper to add data from a background thread"""
        loop = self._ensure_loop()
        if loop and not loop.is_closed():
            loop.call_soon_threadsafe(self.queue.put_nowait, data_format)
        else:
            # Fallback for when loop might not be captured yet (rare)
            # Try to get loop from current context if possible (unlikely in thread)
            print("Error: Event loop not captured in DataIngestionQueue")

    async def get_data(self) -> DataIngestionFormat:
        self._ensure_loop()
        data_format = await self.queue.get()
        self.queue.task_done()
        return data_format

    def qsize(self):
        return self.queue.qsize()


data_ingestion_queue_instance = None
def get_data_ingestion_queue_instance() -> DataIngestionQueue:
    global data_ingestion_queue_instance
    if data_ingestion_queue_instance is None:
        data_ingestion_queue_instance = DataIngestionQueue()
    return data_ingestion_queue_instance