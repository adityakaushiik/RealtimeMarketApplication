from services.redis_timeseries import get_redis_timeseries


class DataSaver:

    def __init__(self):
        self.redis_timeseries = get_redis_timeseries()

    async def save_to_intraday_table(self) -> None:
        """
        Save the stock price data to the intraday table in the database.
        """

        keys = await self.redis_timeseries.get_all_keys()
        data = await self.redis_timeseries.get_all_ohlcv_last_5m(keys)

        for record in data:
            print(record)