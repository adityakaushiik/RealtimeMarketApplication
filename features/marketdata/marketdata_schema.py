from models.base_model_py import BaseModelPy


class PriceHistoryIntradayBase(BaseModelPy):
    instrument_id: int
    timestamp: int
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float | None = None
    previous_close: float | None = None
    adj_close: float | None = None
    volume: int | None = None
    deliver_percentage: float | None = None
    price_not_found: bool = False
    interval: str | None = None


class PriceHistoryIntradayCreate(PriceHistoryIntradayBase):
    pass


class PriceHistoryIntradayUpdate(BaseModelPy):
    instrument_id: int | None = None
    timestamp: int | None = None
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float | None = None
    previous_close: float | None = None
    adj_close: float | None = None
    volume: int | None = None
    deliver_percentage: float | None = None
    price_not_found: bool | None = None
    interval: str | None = None


class PriceHistoryIntradayInDb(PriceHistoryIntradayBase):
    id: int


class PriceHistoryDailyBase(BaseModelPy):
    instrument_id: int
    timestamp: int
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float | None = None
    previous_close: float | None = None
    adj_close: float | None = None
    volume: int | None = None
    deliver_percentage: float | None = None
    price_not_found: bool = False


class PriceHistoryDailyCreate(PriceHistoryDailyBase):
    pass


class PriceHistoryDailyUpdate(BaseModelPy):
    instrument_id: int | None = None
    timestamp: int | None = None
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float | None = None
    previous_close: float | None = None
    adj_close: float | None = None
    volume: int | None = None
    deliver_percentage: float | None = None
    price_not_found: bool | None = None


class PriceHistoryDailyInDb(PriceHistoryDailyBase):
    id: int
