from datetime import datetime
from typing import Optional

from models.base_model_py import BaseModelPy


class PriceHistoryIntradayBase(BaseModelPy):
    instrument_id: int
    datetime: datetime
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    previous_close: Optional[float] = None
    adj_close: Optional[float] = None
    volume: Optional[int] = None
    deliver_percentage: Optional[float] = None
    price_not_found: bool = False
    interval: Optional[str] = None


class PriceHistoryIntradayCreate(PriceHistoryIntradayBase):
    pass


class PriceHistoryIntradayUpdate(BaseModelPy):
    instrument_id: Optional[int] = None
    datetime: Optional[datetime] = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    previous_close: Optional[float] = None
    adj_close: Optional[float] = None
    volume: Optional[int] = None
    deliver_percentage: Optional[float] = None
    price_not_found: Optional[bool] = None
    interval: Optional[str] = None


class PriceHistoryIntradayInDb(PriceHistoryIntradayBase):
    id: int


class PriceHistoryDailyBase(BaseModelPy):
    instrument_id: int
    datetime: datetime
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    previous_close: Optional[float] = None
    adj_close: Optional[float] = None
    volume: Optional[int] = None
    deliver_percentage: Optional[float] = None
    price_not_found: bool = False


class PriceHistoryDailyCreate(PriceHistoryDailyBase):
    pass


class PriceHistoryDailyUpdate(BaseModelPy):
    instrument_id: Optional[int] = None
    datetime: Optional[datetime] = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    previous_close: Optional[float] = None
    adj_close: Optional[float] = None
    volume: Optional[int] = None
    deliver_percentage: Optional[float] = None
    price_not_found: Optional[bool] = None


class PriceHistoryDailyInDb(PriceHistoryDailyBase):
    id: int
