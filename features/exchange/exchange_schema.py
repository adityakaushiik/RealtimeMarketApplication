from typing import Optional
from datetime import time, date

from models.base_model_py import BaseModelPy



class ExchangeHolidayBase(BaseModelPy):
    date: date
    description: Optional[str] = None
    is_closed: bool = True
    open_time: Optional[time] = None
    close_time: Optional[time] = None


class ExchangeHolidayCreate(ExchangeHolidayBase):
    exchange_id: int


class ExchangeHolidayUpdate(BaseModelPy):
    date: Optional[date] = None
    description: Optional[str] = None
    is_closed: Optional[bool] = None
    open_time: Optional[time] = None
    close_time: Optional[time] = None


class ExchangeHolidayInDb(ExchangeHolidayBase):
    id: int
    exchange_id: int


class ExchangeBase(BaseModelPy):
    name: str
    code: str
    timezone: Optional[str] = None
    country: Optional[str] = None
    currency: Optional[str] = None
    pre_market_open_time: Optional[time] = None
    market_open_time: Optional[time] = None
    market_close_time: Optional[time] = None
    post_market_close_time: Optional[time] = None


class ExchangeCreate(ExchangeBase):
    pass


class ExchangeUpdate(BaseModelPy):
    name: Optional[str] = None
    code: Optional[str] = None
    timezone: Optional[str] = None
    country: Optional[str] = None
    currency: Optional[str] = None
    pre_market_open_time: Optional[time] = None
    market_open_time: Optional[time] = None
    market_close_time: Optional[time] = None
    post_market_close_time: Optional[time] = None


class ExchangeInDb(ExchangeBase):
    id: int
    current_day_holidays: list[ExchangeHolidayInDb] = []


class ExchangeProviderMappingCreate(BaseModelPy):
    provider_id: int
    exchange_id: int
    is_active: bool = True
    is_primary: bool = False


class ExchangeProviderMappingUpdate(BaseModelPy):
    is_active: Optional[bool] = None
    is_primary: Optional[bool] = None


class ExchangeProviderMappingInDb(BaseModelPy):
    provider_id: int
    exchange_id: int
    is_active: bool
    is_primary: bool



