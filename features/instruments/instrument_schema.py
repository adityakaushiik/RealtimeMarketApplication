from typing import Optional

from models.base_model_py import BaseModelPy


class InstrumentBase(BaseModelPy):
    symbol: str
    name: str
    exchange_id: int
    instrument_type_id: int
    sector_id: Optional[int] = None
    blacklisted: bool = False
    delisted: bool = False
    should_record_data: bool


class InstrumentCreate(InstrumentBase):
    pass


class InstrumentUpdate(BaseModelPy):
    symbol: Optional[str] = None
    name: Optional[str] = None
    exchange_id: Optional[int] = None
    instrument_type_id: Optional[int] = None
    sector_id: Optional[int] = None
    blacklisted: Optional[bool] = None
    delisted: Optional[bool] = None


class InstrumentInDb(InstrumentBase):
    id: int
    is_active: bool
