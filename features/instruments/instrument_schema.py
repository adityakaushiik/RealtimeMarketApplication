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


class InstrumentTypeBase(BaseModelPy):
    code: str
    name: str
    description: Optional[str] = None
    category: Optional[str] = None
    display_order: Optional[int] = None


class InstrumentTypeCreate(InstrumentTypeBase):
    pass


class InstrumentTypeUpdate(BaseModelPy):
    code: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None
    category: Optional[str] = None
    display_order: Optional[int] = None


class InstrumentTypeInDb(InstrumentTypeBase):
    id: int


class SectorBase(BaseModelPy):
    name: str
    description: Optional[str] = None


class SectorCreate(SectorBase):
    pass


class SectorUpdate(BaseModelPy):
    name: Optional[str] = None
    description: Optional[str] = None


class SectorInDb(SectorBase):
    id: int
