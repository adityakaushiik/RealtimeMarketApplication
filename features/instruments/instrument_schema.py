from models.base_model_py import BaseModelPy


class InstrumentBase(BaseModelPy):
    symbol: str
    name: str
    exchange_id: int
    instrument_type_id: int
    sector_id: int | None = None
    blacklisted: bool = False
    delisted: bool = False


class InstrumentCreate(InstrumentBase):
    pass


class InstrumentUpdate(BaseModelPy):
    symbol: str | None = None
    name: str | None = None
    exchange_id: int | None = None
    instrument_type_id: int | None = None
    sector_id: int | None = None
    blacklisted: bool | None = None
    delisted: bool | None = None


class InstrumentInDb(InstrumentBase):
    id: int


class InstrumentTypeBase(BaseModelPy):
    code: str
    name: str
    description: str | None = None
    category: str | None = None
    display_order: int | None = None


class InstrumentTypeCreate(InstrumentTypeBase):
    pass


class InstrumentTypeUpdate(BaseModelPy):
    code: str | None = None
    name: str | None = None
    description: str | None = None
    category: str | None = None
    display_order: int | None = None


class InstrumentTypeInDb(InstrumentTypeBase):
    id: int


class SectorBase(BaseModelPy):
    name: str
    description: str | None = None


class SectorCreate(SectorBase):
    pass


class SectorUpdate(BaseModelPy):
    name: str | None = None
    description: str | None = None


class SectorInDb(SectorBase):
    id: int
