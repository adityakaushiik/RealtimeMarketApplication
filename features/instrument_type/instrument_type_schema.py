from typing import Optional

from models.base_model_py import BaseModelPy


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
