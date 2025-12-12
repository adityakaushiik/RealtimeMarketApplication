from typing import Optional

from models.base_model_py import BaseModelPy


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
