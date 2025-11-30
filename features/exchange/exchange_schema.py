from typing import Optional

from models.base_model_py import BaseModelPy


class ExchangeBase(BaseModelPy):
    name: str
    code: str
    timezone: Optional[str] = None
    country: Optional[str] = None
    currency: Optional[str] = None


class ExchangeCreate(ExchangeBase):
    pass


class ExchangeUpdate(BaseModelPy):
    name: Optional[str] = None
    code: Optional[str] = None
    timezone: Optional[str] = None
    country: Optional[str] = None
    currency: Optional[str] = None


class ExchangeInDb(ExchangeBase):
    id: int


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
