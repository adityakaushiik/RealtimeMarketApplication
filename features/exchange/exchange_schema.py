from typing import Optional, Union

from models.base_model_py import BaseModelPy


class ExchangeBase(BaseModelPy):
    name: str
    code: str
    timezone: Union[str, None] = None
    country: Union[str, None] = None
    currency: Union[str, None] = None


class ExchangeCreate(ExchangeBase):
    pass


class ExchangeUpdate(BaseModelPy):
    name: Union[str, None] = None
    code: Union[str, None] = None
    timezone: Union[str, None] = None
    country: Union[str, None] = None
    currency: Union[str, None] = None


class ExchangeInDb(ExchangeBase):
    id: int


class ExchangeProviderMappingCreate(BaseModelPy):
    provider_id: int
    exchange_id: int
    is_active: bool = True
    is_primary: bool = False


class ExchangeProviderMappingUpdate(BaseModelPy):
    is_active: Union[bool, None] = None
    is_primary: Union[bool, None] = None


class ExchangeProviderMappingInDb(BaseModelPy):
    provider_id: int
    exchange_id: int
    is_active: bool
    is_primary: bool
