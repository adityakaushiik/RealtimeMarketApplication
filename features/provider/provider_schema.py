from datetime import datetime
from typing import Any, Dict

from models.base_model_py import BaseModelPy


class ProviderCreate(BaseModelPy):
    name: str
    code: str
    credentials: Dict[str, Any] | None = None
    rate_limit: int | None = None


class ProviderUpdate(BaseModelPy):
    name: str | None = None
    code: str | None = None
    credentials: Dict[str, Any] | None = None
    rate_limit: int | None = None


class ProviderInDb(BaseModelPy):
    id: int
    name: str
    code: str
    credentials: Dict[str, Any] | None
    rate_limit: int | None
    created_at: datetime
    updated_at: datetime
    is_active: bool


class ProviderInstrumentMappingCreate(BaseModelPy):
    provider_id: int
    instrument_id: int
    provider_instrument_search_code: str


class ProviderInstrumentMappingUpdate(BaseModelPy):
    provider_instrument_search_code: str | None = None


class ProviderInstrumentMappingInDb(BaseModelPy):
    provider_id: int
    instrument_id: int
    provider_instrument_search_code: str
