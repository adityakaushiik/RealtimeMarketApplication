from datetime import datetime
from typing import Any, Dict, Optional

from models.base_model_py import BaseModelPy


class ProviderCreate(BaseModelPy):
    name: str
    code: str
    credentials: Optional[Dict[str, Any]] = None
    rate_limit: Optional[int] = None


class ProviderUpdate(BaseModelPy):
    name: Optional[str] = None
    code: Optional[str] = None
    credentials: Optional[Dict[str, Any]] = None
    rate_limit: Optional[int] = None


class ProviderInDb(BaseModelPy):
    id: int
    name: str
    code: str
    credentials: Optional[Dict[str, Any]]
    rate_limit: Optional[int]
    created_at: datetime
    updated_at: datetime
    is_active: bool


class ProviderInstrumentMappingCreate(BaseModelPy):
    provider_id: int
    instrument_id: int
    provider_instrument_search_code: str


class ProviderInstrumentMappingUpdate(BaseModelPy):
    provider_instrument_search_code: Optional[str] = None


class ProviderInstrumentMappingInDb(BaseModelPy):
    provider_id: int
    instrument_id: int
    provider_instrument_search_code: str
