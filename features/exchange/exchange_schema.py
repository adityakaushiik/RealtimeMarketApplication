from typing import Optional

from models.base_model_py import BaseModelPy


class ExchangeCreateOrUpdate(BaseModelPy):
    id: Optional[int]
    name: Optional[str]
    code: Optional[str]
    timezone: Optional[str]
    country: Optional[str]
    currency: Optional[str]
