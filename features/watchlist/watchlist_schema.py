from typing import Optional

from features.instruments.instrument_schema import InstrumentInDb
from models.base_model_py import BaseModelPy


class WatchlistItemBase(BaseModelPy):
    instrument_id: int


class WatchlistItemCreate(WatchlistItemBase):
    pass


class WatchlistItemInDb(WatchlistItemBase):
    id: int
    watchlist_id: int


class WatchlistBase(BaseModelPy):
    name: str
    show_on_dashboard: bool


class WatchlistCreate(WatchlistBase):
    pass


class WatchlistUpdate(BaseModelPy):
    name: Optional[str] = None


class WatchlistInDb(WatchlistBase):
    id: int
    user_id: int
    items: list[InstrumentInDb] = []
