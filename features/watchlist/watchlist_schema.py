from typing import Optional
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


class WatchlistCreate(WatchlistBase):
    pass


class WatchlistUpdate(BaseModelPy):
    name: Optional[str] = None


class WatchlistInDb(WatchlistBase):
    id: int
    user_id: int
    items: list[WatchlistItemInDb] = []
