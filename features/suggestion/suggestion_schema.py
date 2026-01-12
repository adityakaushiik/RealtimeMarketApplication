from pydantic import BaseModel, ConfigDict
from datetime import datetime
from typing import Optional
from features.user.user_schema import UserInDb

class SuggestionTypeBase(BaseModel):
    name: str
    description: Optional[str] = None

class SuggestionTypeCreate(SuggestionTypeBase):
    pass

class SuggestionTypeUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None

class SuggestionTypeInDb(SuggestionTypeBase):
    id: int
    created_at: datetime
    updated_at: datetime
    is_active: bool

    model_config = ConfigDict(from_attributes=True)

class SuggestionBase(BaseModel):
    suggestion_type_id: int
    title: str
    description: str

class SuggestionCreate(SuggestionBase):
    pass

class SuggestionUpdate(BaseModel):
    suggestion_type_id: Optional[int] = None
    title: Optional[str] = None
    description: Optional[str] = None
    status: Optional[str] = None

class SuggestionInDb(SuggestionBase):
    id: int
    user_id: int
    status: str
    created_at: datetime
    updated_at: datetime
    is_active: bool

    model_config = ConfigDict(from_attributes=True)


class SuggestionResponse(SuggestionInDb):
    user: UserInDb
    suggestion_type: SuggestionTypeInDb

    model_config = ConfigDict(from_attributes=True)

