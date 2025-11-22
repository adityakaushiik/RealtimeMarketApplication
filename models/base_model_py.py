from pydantic import BaseModel


class BaseModelPy(BaseModel):
    class Config:
        from_attributes = True
