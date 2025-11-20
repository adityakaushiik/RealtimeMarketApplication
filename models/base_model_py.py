from pydantic import BaseModel


class BaseModelPy(BaseModel):
    class Config:
        orm_mode = True
