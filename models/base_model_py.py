from pydantic.v1 import BaseModel


class BaseModelPy(BaseModel):
    class Config:
        orm_mode = True