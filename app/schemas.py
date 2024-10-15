# schemas.py

from pydantic import BaseModel
from datetime import datetime

# Base schema for CleanData
class CleanDataBase(BaseModel):
    channel_title: str
    channel_username: str
    message: str
    date: datetime
    media_path: str

# Schema for creating CleanData (no ID in input)
class CleanDataCreate(CleanDataBase):
    pass

# Schema for returning CleanData (ID included)
class CleanData(CleanDataBase):
    id: int

    class Config:
        orm_mode = True

