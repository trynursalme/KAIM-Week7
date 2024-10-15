# models.py

# models.py

from sqlalchemy import Column, Integer, String, DateTime
from .database import Base

class CleanData(Base):
    __tablename__ = "cleandata"

    ID = Column(Integer, primary_key=True, index=True)
    Channel_Title = Column(String, nullable=False)
    Channel_Username = Column(String, nullable=False)
    Message = Column(String, nullable=False)
    Date = Column(DateTime, nullable=False)
    Media_Path = Column(String, nullable=True)
