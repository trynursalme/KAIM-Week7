# crud.py

from sqlalchemy.orm import Session
from . import models, schemas

# Create a new cleandata entry
def create_clean_data(db: Session, clean_data: schemas.CleanDataCreate):
    db_clean_data = models.CleanData(**clean_data.dict())
    db.add(db_clean_data)
    db.commit()
    db.refresh(db_clean_data)
    return db_clean_data

# Get cleandata by ID (assuming 'ID' is unique in the table)
def get_clean_data(db: Session, clean_data_id: int):
    return db.query(models.CleanData).filter(models.CleanData.ID == clean_data_id).first()

# Get a list of cleandata entries (pagination)
def get_clean_data_list(db: Session, skip: int = 0, limit: int = 10):
    return db.query(models.CleanData).offset(skip).limit(limit).all()

