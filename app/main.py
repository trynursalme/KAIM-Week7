# main.py

import sys
import os
sys.path.append("../app")

from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from .database import SessionLocal, engine
from . import models, crud, schemas

models.Base.metadata.create_all(bind=engine)

app = FastAPI()

# Define a root route
@app.get("/")
def read_root():
    return {"message": "Welcome to the CleanData API!"}

# Dependency to get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# POST route to create a new CleanData entry
@app.post("/clean_data/", response_model=schemas.CleanData)
def create_clean_data(clean_data: schemas.CleanDataCreate, db: Session = Depends(get_db)):
    return crud.create_clean_data(db=db, clean_data=clean_data)

# GET route to read CleanData by ID
@app.get("/clean_data/{clean_data_id}", response_model=schemas.CleanData)
def read_clean_data(clean_data_id: int, db: Session = Depends(get_db)):
    db_clean_data = crud.get_clean_data(db=db, clean_data_id=clean_data_id)
    if db_clean_data is None:
        raise HTTPException(status_code=404, detail="CleanData not found")
    return db_clean_data

# GET route to read CleanData list with pagination
@app.get("/clean_data/", response_model=list[schemas.CleanData])
def read_clean_data_list(skip: int = 0, limit: int = 10, db: Session = Depends(get_db)):
    return crud.get_clean_data_list(db=db, skip=skip, limit=limit)

