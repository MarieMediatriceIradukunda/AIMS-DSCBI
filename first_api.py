from os import path
from fastapi import FastAPI
from pathlib import Path
import pandas as pd
import os
from typing import Optional, List
import pandas as pd
import psycopg2
from fastapi import FastAPI, Query, Path, HTTPException
from dotenv import load_dotenv

app = FastAPI()
load_dotenv()
# 1. Static endpoint
@app.get("/")
def read_root():
    return {"message": "Welcome to my first API!"}

# 2. Path parameter endpoint
@app.get("/greet/{name}/{city}")
def greet_person(name: str, city: str):
    return {"message": f"Hello {name} from {city}!"}

# 3. Data-serving endpoint (Rwanda sample data)
@app.get("/rwanda")
def get_rwanda_data():
    sample_data = {
        "capital": "Kigali",
        "population_millions": 13.8,
        "official_languages": ["Kinyarwanda", "English", "French"],
        "currency": "Rwandan franc (RWF)",
        "famous_for": [
            "Mountain gorillas",
            "Land of a Thousand Hills",
            "Clean and green capital city"
        ]
    }
    return {"rwanda_info": sample_data}

data ={
    "cells": [
        {"id": 1, "type": "residential", "area_sq_km": 2.5},
        {"id": 2, "type": "commercial", "area_sq_km": 1.2},
        {"id": 3, "type": "industrial", "area_sq_km": 3.0},
        {"id": 4, "type": "park", "area_sq_km": 0.8},
        {"id": 5, "type": "mixed-use", "area_sq_km": 1.5}
    ],
    "count": 5
}
@app.get("/cells")
def get_cells():
    return {"data":data} 

@app.get("/cells/{cell_id}")
def get_cell_by_id(cell_id: int):
    cell = next((cell for cell in data["cells"] if cell["id"] == cell_id), None)
    if cell:
        return {"cell": cell}
    else:
        return {"Cell not found"}

DIR_DATA = Path.cwd().parents[0] / "AIMS-DSCBI/data/tmp-db-data"
print(DIR_DATA)
DIR_CELLS = DIR_DATA / "cells.csv"
@app.get("/cells-file")
def get_cells_from_file():
    if not path.exists(DIR_CELLS):
        return {"Data file not found."}
    df = pd.read_csv(DIR_CELLS)
    cells_list = df.to_dict(orient="records")
    return {"data": cells_list}


# Sample in-memory dataset
data = [
    {
        "name": "Denise",
        "age": 30,
        "city": "Kigali",
        "country": "Rwanda",
        "marital_status": "Guess",
    },
    {
        "name": "Donald",
        "age": 20,
        "city": "Kigali",
        "country": "Rwanda",
        "marital_status": "Happily Married",
    },
]


@app.get("/trainees")
def get_trainee_info(
    name: str = Query(..., description="Person's name (required)"),
    marital_status: Optional[bool] = Query(False, description="Include marital status"),
    age: Optional[bool] = Query(False, description="Include age"),
):
    """
    Return trainee info:
    - Always return name, city, and country
    - If marital_status=true, include marital_status
    - If age=true, include age
    """
    # Find the person
    person = next((p for p in data if p["name"].lower() == name.lower()), None)
    if not person:
        raise HTTPException(status_code=404, detail="Person not found")

    # Base response
    response = {
        "name": person["name"],
        "city": person["city"],
        "country": person["country"],
    }

    # Add optional fields
    if marital_status:
        response["marital_status"] = person["marital_status"]
    if age:
        response["age"] = person["age"]

    return {"Trainee-Info": response}


# ==========================================================
# EXERCISE-1 - EDIT trainees endpoint
# =========================================================
# add children parameter (bool) to include number of children
# if children=true, include number_of_children in response
# access the app and see the results for yourself!
# http://localhost:8000/trainees?name=Denise&marital_status=true&age=true&children=true
# http://localhost:8000/trainees?name=Donald&marital_status=true&age=true&children=true


@app.get("/cells")
def list_cells():
    df = pd.read_csv(DIR_CELLS)
    return df.to_dict(orient="records")


# ==========================================================
# RETURN CELLS BY PROVINCE - PATH PARAMETER
# =========================================================
@app.get("/cells/province/{province_name}")
def get_cells_by_province_path(province_name: str):
    """
    Filter cells by province using a PATH parameter
    Example:
      /cells/province/Kigali
    """
    df = pd.read_csv(DIR_CELLS)
    filtered = df[df["province_name"].str.lower() == province_name.lower()]
    return filtered.to_dict(orient="records")


# ==========================================================
# RETURN CELLS BY PROVINCE - QUERY PARAMETER
# =========================================================
@app.get("/cells-by-province")
def get_cells_by_province_query(
    province_name: Optional[str] = Query(None, description="Optional province filter")
):
    """
    Filter cells by province using a QUERY parameter
    Example:
      /cells-by-province?province_name=Kigali
      /cells-by-province   (returns all provinces if not specified)
    """
    df = pd.read_csv(DIR_CELLS)
    if province_name:
        df = df[df["province_name"].str.lower() == province_name.lower()]
    return df.to_dict(orient="records")


# ACCESS THE APP AND SEE THE RESULTS FOR YOURSELF!
# http://localhost:8000/cells
# http://localhost:8000/cells/province/Kigali
# http://localhost:8000/cells-by-province?province_name=Kigali


# ==========================================================
# EXERCISE-2 - RETURN POPULATION BY PROVINCE
# =========================================================
# 1. Use Path parameter to get population by province
# 2. Use Query parameter to get population by province
# Hint: use similar logic as cells by province above


# ==========================================================
# RETURNING DATA WITH A DATA MODEL (PYDANTIC)
# =========================================================
from pydantic import BaseModel
from typing import Optional, List

from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
import pandas as pd
from pathlib import Path

# =============================================================
# DEFINE PYDANTIC MODEL FOR CELL WHICH MATCHES CSV STRUCTURE
# =============================================================
class Cell(BaseModel):
    cell_id: str
    province_name: Optional[str] = None
    district_name: Optional[str] = None
    sector_name: Optional[str] = None
    cell_name: Optional[str] = None


# =============================================================
# DEFINE ENDPOINT TO RETURN CELLS USING THE DATA MODEL
# =============================================================
@app.get("/cells-with-data-model", response_model=List[Cell])
def list_cells():
    df = pd.read_csv(DIR_CELLS)
    return df.to_dict(orient="records")


# ==========================================================
# RETURNING DATA FROM POSTGRESQL DATABASE
# =========================================================

# =============================================================
# Load environment variables from .env file
# =============================================================
load_dotenv()

# Get database connection details from environment variables
PGHOST = os.getenv("PGHOST")
PGPORT = os.getenv("PGPORT")
PGDATABASE = os.getenv("PGDATABASE")
PGUSER = os.getenv("PGUSER")
PGPASSWORD = os.getenv("PGPASSWORD")


# =============================================================
# HELPER FUNCTION TO CONNECT AND QUERY
# =============================================================
def run_query(sql: str, params: tuple = ()):
    conn = psycopg2.connect(
        host=PGHOST,
        port=PGPORT,
        dbname=PGDATABASE,
        user=PGUSER,
        password=PGPASSWORD,
    )
    try:
        df = pd.read_sql(sql, conn, params=params)
    finally:
        conn.close()
    return df.to_dict(orient="records")


# =============================================================
# ENDPOINTS (NO DATA MODEL)
# =============================================================
@app.get("/cells-db")
def list_cells():
    """Return all rows from cells table"""
    sql = "SELECT * FROM cells"
    return run_query(sql)


# ============================================================
# EXERCISE-3 - RETURN POPULATION BY PROVINCE FROM DATABASE
# ============================================================
# 1. use Path parameter to get population by province from DB
# 2. name the endpoint /population/province/{province_name}
