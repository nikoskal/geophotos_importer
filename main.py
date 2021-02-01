from typing import Optional
from fastapi import Depends , FastAPI, File, UploadFile , Form, Header
from fastapi.responses import HTMLResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from typing import List
from time import time
import psycopg2
import aiohttp
import aiofiles
import asyncio
import json
import sys
import random
import hashlib
import datetime
import json
import params
from params import photo_request_data

# from   amGeoImporter  import amGeoImporter;

#Connect to PostgreSQL
t_host    = params.t_host
t_port    = params.t_port
t_dbname  = params.t_dbname
t_user    = params.t_user
t_pw      = params.t_pw
db_conn   = psycopg2.connect(host=t_host, port=t_port, dbname=t_dbname, user=t_user, password=t_pw)
db_cursor = db_conn.cursor()

#Start Service
app      = FastAPI()
security = HTTPBasic()

correct_user_key = params.user_key


@app.post("/doc/responses/{hash_land}")
async def upload_photo(
            hash_land: str,
            file: UploadFile = File(...),
            user_key : Optional[str] = Header(None, convert_underscores=False),
            ):

    ts = datetime.datetime.now().timestamp()
    print ("hash_land ", hash_land)
    print ("user_key ", user_key )
    print("retrieve")

    # timestamp, photo_name, photo_file, parcel_id, user
    photo_name = file.filename
    SQL = """INSERT INTO uploaded_photos(timestamp, photo_name, photo_file, parcel_id) VALUES(%s,%s,%s,%s);"""
    data = (ts,photo_name,psycopg2.Binary(file.file.read()),hash_land)
    db_cursor.execute(SQL, data)

    db_conn.commit()
    print("Insert done")

    if user_key != correct_user_key:
        return {"error": "Invalid User Key"}
    else:
        with open(file.filename, "wb+") as file_object:
            file_object.write(file.file.read())
        return {"res": "file " + file.filename + " uploaded"}


@app.get("/doc/requests/{user_code}")
async def get_requests(
            user_code: str,
            user_key : Optional[str] = Header(None, convert_underscores=False),
            ):

    if user_key != correct_user_key:
        return {"error": "Invalid User Key"}

    if user_code in photo_request_data:
        reply= {
            "id": photo_request_data[user_code]["id"],
            "type": {
                "code": 1,
                "description": "Overclaim MEA land"
            },
            "context": [
                {
                    "type": "land",
                    "label": "J00000012_PT",
                    "comment": "DAFM Geotag Photo Request: Please capture a photograph within the parcel, close to the point illustrated on the map. The app will indicate when to capture the photograph.",
                    "hash": photo_request_data[user_code]["land_hash"],
                    "geoCoordinates": {
                        "type": "Point",
                        "typeName": "Point",
                        "coordinates": [
                            photo_request_data[user_code]["coords_x"],
                            photo_request_data[user_code]["coords_y"]
                        ]
                    }
                }
            ],
            "scheme": "BPS",
            "geotag": "Y",
            "herdNumber": "J0000001",
            "year": 2021,
            "correspondenceId": 3352497060,
            "correspondenceDocNo": 3386371088,
            "hash": user_code
        }
        return reply
    else:
        return {"no data for user:"+user_code}



#just for testing#
@app.get("/retreive/{hash_land}")
async def get_photo(
            hash_land: str,
            user_key : Optional[str] = Header(None, convert_underscores=False),
            ):

    if user_key != correct_user_key:
        return {"error": "Invalid User Key"}

    print (hash_land)
    db_cursor.execute("""SELECT * FROM uploaded_photos WHERE parcel_id = %s;""", (hash_land,))
    mypic = db_cursor.fetchone()

    print(mypic[3])
    open('retr_photo.jpg' , 'wb').write((mypic[3]))

    return ('done')

