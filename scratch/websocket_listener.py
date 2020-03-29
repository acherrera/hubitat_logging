# WS client
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, String, MetaData, DateTime, Float, Integer
from datetime import datetime
import pandas as pd
import json
import shutil
import os
import sys
import asyncio
import websockets

postgresname = 'postgresql://postgres:mysecretpassword@localhost:5432/houselogs'
DB = create_engine(postgresname, echo=False)


def get_table_def(dict_in, db_in):
    """
    Turn dictionary into a table definition for insert / update
    see: https://www.compose.com/articles/using-postgresql-through-sqlalchemy/
    """
    meta = MetaData(db_in)
    
    val_mapping = {
        'pressure': Integer,
        'temperature': Float,
        'humidity': Float,
        'battery': Integer,
        'colorTemperature': Integer,
    }
    
    val_type = val_mapping.get(dict_in['name'], String)
    

    table_def = Table(dict_in['name'], meta,  
                    Column('source', String),
                    Column('name', String),
                    Column('displayName', String),
                    Column('value', String),
                    Column('unit', String),
                    Column('deviceId', Integer),
                    Column('hubId', Integer),
                    Column('locationId', Integer),
                    Column('installedAppId', Integer),
                    Column('descriptionText', String),
                    Column('timestamp', DateTime),
                     )
    return table_def

def insert_data(json_val, db):
    aa = get_table_def(json_val, db)
    try:
        aa.create()
        print(f"Table {json_val['name']} created!")
    except Exception as e:
        pass
    dup = aa.select().where(aa.c.value==json_val['value']).where(aa.c.timestamp==json_val['timestamp']).where(aa.c.deviceId==str(json_val['deviceId']))
    result = db.execute(dup)
    if result.rowcount == 0:
        insert_statement = aa.insert().values(**json_val)
        output = db.connect().execute(insert_statement)
        return 1
    elif result.rowcount == 1:
        return 0
    elif result.rowcount > 1:
        print(f"Duplicate data present for table {json_val['name']}")
        return 0



async def hello():


    uri = "ws://192.168.0.44/eventsocket"
    async with websockets.connect(uri) as websocket:



        message = await websocket.recv()
        message = json.loads(message)
        ts = datetime.utcnow()
        message['timestamp'] = ts.isoformat()

        conn = DB.connect()
        status = insert_data(message, conn)
        conn.close()
        if status == 1:
            print(f"{message} - inserted")
        else:
            print("Insert failed")


if __name__ == "__main__":
    while True:
        asyncio.get_event_loop().run_until_complete(hello())
