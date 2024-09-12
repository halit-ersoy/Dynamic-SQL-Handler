import base64
import os
import time
from threading import Thread
import pyodbc
from fastapi import FastAPI, HTTPException, WebSocket, Request
import requests
import json
from starlette.responses import FileResponse

app = FastAPI()

# SQL sunucusu ve veritabanı bilgileri
server = ''  # Server name
database = ''  # Database name

# WebSocket bağlantıları
connections = set()


# Veritabanına bağlanma işlemi
def connect_to_database():
    try:
        # Kullanıcı adı ve şifre gerekmeyen bağlantı dizesi
        connection_string = f"DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
        conn = pyodbc.connect(connection_string)
        return conn
    except Exception as e:
        print("Veritabanı bağlantısında bir hata oluştu:", e)
        return None


# WebSocket işlemleri
@app.websocket("/ws")
@app.websocket("/web/socket")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connections.add(websocket)

    try:
        while True:
            data = await websocket.receive_text()
            for connection in connections:
                await connection.send_text(data)
    except Exception as e:
        print(e)
    finally:
        connections.remove(websocket)
        if not websocket.close:
            await websocket.close()


# SQL sorgusu işlemleri
def execute_query(cursor, sql_query, params=None):
    try:
        if params:
            cursor.execute(sql_query, params)
        else:
            cursor.execute(sql_query)
        return True
    except Exception as e:
        print(f"SQL sorgusunda bir hata oluştu: {e}")
        return False


# Veri post etme işlemi
@app.post('/p')
@app.post('/post/data')
async def post_data(request: Request, type: str, fun: str = None, param: str = None, table: str = None,
                    columns: str = None, column: str = None, query: str = None, path: str = None):
    try:
        connection = connect_to_database()

        if not connection:
            raise HTTPException(status_code=500, detail='Veritabanı bağlantısı başarısız oldu.')

        cursor = connection.cursor()

        try:
            data = await request.json()
        except:
            data = None

        # İşlevsel veri getirme işlemi
        if type == 'Fun':
            if fun:
                post_param = param
                if post_param:
                    param_list = post_param.split(',')
                    data_list = [data.get(i) for i in param_list]

                    sql_query = f"SELECT dbo.{fun}({', '.join(['?'] * len(data_list))}) AS result"
                    execute_query(cursor, sql_query, tuple(data_list))
                else:
                    sql_query = f"SELECT dbo.{fun}() AS result"
                    execute_query(cursor, sql_query)

                result = cursor.fetchone().result

                return {'type': 'Fun', 'message': 'True' if result == 1 else 'False'}

        # Update ve Insert işlemi
        elif type in ('Update', 'Insert'):
            if table and data:
                if type == 'Update':
                    post_column = column
                    post_query = query
                    if post_query:
                        query_list = post_query.split(':')

                        sql_query = f'UPDATE [IMU-Database].[dbo].[{table}] SET [{post_column}] = ? WHERE [{query_list[0]}] = ?'
                        execute_query(cursor, sql_query, (data.get(post_column), query_list[1]))

                elif type == 'Insert':
                    post_columns = columns
                    if post_columns:
                        column_list = post_columns.split(',')
                        data_list = [data.get(i) for i in column_list]

                        sql_query = f'INSERT INTO [dbo].[{table}] ({', '.join(column_list)}) VALUES ({', '.join(['?'] * len(data_list))})'
                        execute_query(cursor, sql_query, tuple(data_list))

                connection.commit()
                return {'type': type, 'message': 'True'}

        # Sorgu işlemi
        elif type == 'Query':
            if table and columns and data:
                column_list = columns.split(',')
                data_list = [data.get(i) for i in column_list]

                sql_query = f'SELECT COUNT(*) FROM [dbo].[{table}] WHERE {' AND '.join([f'{col} = ?' for col in column_list])}'

                cursor.execute(sql_query, tuple(data_list))
                result = cursor.fetchone()[0]

                connection.commit()

                return {'type': 'Query', 'message': 'True' if result > 0 else 'False'}

        # Dosya alma işlemi
        elif type == 'File':
            if path:
                path = f'C:/Users/asd/OneDrive/Masaüstü/Dosyalar/mssql_image{path}'
                if not os.path.exists(path):
                    os.makedirs(path)
                if data:
                    path = f'{path}/{data.get('name')}'
                    open(path, 'wb')
                    with open(path, 'wb') as file:
                        file.write(base64.b64decode(data.get('content')))
                return {'type': 'File', 'message': 'True', 'path': path}

        else:
            raise HTTPException(status_code=400, detail='Geçersiz type değeri.')

    except Exception as e:
        raise HTTPException(status_code=500, detail=f'Bir hata oluştu: {str(e)}')


# Veri get etme işlemi
@app.get('/g')
@app.get('/get/data')
async def get_data(type: str, table: str = None, query: str = None, columns: str = None, path: str = None,
                   fun: str = None):
    try:
        connection = connect_to_database()

        if not connection:
            raise HTTPException(status_code=500, detail='Veritabanı bağlantısı başarısız oldu.')

        cursor = connection.cursor()

        # Tablo verilerini getirme işlemi
        if type == 'Table':
            if table:
                get_query = query
                get_columns = columns

                if get_query:
                    query_hash_list = {key: value for key, value in (pair.split(':') for pair in get_query.split(','))}
                    conditions = ' AND '.join([f'{key} = ?' for key in query_hash_list.keys()])
                    sql_query = f'SELECT {get_columns} FROM {table} WHERE {conditions}'

                    cursor.execute(sql_query, tuple(query_hash_list.values()))
                else:
                    sql_query = f'SELECT {get_columns} FROM {table}'
                    cursor.execute(sql_query)

                rows = cursor.fetchall()
                data = [dict(zip([column[0] for column in cursor.description], row)) for row in rows]
                return data if data else {'message': 'False'}

        # Resim dosyasını getirme işlemi
        elif type == 'File':
            if path:
                file_ex = path.split('.')[-1]
                if file_ex.lower() in ['png', 'jpg', 'webp', 'bmp', 'jpeg', 'ico', 'mp4', 'mkv', 'avi', 'webm', 'mov']:
                    return FileResponse(f'C:/Users/asd/OneDrive/Masaüstü/Dosyalar/mssql_image{path}')
                else:
                    return FileResponse(f'C:/Users/asd/OneDrive/Masaüstü/Dosyalar/mssql_image{path}',
                                        filename=path.split('/')[-1])

        # İşlevsel veri getirme işlemi
        elif type == 'Fun':
            if fun:
                sql_query = f"SELECT dbo.{fun} AS about"
                cursor.execute(sql_query)

                rows = cursor.fetchall()
                data = [dict(zip([column[0] for column in cursor.description], row)) for row in rows]
                return data if data else {'message': 'Veri çekme işlemi başarısız oldu.'}

        # Geçersiz talepler için hata döndürme işlemi
        else:
            raise HTTPException(status_code=400, detail='Geçersiz type değeri.')

    except Exception as e:
        raise HTTPException(status_code=500, detail=f'Hata oluştu: {str(e)}')
