import psycopg2
from urllib.parse import urlparse
from config import host, user, password, db_name, port, log_pattern
import numpy as np
import re
import json
import sys
import PyQt5
from des import *


def json_fil_ip(cursor):
    query = filter_by_ip(cursor)
    cursor.execute(query)
    rows = cursor.fetchall()
    data_by_ip = {}
    for row in rows:
        ip_address = str(row[0])
        if ip_address not in data_by_ip:
            data_by_ip[ip_address] = []
        data_by_ip[ip_address].append({
            'Data_Date_Time': row[1].strftime('%Y-%m-%d %H:%M:%S'),
            'Data_Request_Method': row[2],
            'Data_Status_Code': row[3],
            'Data_Response_Size': row[4],
            'Data_Referer': row[5],
            'Data_User_Agent': row[6]
        })

    json_data = json.dumps(data_by_ip)
    print(json_data)
    return json_data


def json_group_by_ip(cursor):
    ip_query = group_by_ip(cursor)
    cursor.execute(ip_query)
    ip_rows = cursor.fetchall()

    data_by_ip = [] 

    for ip_row in ip_rows:
        ip_address = ip_row[0]
        total_requests = ip_row[1]

        data_by_ip.append({
            'IP_Address': ip_address,
            'Total_Requests': total_requests
        })

    json_data = json.dumps(data_by_ip) 
    print(json_data)
    return json_data


def check_user(cursor,login='', password=''):
    query = """
    SELECT User_Login, User_Password
    FROM Costumer
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    counter = 0
    for i in rows:
        if(login in i):
            if rows[counter][1] == password:
                return True
            else:
                return False
        counter+=1
    return False
    
def json_group_by_datetime_between(cursor, start_date, end_date):
    query = filter_by_date(cursor)
    
    values = (start_date, end_date)
    cursor.execute(query, values)
    rows = cursor.fetchall()
    print(rows)

    data = []  # Список для хранения данных

    for row in rows:
        ip_address = row[0]
        date_time = row[1].strftime('%Y-%m-%d %H:%M:%S')
        request_method = row[2]
        status_code = row[3]
        response_size = row[4]
        referer = row[5]
        user_agent = row[6]

        data.append({
            'Data_Address_ID': ip_address,
            'Data_Date_Time': date_time,
            'Data_Request_Method': request_method,
            'Data_Status_Code': status_code,
            'Data_Response_Size': response_size,
            'DataReferer': referer,
            'Data_User_Agent': user_agent
        })

    json_data = json.dumps(data) 
    print(json_data)
    return json_data


def filter_by_ip(cursor):
    query = """
        SELECT DISTINCT ON (Data_Address_ID) Data_Address_ID, Data_Date_Time, Data_Request_Method, Data_Status_Code, Data_Response_Size, Data_Referer, Data_User_Agent
        FROM Data
        ORDER BY Data_Address_ID ASC
    """
    return query
def group_by_ip(cursor):
    query = """
    SELECT DISTINCT Data_Address_ID, COUNT(*) AS total_requests
    FROM Data
    GROUP BY Data_Address_ID
    """
    return query
def filter_by_date(cursor):
    query = """
                SELECT  Data_Address_ID, Data_Date_Time, Data_Request_Method, Data_Status_Code, Data_Response_Size, Data_Referer, Data_User_Agent
        FROM Data
                WHERE DATE(Data_Date_Time) BETWEEN %s AND %s ORDER BY Data_Date_Time ASC
            """
    return query
def view_data_by_data(cursor):
    query = """
    SELECT DISTINCT Data_Date_Time, COUNT(*) AS total_requests
    FROM Data   q
    GROUP BY Data_Date_Time
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    arr = []
    for row in rows:
        date = row[0]
        total_requests = row[1]
        arr.append(f'Дата: {date}, Общее количество запросов: {total_requests}')
    return arr


def view_data_by_ip(cursor):
    query = group_by_ip(cursor)
    print(query)
    cursor.execute(query)
    rows = cursor.fetchall()
    arr = []
    for row in rows:
        ip_address = row[0]
        total_requests = row[1]
        arr.append(f'IP адрес: {ip_address}, Общее количество запросов: {total_requests}')
    return arr

def view_data_between_date(start_date, end_date, cursor):
    query = filter_by_date(cursor)
    values = (start_date, end_date)
    cursor.execute(query, values)
    data = cursor.fetchall()

    return data


def parse_and_to_date_base(line, cursor):
    match = re.match(log_pattern, line)
    if match:
        ip_address = match.group(1)
        date_time = match.group(4)
        request_method = match.group(5)
        status_code = match.group(6)
        response_size = match.group(7)
        referer = match.group(8)
        user_agent = match.group(9)
        select_query = """
            SELECT COUNT(*) FROM Data WHERE Data_Address_ID = %s AND Data_Date_Time = %s
        """
        cursor.execute(select_query, (ip_address, date_time))
        count = cursor.fetchone()[0]
        
        if count == 0:
            insert_query = """
                INSERT INTO Data (Data_Address_ID, Data_Date_Time, Data_Request_Method, Data_Status_Code, Data_Response_Size, Data_Referer, Data_User_Agent)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            values = (ip_address, date_time, request_method, status_code, response_size, referer, user_agent)
            cursor.execute(insert_query, values)
            connection.commit()
            
            print(f'IP адрес: {ip_address}')
            print(f'Метод запроса: {request_method}')
            print(f'Код состояния: {status_code}')
            print(f'Размер ответа: {response_size}')
            print(f'Referer: {referer}')
            print(f'User Agent: {user_agent}')
    else:
        print('Не удалось распарсить лог.')
def view_data(cursor):
    query =  """
            SELECT Data_Address_ID as "Address_ID", Data_Date_Time as "Date_Time", Data_Request_Method as "Data_Request_Metod", Data_Status_Code as "status", Data_Response_Size as "Resp_Size", Data_Referer as "Data_Refer", Data_User_Agent as "Data_User" FROM Data;
        """
    cursor.execute(query)
    data = cursor.fetchall()
    #print(data)
    return data

connection = False
def make_connect(digit,start = 0, end = 0,login="",password1=""):
    global connection 
    try:
        connection = psycopg2.connect(
            host=host,
            user=user,
            password=password,
            database=db_name,
            port=port
        )

        with connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM Data ")
            print(cursor.fetchone())
            with open("access.log") as log:
                for line in log:
                    parse_and_to_date_base(line, cursor)
            if(digit == 2):
                return view_data(cursor)
            
            #view_data_by_data(cursor)
            if digit == 3:
                return(view_data_by_data(cursor))
            if digit == 4:
                return view_data_by_ip(cursor)
            if digit == 6:
                return json_group_by_ip(cursor)
            if digit == 7:
                return json_fil_ip(cursor)
            if digit == 5:
                return view_data_between_date(start, end, cursor)
            if digit == 8:
                return json_group_by_datetime_between(cursor, '2022-12-07 15:43:37', '2022-12-12 10:25:06')
            if digit == 0:
                return check_user(cursor,login,password1)
    except Exception as ex:
        print("Ошибка при работе с PostgreSQL:", ex)
        return ex
    finally:
        if connection:
            connection.close()
            print('PostgreSQL connection has been closed')
    return "success"
if __name__ == "__main__":
    make_connect(1)
    make_connect(2)