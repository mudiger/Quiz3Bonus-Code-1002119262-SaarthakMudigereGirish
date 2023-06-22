from flask import request
from flask import Flask
from flask import render_template
import pyodbc
import os
import redis
import time


app = Flask(__name__)
basedir = os.path.abspath(os.path.dirname(__file__))

driver = '{ODBC Driver 17 for SQL Server}'
server = 'sqlserver-1002119262-saarthakmudigeregirish.database.windows.net'
database = 'DataBase-1002119262-SaarthakMudigereGirish'
username = 'saarthakmudigeregirish'
password = 'Hello123'

# Establish the connection
conn = pyodbc.connect(f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}')

# Create a cursor object
cursor = conn.cursor()

# Redis connection details
redis_host = 'Redis-1002119262-SaarthakMudigereGirish.redis.cache.windows.net'
redis_port = 6379
redis_password = 'My2SymFLIM78MdHnE61sCcWkM6G0HGWd6AzCaKwIZUU='

# Redis client
redis_client = redis.Redis(host=redis_host, port=redis_port, password=redis_password)
# redis_client.flushall()

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/page1/", methods=['GET', 'POST'])
def page1():
    total_time = []
    instance_time = []
    salpics = []
    instance = []
    if request.method == "POST":
        minlat = request.form['minlat']
        maxlat = request.form['maxlat']
        minlong = request.form['minlong']
        maxlong = request.form['maxlong']

        for i in range(30):
            instance.append(i + 1)

        query = "SELECT City, State, Population, lat, lon FROM dbo.city1 WHERE lat BETWEEN ? AND ? AND lon BETWEEN ? AND ?"
        cursor.execute(query, minlat, maxlat, minlong, maxlong)
        s =time.time()
        for i in instance:
            start = time.time()

            end = time.time()
            diff = end - start
            instance_time.append(diff)

            row = cursor.fetchall()
            for i in row:
                salpics.append(i)
        e=time.time()
        total_time.append((e-s))

    return render_template("1)Page.html", salpics=salpics, total_time=total_time, instance_time=instance_time, instance=instance)


@app.route("/page2/", methods=['GET', 'POST'])
def page2():
    instance_time = []
    instance = []
    total_time = []
    salpics = []
    if request.method == "POST":
        num = request.form['num']
        min = request.form['min']
        max = request.form['max']

        for i in range(int(num)):
            instance.append(i + 1)

        query = "SELECT City, State, Population, lat, lon  FROM dbo.city1 WHERE Population BETWEEN ? AND ? ORDER BY NEWID() "
        s = time.time()
        for i in instance:
            start = time.time()
            cursor.execute(query, min, max)
            end = time.time()
            diff = end - start
            instance_time.append(diff)

            row = cursor.fetchall()
            for i in row:
                salpics.append(i)
        e = time.time()
        total_time.append(e - s)

    return render_template("2)Page.html", total_time=total_time, instance_time=instance_time, instance=instance, salpics=salpics)


@app.route("/page3/", methods=['GET', 'POST'])
def page3():
    instance_time = []
    instance = []
    total_time = []
    salpics = []
    count=0

    if request.method == "POST":
        state = request.form['state']
        min = request.form['min']
        max = request.form['max']
        inc = request.form['inc']

        query = "UPDATE dbo.city1 SET Population=Population+? WHERE State=? AND Population BETWEEN ? AND ?"
        start = time.time()
        cursor.execute(query, inc, state, min, max)
        conn.commit()
        end = time.time()
        total_time.append(end-start)
        total_min=int(min)+int(inc)
        total_max = int(max) + int(inc)
        query2 = "SELECT City, State, Population, lat, lon FROM dbo.city1 WHERE State=? AND Population BETWEEN ? AND ?"
        cursor.execute(query2, state, total_min, total_max)

        rows = cursor.fetchall()
        for i in rows:
            count+=1
            salpics.append(i)

    return render_template("3)Page.html", total_time=total_time, instance_time=instance_time, count=count, salpics=salpics)


@app.route("/page4a/", methods=['GET', 'POST'])
def page4a():
    instance_time = []
    instance = []
    salpics = []
    redis_time = []

    if request.method == "POST":
        min = request.form['min']
        max = request.form['max']

        for i in range(30):
            instance.append(i + 1)

        query = "SELECT City, State, Population, lat, lon FROM dbo.city1 WHERE Population BETWEEN ? AND ?"
        for i in instance:
            start = time.time()
            cursor.execute(query, min, max)
            end = time.time()
            instance_time.append(end-start)

            rows = cursor.fetchall()
            temp_result = ""
            for j in rows:
                temp_result = temp_result + str(j)
                salpics.append(i)

            redis_client.set(i, temp_result)
            s = time.time()
            temp = redis_client.get(i)
            e = time.time()
            redis_time.append(e - s)

    return render_template("4a)Page.html", redis_time=redis_time, instance_time=instance_time, instance=instance)


@app.route("/page4b/", methods=['GET', 'POST'])
def page4b():
    instance_time = []
    instance = []
    salpics = []
    redis_time = []

    if request.method == "POST":
        num = request.form['num']
        min = request.form['min']
        max = request.form['max']

        for i in range(int(num)):
            instance.append(i + 1)

        query = "SELECT City, State, Population, lat, lon FROM dbo.city1 WHERE Population BETWEEN ? AND ?"
        for i in instance:
            start = time.time()
            cursor.execute(query, min, max)
            end = time.time()
            instance_time.append(end-start)

            rows = cursor.fetchall()
            temp_result = ""
            for j in rows:
                temp_result = temp_result + str(j)
                salpics.append(i)

            redis_client.set(i, temp_result)
            s = time.time()
            temp = redis_client.get(i)
            e = time.time()
            redis_time.append(e - s)

    return render_template("4b)Page.html", redis_time=redis_time, instance_time=instance_time, instance=instance)


if __name__ == "__main__":
    app.run(debug=True)