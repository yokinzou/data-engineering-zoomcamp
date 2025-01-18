import psycopg2

conn = psycopg2.connect(
    host="localhost",
    database="airflow",
    user="airflow",
    password="airflow",
    port="5433"
)

cursor = conn.cursor()
cursor.execute("SELECT count(*) FROM yellow_taxi_trips ")
rows = cursor.fetchall()
for row in rows:
    print(row)