import os
import csv
import tempfile
from flask import Flask, request
from google.cloud import storage
import pymysql

app = Flask(__name__)

# ✅ Env variables
INSTANCE_CONNECTION_NAME = os.environ.get("INSTANCE_CONNECTION_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_NAME = os.environ.get("DB_NAME")

# ✅ DB connection
def get_connection():
    return pymysql.connect(
        unix_socket=f"/cloudsql/{INSTANCE_CONNECTION_NAME}",
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )

# ✅ Health check
@app.route("/", methods=["GET"])
def hello():
    return "Cloud Run is working ✅"


# ✅ Main trigger logic
@app.route("/", methods=["POST"])
def process_file():
    try:
        data = request.get_json()

        bucket_name = data["bucket"]
        file_name = data["name"]

        print(f"Received file: {file_name}")

        # ✅ Condition 1: Orders files
        if (
            "ordersfiles/" in file_name and 
            (file_name.endswith("orders_full.csv") or file_name.endswith("orders_delta.csv"))
        ):
            table_name = "orders_raw"

        # ✅ Condition 2: Customers files
        elif (
            "customersfiles/" in file_name and 
            (file_name.endswith("customers_full.csv") or file_name.endswith("customers_delta.csv"))
        ):
            table_name = "customers"

        # ✅ Ignore unwanted files
        else:
            print("File ignored ❌")
            return "File ignored", 200

        print(f"Processing into table: {table_name}")

        # ✅ Download file
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)

        temp_file = tempfile.NamedTemporaryFile(delete=False)
        blob.download_to_filename(temp_file.name)

        conn = get_connection()
        cursor = conn.cursor()

        with open(temp_file.name, "r") as f:
            reader = csv.DictReader(f)

            # ✅ Insert logic based on table
            if table_name == "orders_raw":
                insert_query = """
                INSERT INTO orders_raw 
                (order_id, order_date, customer_id, region, amount, status)
                VALUES (%s, %s, %s, %s, %s, %s)
                """

                data_to_insert = [
                    (
                        row["order_id"],
                        row["order_date"],
                        row["customer_id"],
                        row["region"],
                        row["amount"],
                        row["status"]
                    )
                    for row in reader
                ]

            elif table_name == "customers":
                insert_query = """
                INSERT INTO customers
                (customer_id, customer_name, region, signup_date, status)
                VALUES (%s, %s, %s, %s, %s)
                """

                data_to_insert = [
                    (
                        row["customer_id"],
                        row["customer_name"],
                        row["region"],
                        row["signup_date"],
                        row["status"]
                    )
                    for row in reader
                ]

            # ✅ Bulk insert (FAST + recommended)
            cursor.executemany(insert_query, data_to_insert)

        conn.commit()
        cursor.close()
        conn.close()

        print("Data loaded successfully ✅")
        return "Success", 200

    except Exception as e:
        print("Error:", str(e))
        return f"Error: {str(e)}", 500
