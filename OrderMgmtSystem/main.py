import os
import csv
import tempfile
from flask import Flask, request
from google.cloud import storage
import pymysql

app = Flask(__name__)

# Environment variables (set in Cloud Run)
INSTANCE_CONNECTION_NAME = os.environ.get("INSTANCE_CONNECTION_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_NAME = os.environ.get("DB_NAME")


# ✅ MySQL Connection using Cloud SQL socket
def get_connection():
    return pymysql.connect(
        unix_socket=f"/cloudsql/{INSTANCE_CONNECTION_NAME}",
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )


# ✅ Test endpoint (VERY IMPORTANT for initial validation)
@app.route("/", methods=["GET"])
def hello():
    return "Cloud Run is working ✅"


# ✅ Main ingestion endpoint (triggered by GCS)
@app.route("/", methods=["POST"])
def load_to_mysql():
    try:
        data = request.get_json()

        bucket_name = data["bucket"]
        file_name = data["name"]

        print(f"Processing file: {file_name}")

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)

        # Download file locally
        temp_file = tempfile.NamedTemporaryFile(delete=False)
        blob.download_to_filename(temp_file.name)

        conn = get_connection()
        cursor = conn.cursor()

        with open(temp_file.name, "r") as f:
            reader = csv.DictReader(f)

            for row in reader:
                query = """
                INSERT INTO orders_raw
                (order_id, order_date, customer_id, region, amount, status)
                VALUES (%s, %s, %s, %s, %s, %s)
                """

                cursor.execute(query, (
                    row["order_id"],
                    row["order_date"],
                    row["customer_id"],
                    row["region"],
                    row["amount"],
                    row["status"]
                ))

        conn.commit()
        cursor.close()
        conn.close()

        return "File processed successfully ✅"

    except Exception as e:
        print("Error:", str(e))
        return f"Error: {str(e)}", 500
    