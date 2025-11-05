from flask import Flask, jsonify, request, abort
import os
import psycopg2
import time

app = Flask(__name__)

def get_db_connection():
    db_host = os.environ.get('DB_HOST')
    db_name = os.environ.get('DB_NAME')
    db_user = os.environ.get('DB_USER')
    db_pass = os.environ.get('DB_PASS')

    retries = 5
    while retries > 0:
        try:
            conn = psycopg2.connect(
                host=db_host,
                database=db_name,
                user=db_user,
                password=db_pass
            )
            return conn
        except psycopg2.OperationalError:
            retries -= 1
            app.logger.warning("Database not ready, retrying...")
            time.sleep(5)

    app.logger.error("Could not connect to database.")
    return None


@app.route("/db-health", methods=["GET"])
def db_health_check():
    conn = get_db_connection()
    if conn is None:
        return jsonify({"status": "error", "message": "Database connection failed"}), 500

    conn.close()
    return jsonify({"status": "ok", "message": "Database connection successful"})


if __name__ == "__main__":
    app.run(threaded=True, host="0.0.0.0", port=3000)
