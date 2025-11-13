from flask import Flask, jsonify, request, abort
import os
import psycopg2
import psycopg2.extras
import time
from werkzeug.exceptions import HTTPException

app = Flask(__name__)


def get_db_connection():
    """
    Intenta conectarse a la base de datos usando variables de entorno:
    DB_HOST, DB_NAME, DB_USER, DB_PASS
    Reintenta varias veces si la base de datos no está lista.
    Devuelve una conexión psycopg2 o None si no se pudo conectar.
    """
    db_host = os.environ.get("DB_HOST")
    db_name = os.environ.get("DB_NAME")
    db_user = os.environ.get("DB_USER")
    db_pass = os.environ.get("DB_PASS")

    if not all([db_host, db_name, db_user, db_pass]):
        app.logger.error("Database environment variables are not fully set.")
        return None

    retries = 5
    delay_seconds = 5
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(
                host=db_host,
                database=db_name,
                user=db_user,
                password=db_pass,
            )
            return conn
        except psycopg2.OperationalError as e:
            app.logger.warning(
                "Database not ready (attempt %d/%d): %s", attempt, retries, e
            )
            if attempt < retries:
                time.sleep(delay_seconds)
    app.logger.error("Could not connect to database after %d attempts.", retries)
    return None


@app.errorhandler(HTTPException)
def handle_http_exception(e):
    """Return JSON for HTTP errors raised with abort(...)."""
    response = e.get_response()
    response.data = jsonify({"error": e.description}).data
    response.content_type = "application/json"
    return response, e.code


@app.route("/db-health", methods=["GET"])
def db_health_check():
    conn = get_db_connection()
    if conn is None:
        return jsonify({"status": "error", "message": "Database connection failed"}), 500
    try:
        conn.close()
    except Exception:
        pass
    return jsonify({"status": "ok", "message": "Database connection successful"}), 200


@app.route("/", methods=["GET"])
def index():
    return jsonify(
        {
            "message": "Welcome to the News API (with Postgres)!",
            "endpoints": {
                "list_all_news": "GET /news",
                "create_news": "POST /news",
                "update_news": "PUT /news/<id>",
                "delete_news": "DELETE /news/<id>",
                "db_health": "GET /db-health",
            },
        }
    )


@app.route("/news", methods=["GET"])
def list_news():
    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Database connection failed"}), 500

    items = []
    try:
        # usar RealDictCursor para obtener diccionarios directamente
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT id, title, content FROM news ORDER BY id;")
            rows = cur.fetchall()
            items = [dict(r) for r in rows]
    except Exception as e:
        app.logger.error("Error listing news: %s", e)
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

    return jsonify({"count": len(items), "items": items}), 200


@app.route("/news", methods=["POST"])
def create_news():
    if not request.is_json:
        abort(400, description="Request must be JSON")
    payload = request.get_json()
    if "title" not in payload or not isinstance(payload["title"], str) or not payload["title"].strip():
        abort(400, description="Missing or invalid 'title' field")

    title = payload["title"].strip()
    content = payload.get("content", "")

    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Database connection failed"}), 500

    new_item = {}
    try:
        with conn.cursor() as cur:
            # usar parámetros para evitar SQL injection
            cur.execute(
                "INSERT INTO news (title, content) VALUES (%s, %s) RETURNING id;",
                (title, content),
            )
            new_id = cur.fetchone()[0]
            conn.commit()
            new_item = {"id": new_id, "title": title, "content": content}
    except Exception as e:
        app.logger.error("Error creating news: %s", e)
        try:
            conn.rollback()
        except Exception:
            pass
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

    return jsonify(new_item), 201


@app.route("/news/<int:item_id>", methods=["PUT"])
def update_news(item_id: int):
    if not request.is_json:
        abort(400, description="Request must be JSON")
    payload = request.get_json()

    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Database connection failed"}), 500

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT title, content FROM news WHERE id = %s;", (item_id,))
            item = cur.fetchone()
            if not item:
                abort(404, description="Item not found")

            # item is a tuple (title, content)
            current_title, current_content = item[0], item[1]
            title = payload.get("title", current_title)
            content = payload.get("content", current_content)

            cur.execute(
                "UPDATE news SET title = %s, content = %s WHERE id = %s;",
                (title, content, item_id),
            )
            conn.commit()
            updated_item = {"id": item_id, "title": title, "content": content}
    except HTTPException:
        # re-raise HTTP exceptions so they are handled by the error handler
        raise
    except Exception as e:
        app.logger.error("Error updating news: %s", e)
        try:
            conn.rollback()
        except Exception:
            pass
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

    return jsonify(updated_item), 200


@app.route("/news/<int:item_id>", methods=["DELETE"])
def delete_news(item_id: int):
    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Database connection failed"}), 500

    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM news WHERE id = %s RETURNING id;", (item_id,))
            deleted = cur.fetchone()
            if not deleted:
                abort(404, description="Item not found")
            conn.commit()
    except HTTPException:
        raise
    except Exception as e:
        app.logger.error("Error deleting news: %s", e)
        try:
            conn.rollback()
        except Exception:
            pass
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

    return jsonify({"status": "deleted", "id": item_id}), 200


if __name__ == "__main__":
    # Opcional: habilitar debug si la variable de entorno DEBUG está establecida a "1"
    debug_flag = os.environ.get("DEBUG", "0") == "1"
    app.run(threaded=True, host="0.0.0.0", port=3000, debug=debug_flag)
