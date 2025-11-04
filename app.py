from flask import Flask, jsonify, request, abort

#help
app = Flask(__name__)

# In-memory store
news = [
    {"id": 1, "title": "Initial News", "content": "This is the first article."}
]

# We use a global variable for the counter
global next_id
next_id = 2  # simple auto-increment for IDs


@app.route("/", methods=["GET"])
def index():
    return jsonify({
        "message": "Welcome to the News API!",
        "endpoints": {
            "list_all_news": "GET /news",
            "create_news": "POST /news",
            "update_news": "PUT /news/<id>",
            "delete_news": "DELETE /news/<id>"
        }
    })


@app.route("/news", methods=["GET"])
def list_news():
    return jsonify({"count": len(news), "items": news})


@app.route("/news", methods=["POST"])
def create_news():
    global next_id
    if not request.json or not "title" in request.json:
        abort(400)  # Bad request

    new_item = {
        "id": next_id,
        "title": request.json["title"],
        "content": request.json.get("content", "")
    }
    news.append(new_item)
    next_id += 1
    return jsonify(new_item), 201  # Created


# Helper function to find an item
def find_news_item(item_id):
    for item in news:
        if item["id"] == item_id:
            return item
    return None


@app.route("/news/<int:item_id>", methods=["PUT"])
def update_news(item_id: int):
    item = find_news_item(item_id)
    if not item:
        abort(404)  # Not found
    if not request.json:
        abort(400)

    # Update fields
    if "title" in request.json:
        item["title"] = request.json["title"]
    if "content" in request.json:
        item["content"] = request.json["content"]

    return jsonify(item)


@app.route("/news/<int:item_id>", methods=["DELETE"])
def delete_news(item_id: int):
    item = find_news_item(item_id)
    if not item:
        abort(404)

    news.remove(item)
    return jsonify({"status": "deleted", "id": item_id})


if __name__ == "__main__":
    app.run(threaded=True, host="0.0.0.0", port=3000)

