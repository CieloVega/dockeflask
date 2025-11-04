from flask import Flask, jsonify

app = Flask(__name__)

@app.route("/")
def index():
    # Change the message to something unique
    return jsonify({"message": "it works! Student Cielo"})

if __name__ == "__main__":
    app.run(threaded=True, host='0.0.0.0', port=3000)
