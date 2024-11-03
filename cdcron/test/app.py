from flask import Flask, jsonify, request

app = Flask(__name__)


@app.route("/api/get", methods=["GET"])
def http_get():
    return jsonify(message="GET request received"), 200


@app.route("/api/post", methods=["POST"])
def http_post():
    data = request.json
    return jsonify(message="POST request received", data=data), 200


@app.route("/api/put", methods=["PUT"])
def http_put():
    data = request.json
    return jsonify(message="PUT request received", data=data), 200


@app.route("/api/delete", methods=["DELETE"])
def http_delete():
    return jsonify(message="DELETE request received"), 200


@app.route("/api/patch", methods=["PATCH"])
def http_patch():
    data = request.json
    return jsonify(message="PATCH request received", data=data), 200


@app.route("/api/head", methods=["HEAD"])
def http_head():
    return "", 200


@app.route("/api/options", methods=["OPTIONS"])
def http_options():
    return "", 200


@app.route("/api/trace", methods=["TRACE"])
def http_trace():
    return "", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
