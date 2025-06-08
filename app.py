import sqlglot
from flask import Flask, request, jsonify

# Create the Flask app instance
app = Flask(__name__)

@app.route("/translate", methods=["POST"])
def translate_sql():
    """
    Translates a Trino SQL query to StarRocks SQL.
    Expects a JSON payload with the key "sql".
    e.g., {"sql": "SELECT CAST(ts AS TIMESTAMP) FROM my_table"}
    """
    # Get the JSON data from the request body
    data = request.get_json()

    if not data or "sql" not in data:
        return jsonify({"error": "Missing 'sql' key in request body"}), 400

    trino_sql = data["sql"]

    try:
        # Perform the translation using SQLGlot
        # The result is a list, so we take the first element
        starrocks_sql = sqlglot.transpile(trino_sql, read="trino", write="starrocks")[0]
        
        # Return the successful conversion in a JSON response
        return jsonify({
            "original_query": trino_sql,
            "translated_query": starrocks_sql
        })

    except Exception as e:
        # Handle potential errors during transpilation
        return jsonify({"error": f"Failed to translate SQL: {str(e)}"}), 500

# This is the magic part!
# Mangum is an adapter for running ASGI applications in AWS Lambda.
# It will wrap our Flask (WSGI) app to make it compatible.
from mangum import Mangum
handler = Mangum(app)
