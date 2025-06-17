# app.py
# A Flask application for AWS Lambda to transpile Trino SQL to StarRocks SQL.
#
# To deploy this to AWS Lambda:
# 1. You will need 'Flask', 'aws-wsgi', and 'sqlglot'.
#    Create a requirements.txt file with:
#    Flask
#    aws-wsgi
#    sqlglot
#
# 2. When configuring your Lambda function in AWS, set the handler to:
#    app.lambda_handler
#    (This tells Lambda to look for the 'lambda_handler' object in the 'app.py' file)
#
# To test the endpoint after deploying, you will use the API Gateway URL provided by AWS.
# For example, using curl:
# curl -X POST -H "Content-Type: application/json" \
#   -d '{"sql_query": "SELECT * FROM my_table"}' \
#   https://<your-api-gateway-id>.execute-api.<region>.amazonaws.com/transpile

from flask import Flask, request, jsonify
import awsgi
from urllib.parse import urlencode
import logging
import json
import sqlglot

# --- Logging Setup ---
logger = logging.getLogger()
logger.setLevel(logging.INFO)
# --- End Logging Setup ---

# Initialize the Flask application
app = Flask(__name__)

@app.route('/', methods=['GET'])
def index():
    """
    Handles GET requests to the root of the application.
    Provides an interactive form to test the /transpile endpoint.
    """
    # HTML content for the interactive form.
    # It includes a form and a script to handle the submission via JavaScript.
    html_content = """
    <!doctype html>
    <html lang="en">
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>SQL Transpiler</title>
        <style>
            body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif; line-height: 1.6; padding: 2em; max-width: 800px; margin: auto; background-color: #f4f4f9; color: #333; }
            h1 { color: #0056b3; }
            form { background: #fff; padding: 2em; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
            label { display: block; margin-bottom: 0.5em; font-weight: bold; }
            textarea { width: 100%; height: 150px; padding: 0.5em; margin-bottom: 1em; border: 1px solid #ccc; border-radius: 4px; box-sizing: border-box; font-family: monospace; }
            button { background-color: #0056b3; color: white; padding: 0.7em 1.5em; border: none; border-radius: 4px; cursor: pointer; font-size: 1em; }
            button:hover { background-color: #004494; }
            #result-container { display: flex; align-items: center; margin-top: 1em; background: #e9ecef; padding: 1em; border-radius: 4px; }
            #result { flex-grow: 1; white-space: pre-wrap; word-wrap: break-word; font-family: monospace;}
            #copy-btn { margin-left: 1em; padding: 0.5em 1em; background-color: #28a745; display: none; }
            #copy-btn:hover { background-color: #218838; }
        </style>
    </head>
    <body>
        <h1>Trino to StarRocks SQL Transpiler</h1>
        <p>Use the form below to convert a Trino SQL query to StarRocks SQL (in lowercase).</p>
        <form id="transpile-form">
            <label for="sql-query">Enter Trino SQL:</label>
            <textarea id="sql-query" name="sql_query" required></textarea>
            <button type="submit">Transpile</button>
        </form>
        <h3>Result (StarRocks SQL):</h3>
        <div id="result-container">
            <pre id="result"></pre>
            <button id="copy-btn">Copy</button>
        </div>

        <script>
            document.getElementById('transpile-form').addEventListener('submit', async function(event) {
                event.preventDefault();
                
                const sqlQuery = document.getElementById('sql-query').value;
                const resultElement = document.getElementById('result');
                const copyBtn = document.getElementById('copy-btn');
                
                resultElement.textContent = 'Processing...';
                copyBtn.style.display = 'none';

                try {
                    const response = await fetch('/transpile', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ sql_query: sqlQuery })
                    });

                    const data = await response.json();

                    if (response.ok) {
                        resultElement.textContent = data.transpiled_sql;
                        copyBtn.style.display = 'inline-block';
                    } else {
                        resultElement.textContent = 'Error: ' + (data.error || 'Unknown error');
                    }
                } catch (error) {
                    resultElement.textContent = 'An error occurred: ' + error.message;
                }
            });

            document.getElementById('copy-btn').addEventListener('click', function() {
                const textToCopy = document.getElementById('result').textContent;
                const textArea = document.createElement('textarea');
                textArea.value = textToCopy;
                document.body.appendChild(textArea);
                textArea.select();
                try {
                    document.execCommand('copy');
                    this.textContent = 'Copied!';
                    setTimeout(() => { this.textContent = 'Copy'; }, 2000);
                } catch (err) {
                    console.error('Failed to copy text: ', err);
                }
                document.body.removeChild(textArea);
            });
        </script>
    </body>
    </html>
    """
    return html_content, 200, {'Content-Type': 'text/html'}


@app.route('/transpile', methods=['POST'])
def transpile_sql():
    """
    This endpoint accepts a POST request with a JSON payload
    containing a Trino SQL query. It transpiles the query to
    StarRocks SQL and returns the result in lowercase.
    """
    if not request.is_json:
        return jsonify({"error": "Request must be JSON"}), 400

    data = request.get_json()

    if 'sql_query' not in data:
        return jsonify({"error": "Missing 'sql_query' in request body"}), 400

    trino_sql = data['sql_query']

    if not isinstance(trino_sql, str) or not trino_sql.strip():
        return jsonify({"error": "'sql_query' must be a non-empty string"}), 400

    try:
        # Transpile the SQL from Trino dialect to StarRocks dialect
        starrocks_sql_list = sqlglot.transpile(trino_sql, read="trino", write="starrocks")
        # Join the list of SQL statements and convert to lowercase
        lowercase_starrocks_sql = " ".join(starrocks_sql_list).lower()

        response_data = {
            "original_trino_sql": trino_sql,
            "transpiled_sql": lowercase_starrocks_sql
        }
        return jsonify(response_data)

    except Exception as e:
        logger.error(f"SQLGlot transpilation error: {e}")
        return jsonify({"error": f"Failed to transpile SQL. Error: {str(e)}"}), 400


def lambda_handler(event, context):
    """
    This function uses awsgi to handle the API Gateway event
    and pass it to the Flask application. It transforms API Gateway's
    HTTP API (payload v2.0) event into a format that awsgi expects.
    """
    if event.get('version') == '2.0':
        path = event.get('rawPath', '/')
        if path == '/{proxy+}':
            path = '/'
        
        transformed_event = {
            'httpMethod': event['requestContext']['http']['method'],
            'path': path, 
            'queryStringParameters': event.get('queryStringParameters'),
            'headers': event.get('headers'),
            'body': event.get('body'),
            'isBase64Encoded': event.get('isBase64Encoded', False),
            'requestContext': event.get('requestContext')
        }
        return awsgi.response(app, transformed_event, context)

    return awsgi.response(app, event, context)
