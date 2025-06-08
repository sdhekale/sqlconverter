import json
import sqlglot

def handler(event, context):
    try:
        # The request body is a JSON string in the 'body' key
        body = json.loads(event.get("body", "{}"))
        
        if "sql" not in body:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Missing 'sql' key in request body"})
            }

        trino_sql = body["sql"]
        starrocks_sql = sqlglot.transpile(trino_sql, read="trino", write="starrocks")[0]
        
        response_body = {
            "original_query": trino_sql,
            "translated_query": starrocks_sql
        }
        
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(response_body)
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Failed to translate SQL: {str(e)}"})
        }
