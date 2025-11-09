import boto3
import requests
from requests_aws4auth import AWS4Auth
import base64
import urllib.parse
import json

region = 'us-east-1'
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

host = 'https://vpc-docsearch-dev1-h457rgb4sc6pm7kqfxeencedda.us-east-1.es.amazonaws.com'
index = 'documents'
url = f"{host}/{index}/_search"

def get_from_Search(query):
    headers = {"Content-Type": "application/json"}
    r = requests.get(url, auth=awsauth, headers=headers, data=json.dumps(query))
    return r.text


def lambda_handler(event, context):
    try:
        print("Event is", event)

        term = None

        # Caso 1: viene como parámetro GET /?q=EC2
        if event.get("queryStringParameters"):
            term = event["queryStringParameters"].get("q")

        # Caso 2: viene en body codificado (POST desde HTML)
        elif event.get("body"):
            try:
                bodyData = base64.b64decode(event["body"])
                formDict = urllib.parse.parse_qs(bodyData.decode('utf-8'))
                term = formDict.get('searchTerm', [None])[0]
            except Exception as e:
                print("Body parse error:", e)

        if not term:
            print("No search term provided.")
            return {"statusCode": 400, "body": json.dumps({"error": "missing search term"})}

        print("Search term:", term)

        # --- QUERY ---
        query = {
            "size": 25,
            "query": {
                "multi_match": {
                    "query": term,
                    "fields": ["Title^3", "Body", "Summary"],
                    "fuzziness": "AUTO"
                }
            }
        }

        print("Sending query to OpenSearch...")
        response = get_from_Search(query)
        response_json = json.loads(response)
        print("Response JSON received")

        hits = response_json.get("hits", {}).get("hits", [])

        if not hits:
            print("No results found for:", term)
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "message": f"No documents found for '{term}'",
                    "results": []
                })
            }

        first_hit = hits[0]["_source"]
        print("First hit:", json.dumps(first_hit, indent=2))

        return {
            "statusCode": 200,
            "body": json.dumps(hits)
        }

    except Exception as e:
        print("Exception is", str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({
                "status": False,
                "message": str(e)
            })
        }
