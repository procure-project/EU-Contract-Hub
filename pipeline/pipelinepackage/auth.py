from dotenv import load_dotenv
import os

load_dotenv()

def get_opensearch_auth():
    username = os.getenv("OPENSEARCH_USERNAME")
    password = os.getenv("OPENSEARCH_PASSWORD")
    if not username or not password:
        raise ValueError("Missing OpenSearch credentials in .env file or environment variables")
    return (username, password)