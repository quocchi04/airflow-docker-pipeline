import msal
import os
import json
import time

def get_access_token(config):
    app = msal.ConfidentialClientApplication(
                    config["client_id"], authority=config["authority"],
                    client_credential=config["secret"],
    )
    token_path = 'secrets/bc_token_cache.json'
    if not os.path.exists(token_path):
        with open(token_path, "w") as f:
            f.write("{}")
            
    with open(token_path, 'r') as token_file:
        try:
            token_cache_json = json.load(token_file)
        except json.JSONDecodeError:
            token_cache_json = {}
    expires_on = token_cache_json.get("expires_on", 0)
    access_token = token_cache_json.get("access_token", "")    
    
    if expires_on and expires_on > int(time.time()):
        print("Using cached access token.")
        return access_token
    else:
        print("Access token expired or not found, fetching a new one.")
        result = None
        result = app.acquire_token_for_client(scopes=config["scope"])
        result["expires_on"] = int(result["expires_in"]) + int(time.time()) - 300
        with open(token_path, "w") as f:
                json.dump(result, f, indent=4)
                f.close()
        if "access_token" not in result:
            print(result.get("error"))
            print(result.get("error_description"))
            print(result.get("correlation_id"))  #
            raise Exception(f"{result.get('error')}")
        return result['access_token']
    
# Thông tin connection
def get_ingestion():
    try:
        from airflow.hooks.base import BaseHook
        ingestion_conn = BaseHook.get_connection("ingestion_conn")
        entra = ingestion_conn.extra_dejson
        return {
            "authority": entra["authority"],
            "client_id": ingestion_conn.login,
            "scope": [ "https://api.businesscentral.dynamics.com/.default" ],
            "secret": ingestion_conn.password,
            "env_name": entra["env_name"],
            "company_id": entra["company_id"],
            "company_name": entra["company_name"]
        }
    except:
        return {
            "authority": os.environ.get('AUTHORITY'),
            "client_id": os.environ.get('CLIENT_ID'),
            "scope": [ "https://api.businesscentral.dynamics.com/.default" ],
            "secret": os.environ.get('SECRET'),
            "env_name": os.environ.get('ENV_NAME'),
            "company_id": os.environ.get('COMPANY_ID'),
            "company_name": os.environ.get('COMPANY_NAME')
        }
def authorization(ingestion_conn):
    return get_access_token(ingestion_conn)