import azure.functions as func
import os
import logging
import requests
import json
from datetime import datetime
from azure.storage.blob import BlobServiceClient

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        # Recupera e envia dados para o blob dentro da função
        api_key = os.environ.get("API_KEY")
        steam_id = os.environ.get("STEAM_ID")
        storage_connection_string = os.environ.get("AzureStorageConnection")
        container_name = "steam"

        games = get_game_ids(api_key, steam_id)
        if games:
            send_to_blobstorage(storage_connection_string, container_name, games)

        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
            "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
            status_code=200
        )

def get_game_ids(api_key, steam_id):
    url = f'http://api.steampowered.com/IPlayerService/GetOwnedGames/v0001/?key={api_key}&steamid={steam_id}&format=json'
    response = requests.get(url)
    data = response.json()
    games = data['response']['games']
    return games

def send_to_blobstorage(storage_connection_string, container_name, games):
    try:
        blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)
        container_client = blob_service_client.get_container_client(container_name)
        try:
            container_client.create_container()
            logging.info(f"Container '{container_name}' criado com sucesso.")
        except Exception as e:
            if "ContainerAlreadyExists" not in str(e):
                logging.error(f"Erro ao criar o container '{container_name}': {e}")
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        blob_name = f"inbound/user/{timestamp}.json"
        json_output = json.dumps(games, indent=4, ensure_ascii=False)
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(json_output, overwrite=True)
        logging.info(f"Dados enviados com sucesso para o blob '{blob_name}' no container '{container_name}'.")
    except Exception as e:
        logging.error(f"Erro ao enviar dados para o Blob Storage: {e}")
