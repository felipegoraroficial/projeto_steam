import azure.functions as func
import logging
import os
import json
import requests
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
        storage_connection_string = os.environ.get("AzureStorageConnection")
        container_name = "steam"

        blob_list = get_latest_json_data_as_list(storage_connection_string, container_name, "inbound/user")
        if blob_list:
            get_game_ids(storage_connection_string, container_name, blob_list)

        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
            "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
            status_code=200
        )

def get_latest_json_data_as_list(storage_connection_string, container_name, directory_path):
    try:
        blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)
        container_client = blob_service_client.get_container_client(container_name)
        blobs_list = container_client.list_blobs(name_starts_with=directory_path)

        json_blobs = [blob for blob in blobs_list if blob.name.endswith('.json')]

        if not json_blobs:
            logging.info("Nenhum arquivo JSON encontrado no diretório.")
            return []

        latest_blob = max(json_blobs, key=lambda b: b.last_modified)

        # Baixa o conteúdo do blob JSON
        blob_client = container_client.get_blob_client(latest_blob.name)
        blob_data = blob_client.download_blob().readall()

        # Carrega o JSON em uma lista
        data = json.loads(blob_data)

        # Caso o JSON não seja uma lista, converter para lista, por ex. se for dict, pode:
        if isinstance(data, dict):
            # Exemplo: converter para lista de valores
            data = list(data.values())

        return data

    except Exception as e:
        logging.error(f"Erro ao obter ou processar o blob JSON mais recente: {e}")
        return []

def send_to_blobstorage(storage_connection_string, container_name, games, name):
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
        blob_name = f"inbound/details/{name}.json"
        json_output = json.dumps(games, indent=4, ensure_ascii=False)
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(json_output, overwrite=True)
        logging.info(f"Dados enviados para o blob '{blob_name}' no container '{container_name}'.")
    except Exception as e:
        logging.error(f"Erro no envio ao Blob Storage: {e}")

def get_game_ids(storage_connection_string, container_name, blobs_list):

    appids = [item['appid'] for item in blobs_list if 'appid' in item]

    for blob_name in appids:
        app_details_response = requests.get(f'https://store.steampowered.com/api/appdetails?appids={blob_name}')
        app_details_data = app_details_response.json()
        for value in app_details_data.values():
            send_to_blobstorage(storage_connection_string, container_name, value, blob_name)

