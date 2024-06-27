import requests
from azure.storage.blob import BlobServiceClient
from pprint import pprint
from datetime import datetime
import os
import json
from dotenv import load_dotenv

load_dotenv()  # .env 파일에서 환경 변수 로드


class Extract:

    def __init__(self):

        self.API_KEY = os.getenv('API_KEY')
        self.URI = os.getenv('URI')
        self.CONNECTION_STRING = os.getenv('CONNECTION_STRING')
        self.CONTAINER_NAME = os.getenv('CONTAINER_NAME')
        self.BLOB_NAME = os.getenv('AZURE_BLOB_NAME')

    def extract_data(self):
        headers = {'x-nxopen-api-key': self.API_KEY}
        result = requests.get(self.URI, headers=headers)
        raw_data = result.json()
        pprint(raw_data)

        return raw_data

    def load_data(self):

        print("load_data")

        blob_service_client = BlobServiceClient.from_connection_string(self.CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(self.CONTAINER_NAME)

        raw_data = self.extract_data()
        today_date = datetime.today()
        filename = f"nx_extract_data-{today_date}.json"

        try:
            blob_client = container_client.get_blob_client(filename)
            data_json = json.dumps(raw_data, indent=4, sort_keys=True, ensure_ascii = False)
            blob_client.upload_blob(data_json, blob_type="BlockBlob")

            return "Upload successful"

        except Exception as e:
            return f"An error occurred while uploading to Blob Storage: {str(e)}"


if __name__ == "__main__":
    extra = Extract()
    extra.extract_data()
    extra.load_data()