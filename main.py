from flask import Flask, jsonify, request
from azure.storage.blob import BlobServiceClient
import pandas as pd
from io import StringIO
import json

# Initialize Flask app
app = Flask(__name__)

# Set your Azure Blob Storage connection string
AZURE_STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=narula12storage;AccountKey=s8rUHL11ngvXxzJMatsIPT1UKaQsXMw61lKTTb7xA4bM2AawsFIpuf0I4Ty5rwsPpqg4t6IDGe6c+AStCavGIg==;EndpointSuffix=core.windows.net"

# Initialize the BlobServiceClient
blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)

@app.route("/get-file/<container_name>/<file_name>", methods=["GET"])
def get_file(container_name, file_name):
    try:
        # Get container client
        container_client = blob_service_client.get_container_client(container_name)
        
        # Get blob client for the specified file
        blob_client = container_client.get_blob_client(file_name)
        
        # Download blob content
        blob_data = blob_client.download_blob().readall()
        
        # Load blob data into a DataFrame
        df = pd.read_csv(StringIO(blob_data.decode('utf-8')))
        
        # Convert DataFrame to JSON
        data_json = df.to_json(orient="records")
        
        return jsonify(json.loads(data_json))  # Return as JSON-compatible dict

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Run the Flask app
if __name__ == "__main__":
    app.run(debug=True)
