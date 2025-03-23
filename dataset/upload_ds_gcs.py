from google.cloud import storage
import os

# Path to your service account key file
service_account_key_path = "/var/tmp/dsai-module-2-project-25282646cfd3.json"

# Set your bucket name
bucket_name = "dsai-module-2-project-ds"

# Set the path to the folder containing your CSV files
source_folder = "/var/tmp/datasets/olistbr/brazilian-ecommerce/versions/2"

# Initialize the Google Cloud Storage client with service account credentials
storage_client = storage.Client.from_service_account_json(service_account_key_path)
bucket = storage_client.bucket(bucket_name)

# Iterate through files in the source folder
for filename in os.listdir(source_folder):
    if filename.endswith(".csv"):
        # Construct the full local file path
        source_path = os.path.join(source_folder, filename)

        # Construct the destination blob name in the bucket
        blob_name = filename

        # Upload the file to the bucket
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(source_path)

        print(f"Uploaded {filename} to gs://{bucket_name}/{blob_name}")

print("All CSV files copied successfully!")