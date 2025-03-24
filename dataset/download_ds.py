import kagglehub
from dotenv import load_dotenv

# Create .env file with KAGGLEHUB_CACHE="SPECIFY_OUTPUT_DIRECTORY_PATH"

# Load local .env to specify the download folder pah
load_dotenv()

# Download the dataset
path = kagglehub.dataset_download("olistbr/brazilian-ecommerce")

print("Path to dataset files:", path)
