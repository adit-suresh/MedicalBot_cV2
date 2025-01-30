import os
from dotenv import load_dotenv

load_dotenv()

# Microsoft Graph API settings
GRAPH_API_ENDPOINT = "https://graph.microsoft.com/v1.0"
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
TENANT_ID = os.getenv("TENANT_ID")
SCOPE = ["https://graph.microsoft.com/.default"]

# Email settings
EMAIL_FOLDER = "inbox"
ATTACHMENT_TYPES = [".pdf", ".xlsx", ".xls", ".jpg", ".jpeg", ".png"]
MAX_EMAIL_FETCH = 50  # Maximum number of emails to fetch in one go

# File paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DATA_DIR = os.path.join(BASE_DIR, "data", "raw")
PROCESSED_DATA_DIR = os.path.join(BASE_DIR, "data", "processed")
LOG_DIR = os.path.join(BASE_DIR, "logs")

# Create necessary directories
for directory in [RAW_DATA_DIR, PROCESSED_DATA_DIR, LOG_DIR]:
    os.makedirs(directory, exist_ok=True)

