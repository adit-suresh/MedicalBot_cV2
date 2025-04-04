import os
from dotenv import load_dotenv

load_dotenv()

# Microsoft Graph API settings
GRAPH_API_ENDPOINT = "https://graph.microsoft.com/v1.0"
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
TENANT_ID = os.getenv("TENANT_ID")
USER_EMAIL = os.getenv("USER_EMAIL")
TARGET_MAILBOX = os.getenv("TARGET_MAILBOX")  


DEEPSEEK_API_KEY = os.getenv('DEEPSEEK_API_KEY', '')
DEEPSEEK_API_URL = os.getenv('DEEPSEEK_API_URL', 'https://api.deepseek.com/v1/vision')
USE_DEEPSEEK_FALLBACK = os.getenv('USE_DEEPSEEK_FALLBACK', 'True').lower() == 'true'

# Email settings
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