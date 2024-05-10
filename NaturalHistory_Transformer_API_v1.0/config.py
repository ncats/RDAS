
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

class Config:
    MODEL_PATH = os.getenv('MODEL_PATH')
    HOST = os.getenv('HOST', '0.0.0.0')  # Default value '0.0.0.0' if HOST is not defined
    PORT = int(os.getenv('PORT', 5000))  # Default value 5000 if PORT is not defined
    DEBUG = os.getenv('DEBUG', 'True').lower() == 'true'  # Default value True if DEBUG is not defined or not 'False'

