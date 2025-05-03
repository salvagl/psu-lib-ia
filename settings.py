# src/tu_paquete/settings.py
import os
from dotenv import load_dotenv

load_dotenv()

IPINFO_TOKEN = os.getenv("IPINFO_TOKEN")
CACHE_FILE = os.getenv("CACHE_FILE", "cache.json")
