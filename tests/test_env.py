import os
import sys
from pathlib import Path

print("Current working directory:", os.getcwd())
print("Script directory:", Path(__file__).parent.absolute())
print("Environment variables:")
for key, value in os.environ.items():
    if 'NOAA' in key or 'EIA' in key or 'PYTHON' in key or 'PATH' in key:
        print(f"{key} = {value}")

# Try to load .env file
print("\nAttempting to load .env file:")
try:
    from dotenv import load_dotenv
    env_path = Path(__file__).parent / '.env'
    print(f"Trying to load .env from: {env_path}")
    if env_path.exists():
        load_dotenv(env_path)
        print("Successfully loaded .env file")
        print(f"NOAA_API_TOKEN exists: {'NOAA_API_TOKEN' in os.environ}")
        print(f"EIA_API_KEY exists: {'EIA_API_KEY' in os.environ}")
    else:
        print(".env file not found at the expected location")
except Exception as e:
    print(f"Error loading .env: {e}")
