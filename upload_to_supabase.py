from supabase import create_client, Client
import os

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
MODEL_FILE = "parking_forecast_model.pkl"
BUCKET = "models"

if not SUPABASE_URL or not SUPABASE_KEY:
    raise EnvironmentError("Supabase credentials are missing")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Upload or overwrite
with open(MODEL_FILE, "rb") as f:
    res = supabase.storage.from_(BUCKET).upload(
        path=MODEL_FILE,
        file=f,
        file_options={"content-type": "application/octet-stream"},
        upsert=True  # overwrite if exists
    )

print("✅ Model uploaded to Supabase:", res)