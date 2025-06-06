from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from io import BytesIO
import pandas as pd
import pickle
import os

from supabase import create_client, Client

# Initialize FastAPI app
app = FastAPI(title="Parking Availability Forecast API")

# Supabase config (store in Render's Environment Variables)
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
MODEL_BUCKET = "models"  # change if different
MODEL_FILENAME = "parking_forecast_model.pkl"

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Global variable for model
model = None

# Define input data format
class ForecastRequest(BaseModel):
    periods: int = 48  # number of 15-min steps (i.e., 12 = next 3 hours)
    
# Load model from Supabase Storage
def load_model_from_supabase():
    global model
    try:
        response = supabase.storage.from_(MODEL_BUCKET).download(MODEL_FILENAME)
        model = pickle.load(BytesIO(response))
        return model
        print("✅ Model loaded successfully.")
    except Exception as e:
        print(f"❌ Failed to load model: {e}")
        raise

# Load model at startup
@app.on_event("startup")
async def startup_event():
    load_model_from_supabase()

# Health check
@app.get("/")
def read_root():
    return {"message": "🚀 Forecast API is running"}

# Forecast endpoint
@app.post("/forecast")
def get_forecast(data: ForecastRequest):
    try:
        # Load model and its training data (history)
        import os
        model_path = "/tmp/parking_forecast_model.pkl"  # use /tmp
        if not os.path.exists(model_path):
            # Download from Supabase
            with open(model_path, "wb") as f:
                res = supabase.storage.from_(MODEL_BUCKET).download(MODEL_FILENAME)
                f.write(res)

        with open(model_path, "rb") as f:
            model_data = pickle.load(f)

        #response = supabase.storage.from_(MODEL_BUCKET).download(MODEL_FILENAME)
        #model_data = pickle.load(BytesIO(response))
        model = model_data['model']
        history_df = model_data['history_df']

        # Make future dataframe starting from end of training data
        future = model.make_future_dataframe(history_df, periods=data.periods)
        # Predict
        forecast = model.predict(future)
        # Return the forecasted part only
        forecast_tail = forecast.tail(data.periods)
        result = [
            {
                "timestamp": row['ds'].strftime('%Y-%m-%d %H:%M:%S'),
                "predicted_availability": round(row['yhat1'], 2)
            }
            for _, row in forecast_tail.iterrows()
        ]
        return {"forecast": result}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating forecast: {e}")