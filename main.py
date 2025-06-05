# main.py
from fastapi import FastAPI
from pydantic import BaseModel
import pandas as pd
from datetime import datetime
import pickle

# Load model and its training data (history)
with open("parking_forecast_model.pkl", "rb") as f:
    model_data = pickle.load(f)

model = model_data['model']
history_df = model_data['history_df']

app = FastAPI()

class ForecastRequest(BaseModel):
    periods: int = 48  # number of 15-min steps (i.e., 12 = next 3 hours)

@app.post("/forecast")
def forecast_parking(req: ForecastRequest):
    # Make future dataframe starting from end of training data
    future = model.make_future_dataframe(history_df, periods=req.periods)

    # Predict
    forecast = model.predict(future)

    # Return the forecasted part only
    forecast_tail = forecast.tail(req.periods)

    result = [
        {
            "timestamp": row['ds'].strftime('%Y-%m-%d %H:%M:%S'),
            "predicted_availability": round(row['yhat1'], 2)
        }
        for _, row in forecast_tail.iterrows()
    ]
    return {"forecast": result}
