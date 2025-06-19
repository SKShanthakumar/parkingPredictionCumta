from pydantic import BaseModel

class ForecastRequest(BaseModel):
    station_name: str
    vehicle_type: int  # 0 for twoWheeler, 1 for threeNFourWheeler

class AvailabilityRequest(BaseModel):
    stations: list[ForecastRequest]
    days: int = 1  # Default to 1 day if not specified

