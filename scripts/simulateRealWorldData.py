import pandas as pd
import time
import os
from datetime import datetime

SOURCE_CSV = r'C:\Users\it shop\OneDrive\Desktop\project\data\Agri_yield_prediction.csv'
STREAMING_LANDING_ZONE = r'C:\Users\it shop\OneDrive\Desktop\project\data\raw_sensor_pings'

SENSOR_COLS = [
    'Temperature', 'Humidity', 'Rainfall', 'pH', 'EC', 
    'Solar_Radiation', 'Wind_Speed', 'NDVI', 'EVI'
]

os.makedirs(STREAMING_LANDING_ZONE, exist_ok=True)

def run_simulator():
    try:
        df = pd.read_csv(SOURCE_CSV)
        df.columns = df.columns.str.strip() 
        
        missing = [c for c in SENSOR_COLS if c not in df.columns]
        if missing:
            print(f" Missing sensor columns in CSV: {missing}")
            return

        print(f" Data Loaded. Simulating {len(SENSOR_COLS)} sensor metrics.")
        print(f" Sending batches to {STREAMING_LANDING_ZONE}")

    except Exception as e:
        print(f" Error loading CSV: {e}")
        return

    batch_size = 5 
    
    for i in range(0, len(df), batch_size):
        chunk = df.iloc[i:i+batch_size][SENSOR_COLS].copy()

        chunk['sensor_id'] = 'AGRI-IOT-001'
        chunk['event_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        for col in SENSOR_COLS:
            chunk[col] = pd.to_numeric(chunk[col], errors='coerce').fillna(0)
        
        file_id = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
        file_path = os.path.join(STREAMING_LANDING_ZONE, f"sensor_ping_{file_id}.json")
        
        chunk.to_json(file_path, orient='records', lines=True)
        
        print(f" Sent Batch {i//batch_size + 1} | Records: {len(chunk)} | Time: {chunk['event_time'].iloc[0]}")
        
        time.sleep(5)

if __name__ == "__main__":
    try:
        run_simulator()
    except KeyboardInterrupt:
        print("Simulator Stopped by User.")