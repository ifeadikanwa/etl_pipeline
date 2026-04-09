import time
from etl import run_pipeline

# run every 60 seconds
while True:
    run_pipeline()
    time.sleep(60)