import pandas as pd
from evidently.metrics import ColumnQuantileMetric
from evidently.report import Report


url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-03.parquet"
df = pd.read_parquet(url)

# Convert lpep_pickup_datetime to date
df['date'] = pd.to_datetime(df['lpep_pickup_datetime']).dt.date

# Initialize a list to store daily results
daily_medians = []

# Calculate median fare amount for each day
for date in df['date'].unique():
    daily_data = df[df['date'] == date]
    
    report = Report(metrics=[
        ColumnQuantileMetric(column_name="fare_amount", quantile=0.5)
    ])
    
    report.run(current_data=daily_data, reference_data=None, column_mapping=None)
    
    result = report.as_dict()
    
    median_fare = result['metrics'][0]['result']['current']['value']
    daily_medians.append(median_fare)

# Find the maximum median fare
max_median_fare = max(daily_medians)

print(f"The maximum daily median fare amount in March 2024 is: ${max_median_fare:.2f}")

