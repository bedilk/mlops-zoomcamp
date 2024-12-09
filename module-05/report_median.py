import pandas as pd
from evidently.metrics import ColumnQuantileMetric, ColumnDriftMetric, DatasetMissingValuesMetric, ColumnValueRangeMetric
from evidently.report import Report

# Download the data
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-03.parquet"
df = pd.read_parquet(url)

# Convert lpep_pickup_datetime to date
df['date'] = pd.to_datetime(df['lpep_pickup_datetime']).dt.date

# Initialize lists to store daily results
daily_medians = []
daily_drifts = []
daily_missing_values = []
daily_value_ranges = []

# Calculate metrics for each day
for date in df['date'].unique():
    daily_data = df[df['date'] == date]
    
    report = Report(metrics=[
        ColumnQuantileMetric(column_name="fare_amount", quantile=0.5),
        ColumnDriftMetric(column_name="fare_amount"),
        DatasetMissingValuesMetric(),
        ColumnValueRangeMetric(column_name="fare_amount")
    ])
    
    report.run(current_data=daily_data, reference_data=None, column_mapping=None)
    
    result = report.as_dict()
    
    median_fare = result['metrics'][0]['result']['current']['value']
    drift_score = result['metrics'][1]['result']['drift_score']
    missing_values = result['metrics'][2]['result']['current']['share_of_missing_values']
    value_range = result['metrics'][3]['result']['current']
    
    daily_medians.append(median_fare)
    daily_drifts.append(drift_score)
    daily_missing_values.append(missing_values)
    daily_value_ranges.append(value_range)

# Find the maximum median fare
max_median_fare = max(daily_medians)
max_drift_score = max(daily_drifts)
max_missing_values = max(daily_missing_values)

# Calculate overall value range
min_fare = min(range['min'] for range in daily_value_ranges)
max_fare = max(range['max'] for range in daily_value_ranges)

print(f"The maximum daily median fare amount in March 2024 is: ${max_median_fare:.2f}")
print(f"The maximum drift score for fare_amount is: {max_drift_score:.4f}")
print(f"The maximum percentage of missing values is: {max_missing_values*100:.2f}%")
print(f"The overall fare amount range is: ${min_fare:.2f} to ${max_fare:.2f}")