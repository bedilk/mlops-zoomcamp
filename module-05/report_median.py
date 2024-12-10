import pandas as pd
from evidently.report import Report
from evidently.metrics import ColumnQuantileMetric

# Assuming val_data is already defined and preprocessed

# Ensure we have a date column
val_data['date'] = pd.to_datetime(val_data['lpep_pickup_datetime']).dt.date

daily_medians = []

for date in val_data['date'].unique():
    daily_data = val_data[val_data['date'] == date]
    
    report = Report(metrics=[
        ColumnQuantileMetric(column_name="fare_amount", quantile=0.5)
    ])
    
    report.run(reference_data=train_data, current_data=daily_data, column_mapping=column_mapping)
    
    result = report.as_dict()
    
    # Extract the median fare for this day
    median_fare = result['metrics'][0]['result']['current']['value']
    daily_medians.append((date, median_fare))

# Find the maximum median fare
max_median_fare = max(daily_medians, key=lambda x: x[1])

print(f"The maximum daily median fare amount in March 2024 is: ${max_median_fare[1]:.2f}")
print(f"This occurred on: {max_median_fare[0]}")

# Print all daily medians for verification
print("\nAll daily median fares:")
for date, median in sorted(daily_medians):
    print(f"Date: {date}, Median fare: ${median:.2f}")

# Print the top 5 highest daily median fares
print("\nTop 5 highest daily median fares:")
for date, median in sorted(daily_medians, key=lambda x: x[1], reverse=True)[:5]:
    print(f"Date: {date}, Median fare: ${median:.2f}")