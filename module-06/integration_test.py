import os
import pandas as pd

def read_data(filename, options):
    df = pd.read_parquet(filename, storage_options=options)
    return df

def run_integration_test():
    options = {
        'client_kwargs': {
            'endpoint_url': 'http://localhost:4566'
        }
    }

    input_file = 's3://zoomcamp-nyc-duration/in/2023-01.parquet'
    output_file = 's3://zoomcamp-nyc-duration/out/2023-01.parquet'

    # Run the batch.py script
    os.system('python batch.py 2023 1')

    # Read the output file
    df_output = read_data(output_file, options)

    # Calculate and print the sum of predicted durations
    sum_predicted = df_output['predicted_duration'].sum()
    print(f"Sum of predicted durations: {sum_predicted:.2f}")

if __name__ == "__main__":
    run_integration_test()
