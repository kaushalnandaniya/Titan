import pandas as pd
import numpy as np

def generate_data(num_rows, filename):
    print(f"Generating {num_rows} rows for {filename}...")
    data = {
        'id': np.arange(num_rows),
        'age': np.random.randint(18, 80, size=num_rows),
        'salary': np.random.randint(30000, 150000, size=num_rows),
        'department': np.random.choice(['Engineering', 'Sales', 'Marketing', 'HR'], size=num_rows)
    }

    df = pd.DataFrame(data)
    # We set row_group_size to 100,000 to ensure we have multiple partitions for large datasets
    df.to_parquet(filename, engine='pyarrow', row_group_size=100000)
    print(f"Saved {filename}")

if __name__ == "__main__":
    generate_data(10_000, 'test_data_10k.parquet')
    generate_data(100_000, 'test_data_100k.parquet')
    generate_data(1_000_000, 'test_data_1m.parquet')
    generate_data(5_000_000, 'test_data_5m.parquet')
