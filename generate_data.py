import pandas as pd
import numpy as np

# Create a small dataset with 10,000 rows
num_rows = 10000

data = {
    'id': np.arange(num_rows),
    'age': np.random.randint(18, 80, size=num_rows),
    'salary': np.random.randint(30000, 150000, size=num_rows),
    'department': np.random.choice(['Engineering', 'Sales', 'Marketing', 'HR'], size=num_rows)
}

df = pd.DataFrame(data)
df.to_parquet('test_data.parquet', engine='pyarrow')
print("Created test_data.parquet")
