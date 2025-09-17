from sdv.single_table import GaussianCopulaSynthesizer
from sdv.metadata import SingleTableMetadata
import pandas as pd
import numpy as np

# Create sample data with some missing values
print("Creating sample data...")
sample_data = pd.DataFrame({
    'id': range(1, 101),  # 1000 rows
    'age': np.random.randint(18, 80, 100),
    'income': np.random.normal(50000, 15000, 100),
    'city': np.random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix'], 100),
    'score': np.random.uniform(0, 100, 100)  # This column will have 20% missing values
})

# Introduce 20% missing values in the 'score' column
missing_indices = np.random.choice(sample_data.index, size=int(0.2 * len(sample_data)), replace=False)
sample_data.loc[missing_indices, 'score'] = np.nan

print(f"Sample data shape: {sample_data.shape}")
print(f"Missing values in 'score' column: {sample_data['score'].isna().sum()} ({sample_data['score'].isna().mean():.1%})")
print("\nFirst 10 rows of sample data:")
print(sample_data.head(10))

# Create metadata for the single table
print("\nCreating metadata...")
metadata = SingleTableMetadata()
metadata.detect_from_dataframe(sample_data)

# The missing values proportion is automatically detected from the data
# No need to manually set it - SDV will learn the pattern from the training data

print("Metadata created successfully!")
print(f"Column names: {metadata.get_column_names()}")

# Initialize the GaussianCopulaSynthesizer
print("\nInitializing synthesizer...")
synthesizer = GaussianCopulaSynthesizer(metadata)

# Fit the synthesizer to the sample data
print("Fitting synthesizer to data...")
synthesizer.fit(sample_data)

# Generate synthetic data
print("Generating synthetic data...")
synthetic_data = synthesizer.sample(num_rows=1000)

print(f"\nSynthetic data shape: {synthetic_data.shape}")
print(f"Missing values in 'score' column: {synthetic_data['score'].isna().sum()} ({synthetic_data['score'].isna().mean():.1%})")

print("\nFirst 10 rows of synthetic data:")
print(synthetic_data.head(10))

print("\nSummary statistics:")
print(synthetic_data.describe())

# Save the synthetic data to CSV
output_file = 'synthetic_data_with_missing.csv'
synthetic_data.to_csv(output_file, index=False)
print(f"\nSynthetic data saved to: {output_file}")
