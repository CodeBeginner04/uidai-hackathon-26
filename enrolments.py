import pandas as pd
import os

# Define the directory path containing your CSV files
directory_path = r"F:\UIDAI Hackathon'26\api_data_aadhar_enrolment"

# Get all CSV files from the directory
csv_files = [f for f in os.listdir(directory_path) if f.endswith('.csv')]

# Sort files to maintain order (optional)
csv_files.sort()

print(f"Found {len(csv_files)} CSV files to merge:")
for file in csv_files:
    print(f"  - {file}")

# List to store individual dataframes
dataframes = []

# Read each CSV file and append to the list
for file in csv_files:
    file_path = os.path.join(directory_path, file)
    try:
        # Try UTF-8 first
        df = pd.read_csv(file_path)
        dataframes.append(df)
        print(f"✓ Successfully read: {file} ({len(df)} rows)")
    except UnicodeDecodeError:
        try:
            # Try latin-1 encoding
            df = pd.read_csv(file_path, encoding='latin-1')
            dataframes.append(df)
            print(f"✓ Successfully read (latin-1): {file} ({len(df)} rows)")
        except Exception as e:
            print(f"✗ Error reading {file}: {e}")
    except Exception as e:
        print(f"✗ Error reading {file}: {e}")

# Merge all dataframes
if dataframes:
    merged_df = pd.concat(dataframes, ignore_index=True)
    
    print(f"\n✓ Merge completed!")
    print(f"Total rows in merged dataset: {len(merged_df)}")
    print(f"Total columns: {len(merged_df.columns)}")
    print(f"Column names: {list(merged_df.columns)}")
    
    # Save the merged dataframe
    output_file = r"F:\UIDAI Hackathon'26\merged_aadhar_enrolment_data.csv"
    merged_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"\n✓ Merged file saved as: {output_file}")
    
    # Display first few rows
    print("\nFirst 5 rows of merged data:")
    print(merged_df.head())
    
else:
    print("No dataframes to merge!")