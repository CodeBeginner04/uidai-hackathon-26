import pandas as pd
import os
import glob

current_folder= os.getcwd()
csv_files= glob.glob(os.path.join(current_folder, "*.csv"))

df_list = []

for file in csv_files:
    print(f"Reading file: {file}")
    df = pd.read_csv(file)
    df_list.append(df)


merged_df = pd.concat(df_list, ignore_index=True)
output_file = os.path.join(current_folder, "merged_output.csv")

print(f"All CSV files merged successfully into {output_file}")