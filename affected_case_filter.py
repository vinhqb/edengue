import pandas as pd
import sys
from datetime import datetime

def main(affected_regions_file, original_file, output_file):
    # Load the affected regions CSV file
    affected_df = pd.read_csv(affected_regions_file)

    # Load the original dataframe with date parsing
    original_df = pd.read_csv(original_file, parse_dates=['year'], dayfirst=True)

    # Extract the year from the date in the original dataframe
    original_df['year'] = original_df['year'].dt.year

    # Create a list to hold the filtered data
    filtered_data = []

    # Iterate over each row in the affected regions dataframe
    for _, affected_row in affected_df.iterrows():
        affected_district = affected_row['Affected_District']
        affected_province = affected_row['Affected_Province']
        affected_year = affected_row['Year']
        
        # Filter the original dataframe
        filtered_df = original_df[
            (original_df['Huyen'] == affected_district) &
            (original_df['Tinh'] == affected_province) &
            ((original_df['VaoVien'] <= affected_year) |
             (original_df['RaVien'] <= affected_year))             
        ]
        
        # Append the filtered rows to the list
        filtered_data.append(filtered_df)

    # Concatenate all the filtered data into a single dataframe
    final_filtered_df = pd.concat(filtered_data)

    # Save the filtered dataframe to the new CSV file
    final_filtered_df.to_csv(output_file, index=False)

    print(f"Filtered data has been saved to {output_file}")

if __name__ == '__main__':
    if len(sys.argv) != 4:
        print("Usage: python affected_case_filter.py <affected_regions_file> <original_file> <output_file>")
    else:
        affected_regions_file = sys.argv[1]
        original_file = sys.argv[2]
        output_file = sys.argv[3]
        main(affected_regions_file, original_file, output_file)
