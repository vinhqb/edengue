# This program read the line listing and create a new line listing csv file with the following fields
# Maso: the unique case ID
# L2Addr: the level 2 address of the case
# The output file will be used to geocode dengue cases

import pandas as pd
import sys
import os

# Function to map MaTinh to Tinh
def map_ma_tinh(ma_tinh):
    mapping = {
        "AGG": "An Giang",
        "HGG": "Hậu Giang",
        "BLU": "Bạc Liêu",
        "BTE": "Bến Tre",
        "CTO": "Cần Thơ",
        "CMU": "Cà Mau",
        "DTP": "Đồng Tháp",
        "HGG": "Hậu Giang",
        "KGG": "Kiên Giang",
        "LAN": "Long An",
        "STG": "Sóc Trăng",
        "TGG": "Tiền Giang",
        "THV": "Trà Vinh",
        "VLG": "Vĩnh Long"        
    }
    return mapping.get(ma_tinh, "")

def main():
    # Check if the input filename is provided
    if len(sys.argv) != 2:
        print("Usage: python script.py <input_csv_file>")
        sys.exit(1)

    # Get the input filename from the command line
    input_csv_file = sys.argv[1]

    # Ensure the input file exists
    if not os.path.isfile(input_csv_file):
        print(f"Error: The file '{input_csv_file}' does not exist.")
        sys.exit(1)

    # Read the CSV file
    df = pd.read_csv(input_csv_file)

    # Create a new DataFrame for rows with valid MaTinh
    df = df[df['MaTinh'].notnull()]

    # Create the address field
    df['L2Addr'] = df.apply(lambda row: f"{row['Xa']}, {row['Huyen']}, {map_ma_tinh(row['MaTinh'])}", axis=1)

    # Create a new DataFrame with key fields
    new_df = df[['MaSo', 'L2Addr', 'VaoVien', 'RaVien', 'DiaChi', 'Ap', 'Xa', 'Huyen', 'MaTinh']]

    # Create the output filename based on the input filename
    input_filename, input_extension = os.path.splitext(input_csv_file)
    output_csv_file = f"{input_filename}_L2Addr{input_extension}"

    # Write the new DataFrame to a new CSV file
    new_df.to_csv(output_csv_file, index=False)

    print(f"Success: new CSV file '{output_csv_file}' has been created.")

if __name__ == "__main__":
    main()
