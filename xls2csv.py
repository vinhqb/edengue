import os
import pandas as pd
import glob
import time
import signal
import random
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import argparse

# Configurable rate limit
rate_limit_per_second = 1

agent_name = "edgcoder_" + str(random.randint(1, 100))

# Initialize geolocator with user_agent
geolocator = Nominatim(user_agent=agent_name)

# Initialize success and fail counters
success_count = 0
fail_count = 0

# Flag to handle graceful shutdown
stop_flag = False

def geocode_address(address):
    try:
        location = geolocator.geocode(address, timeout=10)
        if location:
            return location.latitude, location.longitude
        else:
            return 'NA', 'NA'
    except GeocoderTimedOut:
        return 'NA', 'NA'

def signal_handler(sig, frame):
    global stop_flag
    stop_flag = True
    print("\nGracefully stopping the script. Please wait...")

signal.signal(signal.SIGINT, signal_handler)

def process_excel_files(folder_path):
    global success_count, fail_count, stop_flag
    
    # Get all Excel files starting with 'ED_'
    file_list = glob.glob(os.path.join(folder_path, 'ed_*.xlsx'))

    for file in file_list:
        if stop_flag:
            break

        # Load the Excel file to get the sheet names
        excel_file = pd.ExcelFile(file)
        # Get the name of the first sheet
        first_sheet_name = excel_file.sheet_names[0]

        # Load the first sheet of the Excel file
        df = pd.read_excel(file, sheet_name=first_sheet_name)
       
        # Combine columns 1, 2, 3, and 4 into a new 'address' column
        #df['address'] = df.iloc[:, 1].astype(str) + ',' + df.iloc[:, 2].astype(str) + ',' + df.iloc[:, 3].astype(str)
        df['address'] = df.iloc[:, 2].astype(str) + ',' + df.iloc[:, 3].astype(str)
        
        # Concatenate the spreadsheet name to the 'address' column    
        df['address'] = df['address'] + ',' + first_sheet_name + ',Việt Nam'
        
        # Drop the original columns 1-4
        df.drop(df.columns[1:5], axis=1, inplace=True)

        # Replace spaces in the new file name with underscores
        spreadsheet_name = os.path.splitext(os.path.basename(file))[0]
        new_file_name = os.path.join(folder_path, spreadsheet_name.replace(' ', '_') + '.csv')

        # Initialize latitude and longitude columns
        df['latitude'] = 'NA'
        df['longitude'] = 'NA'

        # Iterate over rows in the DataFrame
        for index in range(len(df)):
            if stop_flag:
                break
            address = df.at[index, 'address']
            lat, lon = geocode_address(address)

            df.at[index, 'latitude'] = lat
            df.at[index, 'longitude'] = lon

            #print(f"\rProcessing {address}: {lat}: {lon} ")

            if lat != 'NA' and lon != 'NA':
                success_count += 1
            else:
                fail_count += 1
                
            print(f"\rProcessing {file}: {index + 1}/{len(df)}: {success_count} successful, {fail_count} failed", end='')
            time.sleep(1 / rate_limit_per_second)  # Rate limiting

            # Save state periodically
            if index % 10 == 0:
                try:
                    df.to_csv(new_file_name, index=False)
                except Exception as e:
                    print(f"\nError saving state file: {e}")
                    return

        # Save final state
        try:
            df.to_csv(new_file_name, index=False)
        except Exception as e:
            print(f"\nError saving final state: {e}")
            return

        # Save the successful geocoded data to a new CSV
        df_successful = df[df['latitude'] != 'NA']
        df_unsuccessful = df[df['latitude'] == 'NA']
        try:
            successful_file_name = os.path.join(folder_path, spreadsheet_name.replace(' ', '_') + '_geocoded.csv')
            df_successful.to_csv(successful_file_name, index=False)
            unsuccessful_file_name = os.path.join(folder_path, spreadsheet_name.replace(' ', '_') + '_recheck.csv')
            df_unsuccessful.to_csv(unsuccessful_file_name, index=False)

        except Exception as e:
            print(f"\nError saving geocoded addresses: {e}")
            return

        # Print the final counts
        print(f"\nFinished processing {file}: {success_count} successful, {fail_count} failed")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process Excel files, geocode addresses, and save the results.")
    parser.add_argument('folder_path', nargs='?', default='./', help="The folder path containing the Excel files.")
    args = parser.parse_args()

    process_excel_files(args.folder_path)
