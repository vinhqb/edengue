import pandas as pd
import time
import signal
import sys
import os
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import argparse

# Configurable rate limit
rate_limit_per_second = 1

# Initialize geolocator with user_agent
geolocator = Nominatim(user_agent="geoapiExercises")

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

def main(input_csv):
    global success_count, fail_count, stop_flag

    # Check for existing state file
    state_file = 'geocoding_state.csv'
    if os.path.exists(state_file):
        try:
            df = pd.read_csv(state_file)
        except pd.errors.EmptyDataError:
            print(f"Error: {state_file} is empty or corrupted.")
            return
        success_count = len(df[df['latitude'] != 'NA'])
        fail_count = len(df[df['latitude'] == 'NA'])
        start_index = df.index.max() + 1
    else:
        try:
            df = pd.read_csv(input_csv)
        except FileNotFoundError:
            print(f"Error: File {input_csv} not found.")
            return
        except pd.errors.EmptyDataError:
            print(f"Error: {input_csv} is empty or corrupted.")
            return
        except pd.errors.ParserError:
            print(f"Error: File {input_csv} has an incorrect structure.")
            return
        
        if 'address' not in df.columns:
            print("Error: The input CSV file must contain an 'address' column.")
            return

        df['latitude'] = 'NA'
        df['longitude'] = 'NA'
        start_index = 0

    # Iterate over rows in the DataFrame starting from last processed index
    for index in range(start_index, len(df)):
        if stop_flag:
            break
        lat, lon = geocode_address(df.at[index, 'address'])
        df.at[index, 'latitude'] = lat
        df.at[index, 'longitude'] = lon
        if lat != 'NA' and lon != 'NA':
            success_count += 1
        else:
            fail_count += 1
        print(f"\rProcessed {index + 1}/{len(df)}: {success_count} successful, {fail_count} failed", end='')
        time.sleep(1 / rate_limit_per_second)  # Rate limiting

        # Save state periodically
        if index % 10 == 0:
            try:
                df.to_csv(state_file, index=False)
            except Exception as e:
                print(f"\nError saving state file: {e}")
                return

    # Save state before exiting
    try:
        df.to_csv(state_file, index=False)
    except Exception as e:
        print(f"\nError saving state file: {e}")
        return

    # Remove successfully geocoded addresses and save remaining to the original CSV
    df_unsuccessful = df[df['latitude'] == 'NA'].drop(columns=['latitude', 'longitude'])
    try:
        df_unsuccessful.to_csv(input_csv, index=False)
    except Exception as e:
        print(f"\nError saving unsuccessful addresses: {e}")
        return

    # Save the successful geocoded data to a new CSV
    df_successful = df[df['latitude'] != 'NA']
    try:
        df_successful.to_csv('addresses_geocoded.csv', index=False)
    except Exception as e:
        print(f"\nError saving geocoded addresses: {e}")
        return

    # Print the final counts
    print(f"\nFinal count: {success_count} successful, {fail_count} failed")

    # Clean up state file if processing completed
    if not stop_flag:
        try:
            os.remove(state_file)
        except OSError as e:
            print(f"\nError removing state file: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Geocode addresses from a CSV file.")
    parser.add_argument('input_csv', type=str, help="The input CSV file containing addresses.")
    args = parser.parse_args()

    main(args.input_csv)
