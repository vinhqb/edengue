import json
import re
import os
import csv
import time
import requests
import random
import signal
import urllib.parse
import pandas as pd
from datetime import datetime, timedelta
from pymongo import MongoClient
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut

# Set this directive to False to stop printing debugging information
__DEBUG__ = True

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
        "KGG": "Kiên Giang",
        "LAN": "Long An",
        "STG": "Sóc Trăng",
        "TGG": "Tiền Giang",
        "THV": "Trà Vinh",
        "VLG": "Vĩnh Long"
    }
    return mapping.get(ma_tinh, "")

# Function to convert VNI text to Unicode
def vni2unicode(text):
    """Convert VNI characters to Unicode characters."""
    uniChars1 = ['Ấ', 'ấ', 'Ầ', 'ầ', 'Ẩ', 'ẩ', 'Ẫ', 'ẫ', 'Ậ', 'ậ', 'Ắ', 'ắ',
                 'Ằ', 'ằ', 'Ẳ', 'ẳ', 'Ẵ', 'ẵ', 'Ặ', 'ặ', 'Ế', 'ế', 'Ề', 'ề', 'Ể', 'ể',
                 'Ễ', 'ễ', 'Ệ', 'ệ', 'Ố', 'ố', 'Ồ', 'ồ', 'Ổ', 'ổ', 'Ỗ', 'ỗ',
                 'Ộ', 'ộ', 'Ớ', 'ớ', 'Ờ', 'ờ', 'Ở', 'ở', 'Ỡ', 'ỡ',
                 'Ợ', 'ợ', 'Ứ', 'ứ', 'Ừ', 'ừ',
                 'Ử', 'ử', 'Ữ', 'ữ', 'Ự', 'ự']
    vniChars1 = ['AÁ', 'aá', 'AÀ', 'aà', 'AÅ', 'aå', 'AÃ', 'aã', 'AÄ', 'aä', 'AÉ', 'aé',
                 'AÈ', 'aè', 'AÚ', 'aú', 'AÜ', 'aü', 'AË', 'aë', 'EÁ', 'eá', 'EÀ', 'eà', 'EÅ', 'eå',
                 'EÃ', 'eã', 'EÄ', 'eä', 'OÁ', 'oá', 'OÀ', 'oà', 'OÅ', 'oå', 'OÃ', 'oã',
                 'OÄ', 'oä', 'ÔÙ', 'ôù', 'ÔØ', 'ôø', 'ÔÛ', 'ôû', 'ÔÕ', 'ôõ',
                 'ÔÏ', 'ôï', 'ÖÙ', 'öù', 'ÖØ', 'öø',
                 'ÖÛ', 'öû', 'ÖÕ', 'öõ', 'ÖÏ', 'öï']
    for vni, uni in zip(vniChars1, uniChars1):
        text = re.sub(vni, uni, text)
    return text

# Check if the columns match the required format
def has_required_columns(df):
    required_columns = ['MaSo', 'MaNoiBC', 'Ho', 'Ten', 'Gioi', 'Tuoi', 'NgaySinh', 'DiaChi', 'Ap', 'Xa', 'Huyen',
                        'MaTinh', 'TenCha', 'LayMauXN', 'ELISA', 'PLVR', 'NS1', 'ODN', 'NgayKB', 'VaoVien', 'CDVaoVien',
                        'RaVien', 'CDRaVien', 'NgayTV', 'LyDoTV', 'NguonDL', 'NgayBC', 'NgayNL', 'NVNhapLieu', 'NgayHC',
                        'GhiChu']
    return all(col in df.columns for col in required_columns)

'''
# Geocode address
def geocode_address_1(address, rate_limit=1):
    time.sleep(1 / rate_limit)  # Rate limiting the requests
    try:
        url = f"https://nominatim.openstreetmap.org/search?q={address}&format=json"
        response = requests.get(url)
        if response.status_code == 200 and response.json():
            location = response.json()[0]
            return float(location['lat']), float(location['lon'])
        return None
    except Exception as e:
        print(f"Geocoding error: {e}")
        return None
'''

# Function to geocode the address and use cache if available
def geocode_address(address):
   
    # Check if the address has already been geocoded
    if address in geocoded_cache:
        if __DEBUG__:
            print("Using cached geocode for:", address)
        return geocoded_cache[address]
    
    # If the address is not cached, perform geocoding
    # Initialize geolocator with user_agent
    agent_name = "edgcoder_" + str(random.randint(1, 100))
    geolocator = Nominatim(user_agent=agent_name)
    try:

        location = geolocator.geocode(address, timeout=10)
        if location:
            # Store the geocoded result in the cache
            geocoded_cache[address] = (location.latitude, location.longitude)
            save_geocoded_cache(geocoded_cache, cache_file)
            if __DEBUG__:
                print("Geocoding and caching:", address)            
            return location.latitude, location.longitude
        else:
            return None
    except GeocoderTimedOut:
        return None

# Function to load geocoded cache from a CSV file with exception handling
def load_geocoded_cache(cache_file='geocache.csv'):
    geocoded_cache = {}
    
    # Check if the cache file exists before attempting to load
    if not os.path.exists(cache_file):
        print(f"Cache file {cache_file} does not exist. Starting with an empty cache.")
        return geocoded_cache
    
    try:
        # Attempt to open and read from the CSV file
        with open(cache_file, mode='r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) == 3:  # Ensure we have address, latitude, and longitude
                    address, latitude, longitude = row
                    try:
                        # Attempt to parse latitude and longitude as floats
                        geocoded_cache[address] = (float(latitude), float(longitude))
                    except ValueError as e:
                        # Handle the case where latitude/longitude cannot be converted to float
                        print(f"Error parsing coordinates for address {address}: {e}")
                        continue
    except FileNotFoundError:
        print(f"Cache file {cache_file} not found. Starting with an empty cache.")
    except Exception as e:
        print(f"An error occurred while loading the cache: {e}")

    return geocoded_cache

# Function to save geocoded cache to a CSV file
def save_geocoded_cache(geocoded_cache, cache_file='geocache.csv'):
    try:
        with open(cache_file, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            for address, coords in geocoded_cache.items():
                writer.writerow([address, coords[0], coords[1]])
    except Exception as e:
        print(f"An error occurred while saving the cache: {e}")

# Handling break
def signal_handler(sig, frame):
    global stop_flag
    stop_flag = True
    print("\nGracefully stopping the script. Please wait...")

def save_to_mongodb(geojson_feature):
    """
    Saves a GeoJSON feature to a MongoDB collection with authentication.

    :param geojson_feature: The GeoJSON feature to save.
    :param db_name: The name of the database (default: 'dengue_db').
    :param collection_name: The name of the collection (default: 'denguecases').
    :param mongo_uri: The MongoDB connection URI including username and password (default: 'mongodb://username:password@localhost:27017/').
    :param auth_source: The authentication database (default: 'admin').
    """
    
    host = "222.252.21.66:7217"
    username = "oct_user_db"
    password = urllib.parse.quote("oct@user@123")
    auth_source = "admin"
    mongo_uri = f"mongodb://{username}:{password}@{host}/"
    db_name = 'oct_e_dengue'
    collection_name = 'historical_cases_2022'
    
    '''
    host = "localhost:27017"
    username = "ed_admin_db"
    password = urllib.parse.quote("ed@admin@123!")
    auth_source = "edengue"
    mongo_uri = f"mongodb://{username}:{password}@{host}/"
    db_name = 'edengue'
    collection_name = 'historical_cases'
    '''
    try:
        # Create a MongoDB client with authentication
        client = MongoClient(mongo_uri, authSource=auth_source)

        # Access the database and collection
        db = client[db_name]
        collection = db[collection_name]

        # Check if a feature with the same patient_id already exists        
        patient_id = geojson_feature['properties'].get('patient', {}).get('id')
        existing_feature = collection.find_one({"properties.patient.id": patient_id})
    
        if existing_feature:
            if __DEBUG__:
                print(f"Feature with patient_id {patient_id} already exists. Skipping insertion.")
            return
    
        # Insert the GeoJSON feature into the collection
        result = collection.insert_one(geojson_feature)
        if __DEBUG__:
            print(f"Feature inserted with ID: {result.inserted_id}")

    except Exception as e:
        print(f"Error saving to MongoDB: {e}")

    finally:
        # Close the MongoDB connection
        client.close()

'''
----------------------------------------------------------------------------
GeoJSON adheres to the WGS 84 (EPSG:4326) coordinate reference system (CRS), 
which is the standard for geospatial data in many systems, including GPS. 
This standard also specifies that coordinates are in the order of:
Longitude (x-coordinate, or the horizontal position),
Latitude (y-coordinate, or the vertical position).
-----------------------------------------------------------------------------
'''
def create_geojson_structure(row, longitude, latitude, address):
    # Create a full name, falling back to 'Ho' if 'Ten' is not available
    full_name = f"{row['Ho']} {row['Ten']}" if pd.notna(row['Ten']) else row['Ho']
    
    # Replace None values in the row with empty strings and convert to strings
    row = row.fillna("").astype(str)

    # Safely handle the 'Tuoi' (age) field, converting to an integer if it's a valid number
    try:
        age = int(float(row['Tuoi'])) if row['Tuoi'].replace('.', '', 1).isdigit() else ""
    except ValueError:
        age = ""  # Fallback if conversion fails

    # Function to convert integer (days since 1900) to date
    def int_to_date(days_since_1900):
        # Define the base date (January 1, 1900)
        base_date = datetime(1900, 1, 1)
        
        # Add the number of days to the base date
        converted_date = base_date + timedelta(days=int(days_since_1900))
        
        return converted_date


    # Helper function to convert string (DD/MM/YYYY) or float-like integer (as string) to ISO datetime
    def convert_to_iso_date(date_value):
        # Check if date_value is a string that contains a float (e.g., "3524.0")
        if isinstance(date_value, str) and re.match(r"^\d+\.\d+$", date_value):
            days_since_1900 = int(float(date_value))  # Convert float string to an integer
            iso_date = int_to_date(days_since_1900).isoformat()
            return iso_date
        
        # Check if date_value is a string representing an integer (e.g., "3524")
        elif isinstance(date_value, str) and date_value.isdigit():
            days_since_1900 = int(date_value)
            iso_date = int_to_date(days_since_1900).isoformat()
            return iso_date
        
        # Check if date_value is a string in the format DD/MM/YYYY
        elif isinstance(date_value, str) and re.match(r"\d{2}/\d{2}/\d{4}", date_value):
            return datetime.strptime(date_value, "%d/%m/%Y").isoformat()
        
        # Return unchanged if format is unknown or invalid
        return date_value
    

    # Apply date formatting to relevant fields

    ngay_vv = convert_to_iso_date(row['VaoVien'])
    ngay_rv = convert_to_iso_date(row['RaVien'])
    ngay_tv = convert_to_iso_date(row['NgayTV'])
    ngay_kb = convert_to_iso_date(row['NgayKB'])
    ngay_bc = convert_to_iso_date(row['NgayBC'])
    ngay_nl = convert_to_iso_date(row['NgayNL'])
    ngay_hc = convert_to_iso_date(row['NgayHC'])
    
    if not ngay_kb:
        ngay_kb = ngay_vv

    # Ensure LayMauXN is converted to an integer (0 or 1)
    lay_mau_xn = int(float(row['LayMauXN'])) if row['LayMauXN'].replace('.', '', 1).isdigit() else 0

    feature = {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [longitude, latitude]
        },
        "properties": {
            "address": {
                "postal": address,
                "level0": "Việt Nam",
                "level1": map_ma_tinh(row['MaTinh']),
                "level2": row['Huyen'],
                "level3": row['Xa'],
                "level4": row['Ap'],
            },
            "patient": {
                "id": row['MaSo'],
                "full_name": full_name,
                "dob": row['NgaySinh'],
                "gender": row['Gioi'],
                "age": age,  # Age is now safely handled
                "contact": "",  # Leave blank
                "national_id": "",  # Leave blank
                "insurance_id": ""  # Leave blank
            },
            "meta": {
                'LayMauXN': lay_mau_xn,
                'ELISA': row['ELISA'],
                'PLVR': row['PLVR'],
                'NS1': row['NS1'],
                'ODN': row['ODN'],
                'NgayKB': ngay_kb,  
                'VaoVien': ngay_vv,  
                'CDVaoVien': row['CDVaoVien'],
                'RaVien': ngay_rv,
                'CDRaVien': row['CDRaVien'],
                'NgayTV': ngay_tv,  
                'LyDoTV': row['LyDoTV'],
                'NguonDL': row['NguonDL'],
                'NgayBC': ngay_bc,  
                'NgayNL': ngay_nl,  
                'NVNhapLieu': row['NVNhapLieu'],
                'NgayHC': ngay_hc,
                'GhiChu': row['GhiChu']
            }
        }
    }

    return feature

# Save GeoJSON to file
def save_geojson(feature, file_name):
    with open(file_name, 'w', encoding='utf-8') as geojson_file:
        json.dump(feature, geojson_file, ensure_ascii=False, indent=2)

def save_failed_rows(failed_rows, file_name="failed_rows.csv"):
    """Save rows that failed geocoding to a CSV file."""
    failed_df = pd.DataFrame(failed_rows)
    failed_df.to_csv(file_name, index=False)

# Convert and process Excel file to CSV
def convert_excel_to_geojson(folder_path, rate_limit=1):    
    # Initialize success and fail counters
    failed_rows = []
    success_count = 0
    failed_count = 0

    for file_name in os.listdir(folder_path):
        if stop_flag:
            break
        if file_name.endswith('.xlsx') or file_name.endswith('.xls'):
            file_path = os.path.join(folder_path, file_name)
            df = pd.read_excel(file_path, sheet_name=0)

            if has_required_columns(df):
                # Convert VNI to Unicode for each cell in the dataframe
                for col in df.columns:
                    df[col] = df[col].apply(lambda x: vni2unicode(str(x)) if isinstance(x, str) else x)

                # Filter out rows where both 'Ho' and 'Ten' are missing
                df = df.dropna(subset=['Ho', 'Ten'], how='all')

                # Add a 'Processed' column if it doesn't exist
                if 'Processed' not in df.columns:
                    df['Processed'] = False

                # Filter out already processed rows
                df_filtered = df[df['Processed'] == False]

                print(f"\rProcessing {file_name} ...")
                # Process geocoding
                for index,row in df_filtered.iterrows():
                    if stop_flag:
                        break
                    address = f"{row['Xa']}, {row['Huyen']}, {map_ma_tinh(row['MaTinh'])}, Việt Nam"
                    geocoded = geocode_address(address)                    

                    if geocoded:
                        latitude, longitude = geocoded
                        feature = create_geojson_structure(row, longitude, latitude, address)
                        file_name = os.path.join(folder_path, f"{row['MaSo']}.geojson")
                        #save_geojson(feature, file_name) #update filename
                        save_to_mongodb(feature)
                        # Mark the row as processed
                        df.at[index, 'Processed'] = True                        
                        success_count += 1
                    else:
                        # Save the row information for failed geocoding
                        failed_rows.append(row)
                        failed_count += 1

                    #print(f"Processed {processed_count}/{total_rows}", end='') 
                    #print(f"\r{success_count} successful, {failed_count} failed", end='')
                    print(f"{success_count} successful, {failed_count} failed")
                    time.sleep(1 / rate_limit)  # Rate limiting the requests
        
                # Save failed rows to a CSV if any
                if failed_rows:
                    save_failed_rows(failed_rows, file_name=os.path.join(folder_path, "failed_geocoded_rows.csv")) 
                
                # Save the updated DataFrame back to the original file
                df.to_excel(file_path, index=False)
                print(f"\rProcessed file saved: {file_path}")                

if __name__ == "__main__":
    '''
    if len(sys.argv) != 2:
        print("Usage: python xls2geojson.py <folder_path>")
        sys.exit(1)
    folder_path = sys.argv[1]
    '''
    folder_path = './casedata'

    cache_file = file_path = os.path.join(folder_path, 'geocache.csv')
    # Initialize the cache by loading it from the CSV file
    geocoded_cache = load_geocoded_cache(cache_file)

    # Initialize signal handler
    signal.signal(signal.SIGINT, signal_handler)
    # Flag to handle graceful shutdown
    stop_flag = False

    convert_excel_to_geojson(folder_path, rate_limit=10)  # 10 request per second
