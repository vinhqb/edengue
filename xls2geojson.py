import json
import re
import os
import time
import requests
import random
import signal
import urllib.parse
import pandas as pd
from datetime import datetime
from pymongo import MongoClient
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut

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

# Geocode address
def geocode_address_1(address, rate_limit=1):
    time.sleep(1 / rate_limit)  # Rate limiting the requests
    try:
        url = f"https://nominatim.openstreetmap.org/search?q={address}&format=json"
        response = requests.get(url)
        if response.status_code == 200 and response.json():
            location = response.json()[0]
            return float(location['lon']), float(location['lat'])
        return None
    except Exception as e:
        print(f"Geocoding error: {e}")
        return None

# Geocode address
def geocode_address(address):
    try:
        location = geolocator.geocode(address, timeout=10)
        if location:
            return location.latitude, location.longitude
        else:
            return None
    except GeocoderTimedOut:
        return None

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
    collection_name = 'historical_cases'

    try:
        # Create a MongoDB client with authentication
        client = MongoClient(mongo_uri, authSource=auth_source)

        # Access the database and collection
        db = client[db_name]
        collection = db[collection_name]

        # Insert the GeoJSON feature into the collection
        result = collection.insert_one(geojson_feature)
        print(f"Feature inserted with ID: {result.inserted_id}")

    except Exception as e:
        print(f"Error saving to MongoDB: {e}")

    finally:
        # Close the MongoDB connection
        client.close()

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

    # Convert date fields from string "YYYY-MM-DD HH:MM:SS" to "DD/MM/YYYY"
    def format_date(value):
        try:
            if isinstance(value, str) and re.match(r'\d{4}-\d{2}-\d{2}', value):
                parsed_date = datetime.strptime(value.split(" ")[0], '%Y-%m-%d')
                return parsed_date.strftime('%d/%m/%Y')
        except ValueError:
            return value  # Return the original value if formatting fails
        return value  # Return the value if it's not a date
    
    # Ensure LayMauXN is converted to an integer (0 or 1)
    lay_mau_xn = int(float(row['LayMauXN'])) if row['LayMauXN'].replace('.', '', 1).isdigit() else 0

    # Apply date formatting to relevant fields
    vao_vien = format_date(row['VaoVien'])
    ngay_tv = format_date(row['NgayTV'])
    ngay_kb = format_date(row['NgayKB'])
    ngay_bc = format_date(row['NgayBC'])
    ngay_nl = format_date(row['NgayNL'])
    ngay_hc = format_date(row['NgayHC'])

    feature = {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [longitude, latitude]
        },
        "properties": {
            "address": {
                "postal": address,
                "level4": row['Ap'],
                "level3": row['Xa'],
                "level2": row['Huyen'],
                "level1": map_ma_tinh(row['MaTinh']),
                "level0": "VIET NAM"
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
                'VaoVien': vao_vien,  
                'CDVaoVien': row['CDVaoVien'],
                'RaVien': row['RaVien'],
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
                        longitude, latitude = geocoded
                        feature = create_geojson_structure(row, longitude, latitude, address)
                        file_name = os.path.join(folder_path, f"{row['MaSo']}.geojson")
                        #save_geojson(feature, file_name) #update filename
                        #save_to_mongodb(feature)
                        # Mark the row as processed
                        df.at[index, 'Processed'] = True                        
                        success_count += 1
                    else:
                        # Save the row information for failed geocoding
                        failed_rows.append(row)
                        failed_count += 1

                    #print(f"Processed {processed_count}/{total_rows}", end='') 
                    print(f"\r{success_count} successful, {failed_count} failed", end='')
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

    agent_name = "edgcoder_" + str(random.randint(1, 100))
    # Initialize geolocator with user_agent
    geolocator = Nominatim(user_agent=agent_name)
    # Initialize signal handler
    signal.signal(signal.SIGINT, signal_handler)
    # Flag to handle graceful shutdown
    stop_flag = False

    folder_path = './casedata'
    convert_excel_to_geojson(folder_path, rate_limit=5)  # 10 request per second
