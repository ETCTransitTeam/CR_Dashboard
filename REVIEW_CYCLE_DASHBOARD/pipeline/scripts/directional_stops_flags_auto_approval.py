import pandas as pd
import geopy.distance
from datetime import date

def check_all_characters_present(df, columns_to_check):
    # Function to clean a string by removing underscores and square brackets and converting to lowercase
    def clean_string(s):
        return s.replace('_', '').replace('[', '').replace(']', '').replace(' ','').replace('#','').lower()
    # Clean and convert all column names in df to lowercase for case-insensitive comparison
    df_columns_lower = [clean_string(column) for column in df.columns]
    # Clean and convert the columns_to_check list to lowercase for case-insensitive comparison
    columns_to_check_lower = [clean_string(column) for column in columns_to_check]
    # Use a list comprehension to filter columns
    matching_columns = [column for column in df.columns if clean_string(column) in columns_to_check_lower]
    return matching_columns

# Get today's date
today_date = date.today()
today_date = ''.join(str(today_date).split('-'))

# Load and prepare data
project_name = 'INDY_GO'

# file_name='PARK_CITY_KINGElvis_auto_approval_20260404.csv'
# file_path="details_ParkCity_154732_od_excel.xlsx"
# df1=pd.read_csv('elvis_transit_ls6_154732_export_odbc.csv')

file_name='INDY_GO_KINGElvis_auto_approval_20260411.csv'
file_path="details_lndyGO_574774_od_excel.xlsx"
df1=pd.read_csv('elvis_transit_ls6_574774_export_odbc.csv')
elvis_df=pd.read_csv(file_name)

stops_df = pd.read_excel(file_path, sheet_name="STOPS")
xfer_df  = pd.read_excel(file_path, sheet_name="XFER_STOPS")

xfer_rename_map = {
    "GTFS_VER": "gtfs_ver",
    "GTFS_DATE": "gtfs_date",
    "SEQUENCE_FIXED": "seq_fixed",
    "LAT6": "stop_lat6",
    "LON6": "stop_lon6",
    "route_short_name": "route_short_name",
    "route_long_name": "route_long_name",
    "stop_id": "stop_id",
    "stop_name": "stop_name",
    "stop_lat": "stop_lat",
    "stop_lon": "stop_lon",
    "ETC_STOP_ID": "ETC_STOP_ID",
    "ETC_STOP_NAME": "ETC_STOP_NAME",
    "XFER_ROUTE_ID": "XFER_ROUTE_ID"
}

xfer_df = xfer_df.rename(columns=xfer_rename_map)

for col in stops_df.columns:
    if col not in xfer_df.columns:
        xfer_df[col] = None

# Ensure same column order
xfer_df = xfer_df[stops_df.columns]

detail_df = pd.concat([stops_df, xfer_df], ignore_index=True)

print("total stops in details now: ", len(detail_df))


##################### For LS6 headers ###########################
# Load header mapping and rename columns
mapping_file = "request_20250708_ls6tols2-headers.xlsx"
sheet_name = "Example"
header_df = pd.read_excel(mapping_file, sheet_name=sheet_name)
header_mapping = dict(zip(header_df["Headers-ls6"], header_df["FormattedHeader-ls2"]))
df = df1.rename(columns=header_mapping)

# Merge 'Final_Usage' and 'FINAL_REVIEWER' from elvis_df
df = df.merge(
    elvis_df[['id', 'Final_Usage', 'FINAL_REVIEWER']],
    on='id',
    how='left'
)

# Filter by Final_Usage - convert to lowercase and keep only 'use' records
if 'Final_Usage' in df.columns:
    df['Final_Usage'] = df['Final_Usage'].astype(str).str.lower()
    df = df[df['Final_Usage'] == 'use']
else:
    print("Warning: 'Final_Usage' column not found in dataframe")
print("DF Shape:", df.shape)

# Function to calculate distance
def get_distance_between_coordinates(lat1, lon1, lat2, lon2):
    try:
        lat1 = float(lat1)
        lon1 = float(lon1)
        lat2 = float(lat2)
        lon2 = float(lon2)
        
        coords_1 = (lat1, lon1)
        coords_2 = (lat2, lon2)
        
        distance = geopy.distance.distance(coords_1, coords_2).miles
        return distance
    except (ValueError, TypeError) as e:
        print(f"Error calculating distance: {e}")
        return None

# Function to clean route codes (only split _00 or _01 at the end)
def clean_route_code(route_code):
    if pd.isna(route_code):
        return None
    route_str = str(route_code).strip()
    
    # Only split if it ends with _00 or _01
    if route_str.endswith('_00'):
        return route_str[:-3]
    elif route_str.endswith('_01'):
        return route_str[:-3]
    return route_str

# Prepare detail_df by cleaning route codes
detail_df['CLEAN_ETC_ROUTE_ID'] = detail_df['ETC_ROUTE_ID'].apply(clean_route_code)

# Function to validate stops
def validate_stop(row, lat_col, lon_col, route_code_col, stop_type):
    lat = row[lat_col]
    lon = row[lon_col]
    route_code = row[route_code_col]
    
    if pd.isna(lat) or pd.isna(lon) or pd.isna(route_code):
        return None, False  # Mark as invalid if missing data
    
    clean_route = clean_route_code(route_code)
    if not clean_route:
        return f"Invalid route code: {route_code}", False
    
    # Get all stops for this route
    route_stops = detail_df[detail_df['CLEAN_ETC_ROUTE_ID'] == clean_route]
    if route_stops.empty:
        return f"No stops found for route {clean_route}", False
    
    min_distance = float('inf')
    nearest_stop = None
    
    for _, stop in route_stops.iterrows():
        stop_lat = stop['stop_lat']
        stop_lon = stop['stop_lon']
        
        if pd.isna(stop_lat) or pd.isna(stop_lon):
            continue
            
        distance = get_distance_between_coordinates(lat, lon, stop_lat, stop_lon)
        if distance is not None and distance < min_distance:
            min_distance = distance
            nearest_stop = stop['stop_id']
    
    if min_distance == float('inf'):
        return f"No valid stops found for route {clean_route}", False
    elif min_distance > 0.10:
        return f"{stop_type} is {min_distance:.2f} miles from nearest stop {nearest_stop}", True
    else:
        return None, False  # Only return True if validation passes

# Create mappings between coordinate columns and their corresponding route code columns
coordinate_mappings = [
    # Boarding and Alighting
    {'lat': 'STOP_ON_LAT', 'lon': 'STOP_ON_LONG', 'route': 'ROUTE_SURVEYEDCode', 'type': 'Boarding'},
    {'lat': 'STOP_OFF_LAT', 'lon': 'STOP_OFF_LONG', 'route': 'ROUTE_SURVEYEDCode', 'type': 'Alighting'},
    
    # Previous transfers (on/off)
    {'lat': 'PREV_TRAN_1_ON_BUS_LAT', 'lon': 'PREV_TRAN_1_ON_BUS_LONG', 'route': 'TRIP_FIRST_ROUTECode', 'type': 'Prev1_On'},
    {'lat': 'PREV_TRAN_1_OFF_BUS_LAT', 'lon': 'PREV_TRAN_1_OFF_BUS_LONG', 'route': 'TRIP_FIRST_ROUTECode', 'type': 'Prev1_Off'},
    {'lat': 'PREV_TRAN_2_ON_BUS_LAT', 'lon': 'PREV_TRAN_2_ON_BUS_LONG', 'route': 'TRIP_SECOND_ROUTECode', 'type': 'Prev2_On'},
    {'lat': 'PREV_TRAN_2_OFF_BUS_LAT', 'lon': 'PREV_TRAN_2_OFF_BUS_LONG', 'route': 'TRIP_SECOND_ROUTECode', 'type': 'Prev2_Off'},
    {'lat': 'PREV_TRAN_3_ON_BUS_LAT', 'lon': 'PREV_TRAN_3_ON_BUS_LONG', 'route': 'TRIP_THIRD_ROUTECode', 'type': 'Prev3_On'},
    {'lat': 'PREV_TRAN_3_OFF_BUS_LAT', 'lon': 'PREV_TRAN_3_OFF_BUS_LONG', 'route': 'TRIP_THIRD_ROUTECode', 'type': 'Prev3_Off'},
    {'lat': 'PREV_TRAN_4_ON_BUS_LAT', 'lon': 'PREV_TRAN_4_ON_BUS_LONG', 'route': 'TRIP_FOURTH_ROUTECode', 'type': 'Prev4_On'},
    {'lat': 'PREV_TRAN_4_OFF_BUS_LAT', 'lon': 'PREV_TRAN_4_OFF_BUS_LONG', 'route': 'TRIP_FOURTH_ROUTECode', 'type': 'Prev4_Off'},
    
    # Next transfers (on/off)
    {'lat': 'NEXT_TRAN_1_ON_BUS_LAT', 'lon': 'NEXT_TRAN_1_ON_BUS_LONG', 'route': 'TRIP_NEXT_ROUTECode', 'type': 'Next1_On'},
    {'lat': 'NEXT_TRAN_1_OFF_BUS_LAT', 'lon': 'NEXT_TRAN_1_OFF_BUS_LONG', 'route': 'TRIP_NEXT_ROUTECode', 'type': 'Next1_Off'},
    {'lat': 'NEXT_TRAN_2_ON_BUS_LAT', 'lon': 'NEXT_TRAN_2_ON_BUS_LONG', 'route': 'TRIP_AFTER_ROUTECode', 'type': 'Next2_On'},
    {'lat': 'NEXT_TRAN_2_OFF_BUS_LAT', 'lon': 'NEXT_TRAN_2_OFF_BUS_LONG', 'route': 'TRIP_AFTER_ROUTECode', 'type': 'Next2_Off'},
    {'lat': 'NEXT_TRAN_3_ON_BUS_LAT', 'lon': 'NEXT_TRAN_3_ON_BUS_LONG', 'route': 'TRIP_3RD_ROUTECode', 'type': 'Next3_On'},
    {'lat': 'NEXT_TRAN_3_OFF_BUS_LAT', 'lon': 'NEXT_TRAN_3_OFF_BUS_LONG', 'route': 'TRIP_3RD_ROUTECode', 'type': 'Next3_Off'},
    {'lat': 'NEXT_TRAN_4_ON_BUS_LAT', 'lon': 'NEXT_TRAN_4_ON_BUS_LONG', 'route': 'TRIP_LAST4TH_RTECode', 'type': 'Next4_On'},
    {'lat': 'NEXT_TRAN_4_OFF_BUS_LAT', 'lon': 'NEXT_TRAN_4_OFF_BUS_LONG', 'route': 'TRIP_LAST4TH_RTECode', 'type': 'Next4_Off'}
]

# Initialize flag and reason columns - default to False
df['FLAG'] = False  # Start with all False, only set to True if validation passes
df['REASON'] = ''

# Validate each coordinate pair
for mapping in coordinate_mappings:
    lat_col = mapping['lat']
    lon_col = mapping['lon']
    route_col = mapping['route']
    stop_type = mapping['type']
    
    # Check if columns exist in df
    missing_cols = [col for col in [lat_col, lon_col, route_col] if col not in df.columns]
    if missing_cols:
        print(f"Skipping {stop_type} - missing columns: {missing_cols}")
        continue
    
    print(f"Processing {stop_type}...")
    
    for idx, row in df.iterrows():
        reason, is_valid = validate_stop(row, lat_col, lon_col, route_col, stop_type)
        
        # Only update FLAG if validation passes (is_valid = True)
        if is_valid:
            df.at[idx, 'FLAG'] = True
        
        if reason:
            if df.at[idx, 'REASON']:
                df.at[idx, 'REASON'] += '; ' + reason
            else:
                df.at[idx, 'REASON'] = reason

# Create a clean output with only flagged records and relevant columns
flagged_df = df[df['FLAG'] == True].copy()  # Get only flagged records (where validation failed)

# Select only the columns we want to show in the output
output_columns = [
    'id', 
    'ROUTE_SURVEYEDCode',
    'STOP_ON_LAT', 'STOP_ON_LONG',
    'STOP_OFF_LAT', 'STOP_OFF_LONG',
    'FLAG', 
    'REASON'
]

# Add any additional columns that might be useful for understanding the context
additional_columns = [col for col in df.columns if col.startswith('TRIP_') or col.startswith('PREV_') or col.startswith('NEXT_')]
output_columns.extend(additional_columns)

# Filter to only columns that exist in the dataframe
output_columns = [col for col in output_columns if col in flagged_df.columns]

# Create the final output dataframe
final_output = flagged_df[output_columns]

# Add distance information to make it more understandable
def extract_distance(reason):
    if 'miles from nearest stop' in reason:
        return float(reason.split('is ')[1].split(' miles')[0])
    return None

final_output['DISTANCE_MILES'] = final_output['REASON'].apply(extract_distance)

# Sort by distance (most problematic first)
final_output = final_output.sort_values('DISTANCE_MILES', ascending=False)

# Save the results
output_filename = f"{project_name}_flagged_stops_{today_date}_auto_approved.xlsx"
final_output.to_excel(output_filename, index=False)

print(f"Found {len(final_output)} flagged records")
print(f"Validation complete. Results saved to {output_filename}")

# Print summary statistics
print("\nSummary of flagged stops:")
if final_output.empty:
    print("No directional stop flags (zero flagged records); continuing pipeline.")
else:
    print(f"Maximum distance: {final_output['DISTANCE_MILES'].max():.2f} miles")
    print(f"Average distance: {final_output['DISTANCE_MILES'].mean():.2f} miles")
    print(f"Number of unique routes with issues: {final_output['ROUTE_SURVEYEDCode'].nunique()}")