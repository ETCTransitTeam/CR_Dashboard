import re
import pandas as pd
import numpy as np
from geopy.distance import geodesic
import os
from datetime import date
import warnings

warnings.filterwarnings("ignore")

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


def _clean_col_name(s):
    return s.replace('_', '').replace('[', '').replace(']', '').replace(' ','').replace('#','').lower()


def get_columns_by_cleaned_name(df, cleaned_names, prefer_text_over_code=None):
    """Return dict mapping cleaned_name -> actual column name in df.
    When prefer_text_over_code is set (e.g. ['origintransport','destintransport']), prefer columns
    whose name does NOT contain 'code' so we get the text/label column, not the code column."""
    prefer_text_over_code = prefer_text_over_code or []
    out = {}
    candidates = {}
    for col in df.columns:
        c = _clean_col_name(col)
        if c not in cleaned_names:
            continue
        if c not in candidates:
            candidates[c] = []
        candidates[c].append(col)
    for c, cols in candidates.items():
        if c in prefer_text_over_code:
            non_code = [col for col in cols if 'code' not in col.lower()]
            out[c] = (non_code[0] if non_code else cols[0])
        else:
            out[c] = cols[0]
    return out


def find_transport_column(df, origin_not_destin):
    """Find origin or destination transport TEXT column by multiple possible names or contains logic."""
    want = 'origin' if origin_not_destin else 'destin'
    # Try exact cleaned names (with common LS2 variants)
    if origin_not_destin:
        variants = ['origintransport', 'originmode', 'accessmode', 'origintransportmode', 'origin_transport']
    else:
        variants = ['destintransport', 'destinmode', 'egressmode', 'destintransportmode', 'destin_transport']
    tt = get_columns_by_cleaned_name(df, variants, prefer_text_over_code=variants)
    for v in variants:
        if v in tt and 'code' not in tt[v].lower():
            return tt[v]
    # Fallback: any column whose cleaned name contains want + 'transport' and not 'code'
    for col in df.columns:
        c = _clean_col_name(col)
        if 'code' in col.lower():
            continue
        if want in c and 'transport' in c:
            return col
    return None


def find_transport_code_column(df, origin_not_destin):
    """Find origin or destination transport CODE column (numeric 1=Walk, 7=Drive, etc.)."""
    if origin_not_destin:
        variants = ['origintransportcode', 'originmodecode', 'accessmodecode']
    else:
        variants = ['destintransportcode', 'destinmodecode', 'egressmodecode']
    tt = get_columns_by_cleaned_name(df, variants)
    for v in variants:
        if v in tt:
            return tt[v]
    for col in df.columns:
        c = _clean_col_name(col)
        if origin_not_destin and 'origin' in c and 'transport' in c and 'code' in col.lower():
            return col
        if not origin_not_destin and 'destin' in c and 'transport' in c and 'code' in col.lower():
            return col
    return None


today_date = date.today()
today_date=''.join(str(today_date).split('-'))

project_name='INDY_GO'


# file_name='STL_MO_2025_KINGElvis.xlsx'
# detail_df=pd.read_excel("details_saint_louis_MO_od_excel.xlsx",sheet_name='STOPS')
# df=pd.read_csv('elvisstlouis2025obweekday_export_odbc.csv')
# elvis_df=pd.read_excel(file_name,sheet_name='Elvis_Review')

file_name='INDY_GO_KINGElvis_auto_approval_20260411.csv'
file_path="details_lndyGO_574774_od_excel.xlsx"
df1=pd.read_csv('elvis_transit_ls6_574774_export_odbc.csv')

# file_name='PARK_CITY_KINGElvis_auto_approval_20260404.csv'
# file_path="details_ParkCity_154732_od_excel.xlsx"
# df1=pd.read_csv('elvis_transit_ls6_154732_export_odbc.csv')
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

df1 = df1.drop(0).reset_index(drop=True)

##################### For LS6 headers ###########################
mapping_file = "request_20250708_ls6tols2-headers.xlsx"
sheet_name = "Example"

header_df = pd.read_excel(mapping_file, sheet_name=sheet_name)

# Create a dictionary: {old_name: new_name}
header_mapping = dict(zip(header_df["Headers-ls6"], header_df["FormattedHeader-ls2"]))

# Step 2: Rename df1 columns to get df2
df = df1.rename(columns=header_mapping)

# Optional: Check changes
print("Renamed Columns:")
print(df.columns.tolist())
##################### For LS6 headers ###########################

if 'PREV_TRANSFERSCode' in df.columns:
    df['PREV_TRANSFERSCode'] = pd.to_numeric(df['PREV_TRANSFERSCode'], errors='coerce').fillna(0).astype(int)
if 'NEXT_TRANSFERSCode' in df.columns:
    df['NEXT_TRANSFERSCode'] = pd.to_numeric(df['NEXT_TRANSFERSCode'], errors='coerce').fillna(0).astype(int)


if file_name.split('_')[0].isdigit():
    file_first_name=file_name.split('_')[0]+'_'+file_name.split('_')[1]
else:
    file_first_name=file_name.split('_')[0]

elvis_date_check=['elvisdate']
elvis_date=check_all_characters_present(elvis_df,elvis_date_check)

df = df.merge(elvis_df[[elvis_date[0], 'id', 'Final_Usage','FINAL_REVIEWER']], on='id', how='left')

df=df[df['Final_Usage'].str.lower()=='use']


home_airport_hotel_column_names=['originaddresslat','originaddresslong', 'destinaddresslat',
                                 'destinaddresslong','originplacetype','homeaddresslat','homeaddresslong',
                                 'destinairportcode','destinplacetype']


def _get_details_lat_lon(details_row):
    """Get (lat, lon) from a single row of details_df. Tries common column names."""
    candidates = [
        ('LAT6', 'LON6'), ('stop_lat6', 'stop_lon6'), ('lat6', 'lng6'),
        ('lat6', 'lon6'), ('stop_lat', 'stop_lon'), ('LAT', 'LON')
    ]
    for lat_name, lon_name in candidates:
        if lat_name in details_row.index and lon_name in details_row.index:
            lat_val = details_row[lat_name]
            lon_val = details_row[lon_name]
            if pd.notna(lat_val) and pd.notna(lon_val):
                return lat_val, lon_val
    return None, None


def check_home_airport_hotel(df, details_df):
    # Resolve columns by cleaned name so we don't rely on data_list sort order (which varies by actual column names)
    name_to_col = get_columns_by_cleaned_name(df, home_airport_hotel_column_names)
    col_origin_lat = name_to_col.get('originaddresslat')
    col_origin_lng = name_to_col.get('originaddresslong')
    col_destin_lat = name_to_col.get('destinaddresslat')
    col_destin_lng = name_to_col.get('destinaddresslong')
    col_origin_type = name_to_col.get('originplacetype')
    col_destin_type = name_to_col.get('destinplacetype')
    col_airport_code = name_to_col.get('destinairportcode')
    col_home_lat = name_to_col.get('homeaddresslat')
    col_home_lng = name_to_col.get('homeaddresslong')
    if not all([col_origin_lat, col_origin_lng, col_destin_lat, col_destin_lng]):
        return df
    lime_col = 'LIME_CODE' if 'LIME_CODE' in details_df.columns else None

    for index, row in df.iterrows():
        origin_addr_lat = row[col_origin_lat]
        origin_addr_lng = row[col_origin_lng]
        destin_addr_lat = row[col_destin_lat]
        destin_addr_lng = row[col_destin_lng]

        origin_lat_na = pd.isna(origin_addr_lat)
        origin_lng_na = pd.isna(origin_addr_lng)
        destin_lat_na = pd.isna(destin_addr_lat)
        destin_lng_na = pd.isna(destin_addr_lng)

        # ORIGIN: only fill when missing; do not overwrite valid coordinates
        if origin_lat_na or origin_lng_na:
            origin_place_type = row[col_origin_type] if col_origin_type else None
            place_type = '' if pd.isna(origin_place_type) else str(origin_place_type).lower()
            lat_val, lng_val = None, None

            if ('hotel' in place_type or 'home' in place_type) and col_home_lat and col_home_lng:
                # Home/Hotel → use home address lat/long
                fallback_lat = row[col_home_lat]
                fallback_lng = row[col_home_lng]
                if pd.notna(fallback_lat):
                    lat_val = fallback_lat
                if pd.notna(fallback_lng):
                    lng_val = fallback_lng
            elif 'airport' in place_type and lime_col and col_airport_code:
                airport_destin_code = row[col_airport_code]
                if pd.notna(airport_destin_code):
                    code_str = str(airport_destin_code).strip()
                    match = details_df[lime_col].astype(str).str.strip() == code_str
                    airport_row = details_df.loc[match]
                    if not airport_row.empty:
                        lat_val, lng_val = _get_details_lat_lon(airport_row.iloc[0])

            if lat_val is not None and origin_lat_na:
                df.at[index, col_origin_lat] = lat_val
            if lng_val is not None and origin_lng_na:
                df.at[index, col_origin_lng] = lng_val

        # DESTINATION: only fill when missing; do not overwrite valid coordinates
        if destin_lat_na or destin_lng_na:
            destin_place_type = row[col_destin_type] if col_destin_type else None
            place_type = '' if pd.isna(destin_place_type) else str(destin_place_type).lower()
            lat_val, lng_val = None, None

            if ('hotel' in place_type or 'home' in place_type) and col_home_lat and col_home_lng:
                # Home/Hotel → use home address lat/long
                fallback_lat = row[col_home_lat]
                fallback_lng = row[col_home_lng]
                if pd.notna(fallback_lat):
                    lat_val = fallback_lat
                if pd.notna(fallback_lng):
                    lng_val = fallback_lng
            elif 'airport' in place_type and lime_col and col_airport_code:
                airport_destin_code = row[col_airport_code]
                if pd.notna(airport_destin_code):
                    code_str = str(airport_destin_code).strip()
                    match = details_df[lime_col].astype(str).str.strip() == code_str
                    airport_row = details_df.loc[match]
                    if not airport_row.empty:
                        lat_val, lng_val = _get_details_lat_lon(airport_row.iloc[0])

            if lat_val is not None and destin_lat_na:
                df.at[index, col_destin_lat] = lat_val
            if lng_val is not None and destin_lng_na:
                df.at[index, col_destin_lng] = lng_val

    return df




df=check_home_airport_hotel(df,detail_df)

df.to_csv("Check_home_airport_new.csv")


blank_columns_checks=['originaddresslat', 'originaddresslong', 'destinaddresslat',
                      'destinaddresslong','stoponlat', 'stoponlong', 'stopofflat', 'stopofflong']
blank_column_names=check_all_characters_present(df,blank_columns_checks)



df.dropna(subset=blank_column_names, how='any',inplace=True)

df.to_csv("dropped_new.csv")

print(f"Rows used for distance checks (after Final_Usage='use' and dropna): {len(df)}")

boarding_columns_checks=['prevtran1onbuslat', 'prevtran1onbuslong',
                         'prevtran2onbuslat', 'prevtran2onbuslong',
                         'prevtran3onbuslat', 'prevtran3onbuslong', 
                         'prevtran4onbuslat', 
                         'prevtran4onbuslong','stoponlat', 'stoponlong',
                         'stopofflat', 'stopofflong',
                          'nexttran1offbuslat','nexttran1offbuslong',  
                         'nexttran2offbuslat', 'nexttran2offbuslong', 
                          'nexttran3offbuslat', 'nexttran3offbuslong', 
                          'nexttran4offbuslat', 'nexttran4offbuslong',]
boarding_columns=check_all_characters_present(df,boarding_columns_checks)
boarding_columns.sort()

origin_destin_columns_checks=['originaddresslat','originaddresslong', 'destinaddresslat', 'destinaddresslong']
origin_destin_columns=check_all_characters_present(df,origin_destin_columns_checks)
origin_destin_columns.sort()


df['FIRST_BOARDING_LAT']=None 
df['FIRST_BOARDING_LONG']=None
df['LAST_ALIGHTING_LAT']=None
df['LAST_ALIGHTING_LONG']=None
df['ORIGIN_TO_SURVEYBOARD']=None    
df['ORIGIN_TO_FIRST_BOARD']=None    
df['SURVEYBOARDING_TO_SURVEYALIGHTING']=None   
df['ORIGIN_TO_DESTINATION']=None    
df['SURVEYALIGHTING_TO_DESTINATION']=None   
df['LAST_ALIGHTING_LOCATION_TO_DESTIN']=None   


def get_distance_between_coordinates(lat1, lon1, lat2, lon2):
    try:


        lat1 = float(lat1)
        lon1 = float(lon1)
        lat2 = float(lat2)
        lon2 = float(lon2)
        
        coords_1 = (lat1, lon1)
        coords_2 = (lat2, lon2)
        
        distance = geodesic(coords_1, coords_2).miles
        return distance
    except (ValueError, TypeError) as e:
        # Handle the exception here
        print(f"Error calculating distance: {e}")  # Change to the desired distance unit


for index, row in df.iterrows():
    if not pd.isna(row[boarding_columns[8]]) and not pd.isna(row[boarding_columns[9]]):
        #'PREV_TRAN_1_ON_BUS_LAT',
        #'PREV_TRAN_1_ON_BUS_LONG'
        df.loc[index, 'FIRST_BOARDING_LAT'] = row[boarding_columns[8]]
        df.loc[index, 'FIRST_BOARDING_LONG'] = row[boarding_columns[9]]
    elif not pd.isna(row[boarding_columns[10]]) and not pd.isna(row[boarding_columns[11]]):
        #  'PREV_TRAN_2_ON_BUS_LAT',
        # 'PREV_TRAN_2_ON_BUS_LONG'
        df.loc[index, 'FIRST_BOARDING_LAT'] = row[boarding_columns[10]]
        df.loc[index, 'FIRST_BOARDING_LONG'] = row[boarding_columns[11]]
    elif not pd.isna(row[boarding_columns[12]]) and not pd.isna(row[boarding_columns[13]]):
        #  'PREV_TRAN_3_ON_BUS_LAT',
        # 'PREV_TRAN_3_ON_BUS_LONG'
        df.loc[index, 'FIRST_BOARDING_LAT'] = row[boarding_columns[12]]
        df.loc[index, 'FIRST_BOARDING_LONG'] = row[boarding_columns[13]]
    elif not pd.isna(row[boarding_columns[14]]) and not pd.isna(row[boarding_columns[15]]):
        #  'PREV_TRAN_4_ON_BUS_LAT',
        # 'PREV_TRAN_4_ON_BUS_LONG'
        df.loc[index, 'FIRST_BOARDING_LAT'] = row[boarding_columns[14]]
        df.loc[index, 'FIRST_BOARDING_LONG'] = row[boarding_columns[15]]
    elif not pd.isna(row[boarding_columns[18]]) and not pd.isna(row[boarding_columns[19]]):
        #  'STOP_ON_LAT',
        # 'STOP_ON_LONG'
        df.loc[index, 'FIRST_BOARDING_LAT'] = row[boarding_columns[18]]
        df.loc[index, 'FIRST_BOARDING_LONG'] = row[boarding_columns[19]]
    else:
        df.loc[index, 'FIRST_BOARDING_LAT'] = None
        df.loc[index, 'FIRST_BOARDING_LONG'] = None
    #      
    if not pd.isna(row[boarding_columns[6]]) and not pd.isna(row[boarding_columns[7]]):
        #  'NEXT_TRAN_4_OFF_BUS_LAT',
        # 'NEXT_TRAN_4_OFF_BUS_LONG'
        df.loc[index, 'LAST_ALIGHTING_LAT'] = row[boarding_columns[6]]
        df.loc[index, 'LAST_ALIGHTING_LONG'] = row[boarding_columns[7]]
    elif not pd.isna(row[boarding_columns[4]]) and not pd.isna(row[boarding_columns[5]]):
        #  'NEXT_TRAN_3_OFF_BUS_LAT',
        # 'NEXT_TRAN_3_OFF_BUS_LONG'
        df.loc[index, 'LAST_ALIGHTING_LAT'] = row[boarding_columns[4]]
        df.loc[index, 'LAST_ALIGHTING_LONG'] = row[boarding_columns[5]]
    elif not pd.isna(row[boarding_columns[2]]) and not pd.isna(row[boarding_columns[3]]):
        #  'NEXT_TRAN_2_OFF_BUS_LAT',
        # 'NEXT_TRAN_2_OFF_BUS_LONG'
        df.loc[index, 'LAST_ALIGHTING_LAT'] = row[boarding_columns[2]]
        df.loc[index, 'LAST_ALIGHTING_LONG'] = row[boarding_columns[3]]
    elif not pd.isna(row[boarding_columns[0]]) and not pd.isna(row[boarding_columns[1]]):
        #  'NEXT_TRAN_1_OFF_BUS_LAT',
        # 'NEXT_TRAN_1_OFF_BUS_LONG'
        df.loc[index, 'LAST_ALIGHTING_LAT'] = row[boarding_columns[0]]
        df.loc[index, 'LAST_ALIGHTING_LONG'] = row[boarding_columns[1]]
    elif not pd.isna(row[boarding_columns[16]]) and not pd.isna(row[boarding_columns[17]]):
        #  'STOP_OFF_LAT',
        # 'STOP_OFF_LONG'
        df.loc[index, 'LAST_ALIGHTING_LAT'] = row[boarding_columns[16]]
        df.loc[index, 'LAST_ALIGHTING_LONG'] = row[boarding_columns[17]]
    else:
        df.loc[index, 'LAST_ALIGHTING_LAT'] = None
        df.loc[index, 'LAST_ALIGHTING_LONG'] = None

cleaning_columns = origin_destin_columns + ['FIRST_BOARDING_LAT', 'FIRST_BOARDING_LONG']
# Function to clean up extra dots in numeric strings
def fix_extra_dots(value):
    if isinstance(value, str):
        # Remove non-numeric characters except dots and minus signs
        value = ''.join(c for c in value if c.isdigit() or c in ['.', '-'])
        # If there are multiple dots, keep only the first one
        parts = value.split('.')
        if len(parts) > 2:  # More than one dot
            value = parts[0] + '.' + ''.join(parts[1:])  # Keep only first dot
        return value
    return value  # Return as is if not a string

# Apply cleaning to each column
for col in cleaning_columns:
    df[col] = df[col].astype(str).apply(fix_extra_dots)  # Clean extra dots
    df[col] = pd.to_numeric(df[col], errors='coerce')  # Convert to numeric

# # Check results
print(df[cleaning_columns].head())
# print(df['FIRST_BOARDING_LONG'].unique())  # Should not contain 'c' anymore

# print(df.dtypes)  # Verify data types

df[boarding_columns[19]] = pd.to_numeric(df[boarding_columns[19]], errors='coerce').fillna(0.0)

for index, row in df.iterrows():
    df.loc[index,'ORIGIN_TO_SURVEYBOARD']=get_distance_between_coordinates(row[origin_destin_columns[2]],row[origin_destin_columns[3]], row[boarding_columns[18]],row[boarding_columns[19]])
    df.loc[index,'ORIGIN_TO_FIRST_BOARD']=get_distance_between_coordinates(row[origin_destin_columns[2]],row[origin_destin_columns[3]],row['FIRST_BOARDING_LAT'],row['FIRST_BOARDING_LONG'])
    df.loc[index,'SURVEYBOARDING_TO_SURVEYALIGHTING']=get_distance_between_coordinates(row[boarding_columns[18]],row[boarding_columns[19]],row[boarding_columns[16]],row[boarding_columns[17]])
    df.loc[index,'ORIGIN_TO_DESTINATION']=get_distance_between_coordinates(row[origin_destin_columns[2]],row[origin_destin_columns[3]],row[origin_destin_columns[0]],row[origin_destin_columns[1]])
    df.loc[index,'SURVEYALIGHTING_TO_DESTINATION']=get_distance_between_coordinates(row[boarding_columns[16]],row[boarding_columns[17]],row[origin_destin_columns[0]],row[origin_destin_columns[1]])
    df.loc[index,'LAST_ALIGHTING_LOCATION_TO_DESTIN']=get_distance_between_coordinates(row['LAST_ALIGHTING_LAT'],row['LAST_ALIGHTING_LONG'],row[origin_destin_columns[0]],row[origin_destin_columns[1]])

# ===== DEBUG: Print O → B distance for specific IDs =====
check_ids = [9998, 9119, 6446]

print("\nO → B (Origin to First Board) distances:")
subset = df[df['id'].isin(check_ids)]

if subset.empty:
    print("No matching IDs found.")
else:
    print(subset[['id', 'ORIGIN_TO_FIRST_BOARD']].to_string(index=False))

# Ensure 'id' is properly formatted
df['id'] = df['id'].astype(str).str.strip().astype(int)
# df['id'] = df['id'].astype(str).str.strip().astype(float).astype(int)


# Print unique values to verify presence
print("Unique IDs in DataFrame:", df['id'].unique())

# Print all rows where id == 12086
matching_rows = df[df['id'] == 12086]
if not matching_rows.empty:
    print("Matching Row Found:\n", matching_rows)
    distance_value = matching_rows['ORIGIN_TO_FIRST_BOARD'].values[0]
    print(f"Distance for ID 12086: {distance_value}")
else:
    print("ID 12086 not found in DataFrame")
    
df['O2B/O2D']=None   #df['ORIGIN_TO_SURVEYBOARD']/df['ORIGIN_TO_DESTINATION'] ORIGIN_TO_BOARD Divide by ORIGIN_TO_DESTINATION
df['B2A/OD']=None   #df['SURVEYBOARDING_TO_SURVEYALIGHTING']/df['ORIGIN_TO_DESTINATION'] BOARDING_TO_ALIGHTING Divide by ORIGIN_TO_DESTINATION
df['A2D/OD']=None   #df['SURVEYALIGHTING_TO_DESTINATION']/df['ORIGIN_TO_DESTINATION'] ALIGHTING_TO_DESTINATION Divide by ORIGIN_TO_DESTINATION

for index, row in df.iterrows():
    origin_to_destination = row['ORIGIN_TO_DESTINATION']
    if origin_to_destination==0:
        df.loc[index,'O2B/O2D']=0
        df.loc[index,'B2A/OD']=0
        df.loc[index,'A2D/OD']=0
    else:
        # df['ORIGIN_TO_SURVEYBOARD'] = pd.to_numeric(df['ORIGIN_TO_SURVEYBOARD'])
        # df['ORIGIN_TO_DESTINATION'] = pd.to_numeric(df['ORIGIN_TO_DESTINATION'])

        df.loc[index,'O2B/O2D']=row['ORIGIN_TO_SURVEYBOARD']/row['ORIGIN_TO_DESTINATION']
      
        df.loc[index, 'B2A/OD'] = row['SURVEYBOARDING_TO_SURVEYALIGHTING'] / row['ORIGIN_TO_DESTINATION']

        # df.loc[index,'B2A/OD']=row['SURVEYBOARDING_TO_SURVEYALIGHTING']/row['ORIGIN_TO_DESTINATION']
        df.loc[index,'A2D/OD']=row['SURVEYALIGHTING_TO_DESTINATION']/row['ORIGIN_TO_DESTINATION']

df['O_B_Dist_Check1']=None #(df['ORIGIN_TO_FIRST_BOARD'] > 1.85) & (df['ORIGIN_TRANSPORTCode'].isin(['1', '2', '-oth-']))  if [ORIGIN_TO_FIRST_BOARD]>1.85 and ORIGIN_NEW_CODE = WALK [(Text.Contains([ORIGIN_TRANSPORT],"Walk") or Text.Contains([ORIGIN_TRANSPORT],"Wheelchair") or Text.Contains([ORIGIN_TRANSPORT],"Skateboard"))]
df['O_B_Dist_Check2']=None #(df['ORIGIN_TO_FIRST_BOARD'] < 0.25) & (df['ORIGIN_TRANSPORTCode'].isin(['7', '8','9','10','11'])) if [ORIGIN_TO_FIRST_BOARD]<.25 and ORIGIN_NEW_CODE = "DRIVE" then 1 (Flag) else 0 (Non-Flag)
df['O_B_Dist_Check3']=None #(df['ORIGIN_TO_FIRST_BOARD'] < 0.25)  if [ORIGIN_TO_SURVEYBOARD]<0.25 and [#"PREV_TRANSFERS[Code]"]!="0" then 1 (Flag) else 0 (Non-Flag)


transport_transfer_columns_checks=['origintransport','destintransport','nexttransferscode','prevtransferscode','origintransportcode','destintransportcode']
transport_transfer_columns=check_all_characters_present(df,transport_transfer_columns_checks)
transport_transfer_columns.sort()
# Resolve transport: try multiple name variants and code fallback
_col_origin_transport = find_transport_column(df, origin_not_destin=True)
if _col_origin_transport is None:
    _tt = get_columns_by_cleaned_name(df, ['origintransport'], prefer_text_over_code=['origintransport'])
    _col_origin_transport = _tt.get('origintransport')
_col_destin_transport = find_transport_column(df, origin_not_destin=False)
if _col_destin_transport is None:
    _tt = get_columns_by_cleaned_name(df, ['destintransport'], prefer_text_over_code=['destintransport'])
    _col_destin_transport = _tt.get('destintransport')
_col_origin_transport_code = find_transport_code_column(df, origin_not_destin=True)
_col_destin_transport_code = find_transport_code_column(df, origin_not_destin=False)
_tt = get_columns_by_cleaned_name(df, ['nexttransferscode', 'prevtransferscode'])
_col_next_transfers_code = _tt.get('nexttransferscode')
_col_prev_transfers_code = _tt.get('prevtransferscode')
# Ensure transfer code columns exist after possible rename (use resolved names)
if _col_prev_transfers_code and _col_prev_transfers_code in df.columns:
    df[_col_prev_transfers_code] = pd.to_numeric(df[_col_prev_transfers_code], errors='coerce').fillna(0).astype(int)
if _col_next_transfers_code and _col_next_transfers_code in df.columns:
    df[_col_next_transfers_code] = pd.to_numeric(df[_col_next_transfers_code], errors='coerce').fillna(0).astype(int)

walk=['walk','wheelchair or scooter','other','walked','skateboard','bike, e-bike, skateboard, scooter, e-scooter','wheelchair','walked or used mobility aid']
drive=['was dropped off by someone','drove alone and parked','drove or rode with others and parked','taxi','uber, lyft, etc.',
       'get in a parked vehicle & drive alone','be picked up by someone','taxi / shuttle','get in a parked vehicle & drive, alone or w/others',
       'get in a parked vehicle & drive/ride w/others','get in a parked vehicle & drive, alone or w/others','rode with others and was dropped off',
      'rode in an uber / lyft / taxi / etc. vehicle','get in a parked vehicle & drive alone'
      ]


def _normalize_transport_text(series):
    """Normalize for comparison: strip, lower, remove leading (code) or code - prefix."""
    s = series.astype(str).str.strip().str.lower().replace('nan', '')
    # Remove leading "(1) ", "1 - ", "(1)", etc.
    s = s.str.replace(r'^\s*\(\s*\d+\s*\)\s*', '', regex=True)
    s = s.str.replace(r'^\s*\d+\s*[-–]\s*', '', regex=True)
    return s.str.strip()


def _transport_isin(series, values):
    """Case-insensitive check: series value (normalized) in values. Handles (1) Walk style. Also matches if normalized text contains any value as substring."""
    norm = _normalize_transport_text(series)
    values_lower = [v.lower() for v in values]
    exact = norm.isin(values_lower)
    # Fallback: normalized text contains any of the value phrases (e.g. "get in a parked vehicle" in "get in a parked vehicle & drive alone")
    contains = pd.Series(False, index=series.index)
    for v in values_lower:
        if len(v) < 3:
            continue
        contains = contains | norm.str.contains(re.escape(v), case=False, na=False)
    return exact | contains


def _transfer_code_ne_zero(series):
    """True where transfer code is not 0 (handles int, float, object '0')."""
    n = pd.to_numeric(series, errors='coerce').fillna(0)
    return (n != 0)


def _transfer_code_eq_zero(series):
    """True where transfer code is 0 (handles int, float, object '0')."""
    n = pd.to_numeric(series, errors='coerce').fillna(0)
    return (n == 0)


# Fallback: use pre-existing distance columns when computed ones are missing
_ob_cols = [c for c in df.columns if _clean_col_name(c) in (
    'valdistob', 'origintoboard', 'origintofirstboard', 'origin_to_board', 'origintoboard', 'distob', 'val_dist_ob'
)]
_lad_cols = [c for c in df.columns if _clean_col_name(c) in (
    'valdistad', 'alightingtodestination', 'alighting_to_destination', 'lastalightinglocationtodestin', 'val_dist_ad'
)]
if _ob_cols:
    _ob_fallback = pd.to_numeric(df[_ob_cols[0]], errors='coerce')
    if 'ORIGIN_TO_FIRST_BOARD' in df.columns:
        df['ORIGIN_TO_FIRST_BOARD'] = df['ORIGIN_TO_FIRST_BOARD'].fillna(_ob_fallback)
    else:
        df['ORIGIN_TO_FIRST_BOARD'] = _ob_fallback
if _lad_cols and 'LAST_ALIGHTING_LOCATION_TO_DESTIN' in df.columns:
    _lad_fallback = pd.to_numeric(df[_lad_cols[0]], errors='coerce')
    df['LAST_ALIGHTING_LOCATION_TO_DESTIN'] = df['LAST_ALIGHTING_LOCATION_TO_DESTIN'].fillna(_lad_fallback)

# Ensure distance columns are numeric for comparisons
_ob = pd.to_numeric(df['ORIGIN_TO_FIRST_BOARD'], errors='coerce')
_osb = pd.to_numeric(df['ORIGIN_TO_SURVEYBOARD'], errors='coerce')
_lad = pd.to_numeric(df['LAST_ALIGHTING_LOCATION_TO_DESTIN'], errors='coerce')
_sad = pd.to_numeric(df['SURVEYALIGHTING_TO_DESTINATION'], errors='coerce')
_od = pd.to_numeric(df['ORIGIN_TO_DESTINATION'], errors='coerce')
_b2a = pd.to_numeric(df['B2A/OD'], errors='coerce')

# Walk/drive by code: 1,2 = walk; 7,8,9,10,11 = drive (per original rule comments)
_ORIGIN_WALK_CODES = {1, 2}
_ORIGIN_DRIVE_CODES = {7, 8, 9, 10, 11}

def _code_in_set(series, allowed):
    """True where series value (as numeric) is in allowed set. Handles object dtype '1','7', etc."""
    n = pd.to_numeric(series, errors='coerce').fillna(-999).astype(int)
    return n.isin(allowed)

def _origin_walk():
    text_ok = _transport_isin(df[_col_origin_transport], walk) if _col_origin_transport else pd.Series(False, index=df.index)
    code_ok = _code_in_set(df[_col_origin_transport_code], _ORIGIN_WALK_CODES) if _col_origin_transport_code and _col_origin_transport_code in df.columns else pd.Series(False, index=df.index)
    return text_ok | code_ok

def _origin_drive():
    text_ok = _transport_isin(df[_col_origin_transport], drive) if _col_origin_transport else pd.Series(False, index=df.index)
    code_ok = _code_in_set(df[_col_origin_transport_code], _ORIGIN_DRIVE_CODES) if _col_origin_transport_code and _col_origin_transport_code in df.columns else pd.Series(False, index=df.index)
    return text_ok | code_ok

def _destin_walk():
    text_ok = _transport_isin(df[_col_destin_transport], walk) if _col_destin_transport else pd.Series(False, index=df.index)
    code_ok = _code_in_set(df[_col_destin_transport_code], _ORIGIN_WALK_CODES) if _col_destin_transport_code and _col_destin_transport_code in df.columns else pd.Series(False, index=df.index)
    return text_ok | code_ok

def _destin_drive():
    text_ok = _transport_isin(df[_col_destin_transport], drive) if _col_destin_transport else pd.Series(False, index=df.index)
    code_ok = _code_in_set(df[_col_destin_transport_code], _ORIGIN_DRIVE_CODES) if _col_destin_transport_code and _col_destin_transport_code in df.columns else pd.Series(False, index=df.index)
    return text_ok | code_ok
def _prev_ne_zero(): return _transfer_code_ne_zero(df[_col_prev_transfers_code]) if _col_prev_transfers_code else pd.Series(False, index=df.index)
def _next_eq_zero(): return _transfer_code_eq_zero(df[_col_next_transfers_code]) if _col_next_transfers_code else pd.Series(False, index=df.index)
def _prev_eq_zero(): return _transfer_code_eq_zero(df[_col_prev_transfers_code]) if _col_prev_transfers_code else pd.Series(False, index=df.index)
def _next_ne_zero(): return _transfer_code_ne_zero(df[_col_next_transfers_code]) if _col_next_transfers_code else pd.Series(False, index=df.index)

o_b_check1 = (_ob > 1.85) & _origin_walk()
o_b_check2 = (_ob < 0.25) & _origin_drive()
o_b_check3 = (_osb < 0.25) & _prev_ne_zero()


# # Use np.where to assign values based on conditions
df['O_B_Dist_Check1'] = np.where(o_b_check1, 1, 0)
df['O_B_Dist_Check2'] = np.where(o_b_check2, 1, 0)
df['O_B_Dist_Check3'] = np.where(o_b_check3, 1, 0)

df['A_D_Dist_Check1']=None #(df['LAST_ALIGHTING_LOCATION_TO_DESTIN'] > 1.85) & (df['ORIGIN_TRANSPORTCode'].isin(['1', '2', '-oth-']))  if [ORIGIN_TO_FIRST_BOARD]>1.85 and ORIGIN_NEW_CODE = WALK [(Text.Contains([ORIGIN_TRANSPORT],"Walk") or Text.Contains([ORIGIN_TRANSPORT],"Wheelchair") or Text.Contains([ORIGIN_TRANSPORT],"Skateboard"))]
df['A_D_Dist_Check2']=None #(df['LAST_ALIGHTING_LOCATION_TO_DESTIN'] < 0.25) & (df['ORIGIN_TRANSPORTCode'].isin(['7', '8','9','10','11'])) if [ORIGIN_TO_FIRST_BOARD]<.25 and ORIGIN_NEW_CODE = "DRIVE" then 1 (Flag) else 0 (Non-Flag)
df['A_D_Dist_Check3']=None #(df['SURVEYALIGHTING_TO_DESTINATION'] < 0.25)  if [SURVEYALIGHTING_TO_DESTINATION]<0.25 and [#"NEXT_TRANSFERS[Code]"]!="0" then 1 (Flag) else 0 (Non-Flag)



a_d_check1 = (_lad > 1.85) & _destin_walk()
a_d_check2 = (_lad < 0.25) & _destin_drive()
a_d_check3 = (_sad < 0.25) & _next_ne_zero()

# # Use np.where to assign values based on conditions
df['A_D_Dist_Check1'] = np.where(a_d_check1, 1, 0)
df['A_D_Dist_Check2'] = np.where(a_d_check2, 1, 0)
df['A_D_Dist_Check3'] = np.where(a_d_check3, 1, 0)

df['O_D_Dist_Check1']=None #if [ORIGIN_TO_DESTINATION]<0.05 then 1 else 0 =DELETE
df['O_D_Dist_Check2']=None # if [ORIGIN_TO_DESTINATION]<0.25 then 1 else 0) = REVIEW
df['O_D_Dist_Check3']=None # if [ORIGIN_TO_DESTINATION]>50 then 1 else 0) = REVIEW

# Create boolean arrays for each condition
o_d_check1 = _od < 0.05
o_d_check2 = _od < 0.25
o_d_check3 = _od > 50

# Use np.where to assign values based on conditions
df['O_D_Dist_Check1'] = np.where(o_d_check1, 1, 0)
df['O_D_Dist_Check2'] = np.where(o_d_check2, 1, 0)
df['O_D_Dist_Check3'] = np.where(o_d_check3, 1, 0)

df['B_A_Dist_Check1']=None #if ["B2A/OD"]>1.75 then 1 (Flag) else 0 (Non-Flag)
df['B_A_Dist_Check2']=None #if [#"B2A/OD"]<0.45 and ORIGIN_NEW_CODE = "WALK" and DESTIN_NEW_CODE = "WALK"([#"PREV_TRANSFERS[Code]"]="0" and [#"NEXT_TRANSFERS[Code]"]="0") then 1 (Flag) else 0 (Non-Flag)
df['Wheelchair_Walk_Combo'] = None

# for index,row in df.iterrows():
b_a_check1 = _b2a > 1.75
b_a_check2 = (_b2a < 0.45) & _origin_walk() & _destin_walk() & _next_eq_zero() & _prev_eq_zero()

df['B_A_Dist_Check1']=np.where(b_a_check1,1,0)
df['B_A_Dist_Check2']=np.where(b_a_check2,1,0)


def _wheelchair_walk_combo(row):
    if _col_origin_transport is None or _col_destin_transport is None:
        return 0
    o = _normalize_transport_text(pd.Series([row[_col_origin_transport]])).iloc[0]
    d = _normalize_transport_text(pd.Series([row[_col_destin_transport]])).iloc[0]
    if (o in ['walk', 'walked'] and d == 'wheelchair') or (d in ['walk', 'walked'] and o == 'wheelchair'):
        return 1
    return 0

df['Wheelchair_Walk_Combo'] = df.apply(_wheelchair_walk_combo, axis=1)

_check_cols = ['O_B_Dist_Check1','O_B_Dist_Check2','O_B_Dist_Check3','A_D_Dist_Check1','A_D_Dist_Check2','A_D_Dist_Check3','O_D_Dist_Check1','O_D_Dist_Check2','O_D_Dist_Check3','B_A_Dist_Check1','B_A_Dist_Check2', 'Wheelchair_Walk_Combo']
# Ensure check columns are int 0/1 and any one flag sets overall flag
_df_checks = df[_check_cols].fillna(0).apply(pd.to_numeric, errors='coerce').fillna(0).astype(int)
df['SUM_ALL_CHECKS'] = np.where((_df_checks == 1).any(axis=1), 1, 0)

# When multiple rows exist per id, take max of each check so the kept row (after drop_duplicates) has correct flags
# Use merge (not map) to avoid id type mismatch (int vs float/str) which would give NaN and wrongly set SUM_ALL_CHECKS=0
_check_cols_plus_sum = _check_cols + ['SUM_ALL_CHECKS']
df['_id_agg'] = pd.to_numeric(df['id'], errors='coerce')
_max_by_id = df.groupby('_id_agg', as_index=False)[_check_cols_plus_sum].max()
_merged = df[['_id_agg']].merge(_max_by_id, on='_id_agg', how='left')
for col in _check_cols_plus_sum:
    df[col] = _merged[col].fillna(0).astype(int).values
df.drop(columns=['_id_agg'], inplace=True, errors='ignore')

# Diagnostic: for example IDs, show if they're in the checked set and their flag values
_diag_ids = [6321, 9722, 5206, 7469, 8377, 7455, 6805, 9747, 9723, 9719, 9519, 9453]
df_id_numeric = pd.to_numeric(df['id'], errors='coerce')
print("\n=== Diagnostic: example IDs in checked set ===")
for _did in _diag_ids:
    _mask = (df_id_numeric == _did)
    if _mask.any():
        _row = df.loc[_mask].iloc[0]
        _ob = _row.get('ORIGIN_TO_FIRST_BOARD', '')
        _ot = _row.get(_col_origin_transport, '') if _col_origin_transport else ''
        print(f"  id {_did}: IN SET | ORIGIN_TO_FIRST_BOARD={_ob} | origin_transport={_ot} | O_B_Check1={_row.get('O_B_Dist_Check1', '')} O_B_Check2={_row.get('O_B_Dist_Check2', '')} A_D_Check1={_row.get('A_D_Dist_Check1', '')} A_D_Check2={_row.get('A_D_Dist_Check2', '')} | SUM_ALL_CHECKS={_row.get('SUM_ALL_CHECKS', '')}")
    else:
        print(f"  id {_did}: NOT in checked set (dropped by Final_Usage or dropna)")
print("=== End diagnostic ===\n")

# print(df[df['SUM_ALL_CHECKS']==1])




powerbi_columns_checks=['id',
                        'homeaddresscity','homeaddresszip','homeaddresslat',
                        'homeaddresslong','homeaddressaddr','homeaddressplace','homeaddressstate',
                        'originplacetypecode','originplacetype','originaddressaddr','originaddresscity',
                        'originaddressstate','originaddresszip','originaddresslat','originaddresslong','origintransport',
                        'destinplacetypecode','destinplacetype','destinaddressaddr','destinaddresscity',
                        'destinaddressstate','destinaddresszip','destinaddresslat','destinaddresslong',
                       'destintransport','routesurveyedcode','routesurveyed']
powerbi_columns=check_all_characters_present(df,powerbi_columns_checks)

# powerbi_columns
new_df_columns=['FINAL_REVIEWER','ORIGIN_TO_SURVEYBOARD','ORIGIN_TO_FIRST_BOARD','SURVEYBOARDING_TO_SURVEYALIGHTING','ORIGIN_TO_DESTINATION','SURVEYALIGHTING_TO_DESTINATION','LAST_ALIGHTING_LOCATION_TO_DESTIN','O2B/O2D','B2A/OD','A2D/OD','O_B_Dist_Check1','O_B_Dist_Check2','O_B_Dist_Check3','A_D_Dist_Check1','A_D_Dist_Check2','A_D_Dist_Check3','O_D_Dist_Check1','O_D_Dist_Check2','O_D_Dist_Check3','B_A_Dist_Check1','B_A_Dist_Check2','Wheelchair_Walk_Combo','SUM_ALL_CHECKS']
distance_checks_columns=[*powerbi_columns,*boarding_columns,*origin_destin_columns,*transport_transfer_columns,*new_df_columns]

od_df=df[distance_checks_columns]

od_df = od_df.rename(columns={od_df.columns[1]: 'Final_Direction_Code', od_df.columns[2]: 'Final_Direction'})
od_df.drop_duplicates(subset='id', keep='first', inplace=True)


od_output_file = f'reviewtool_{today_date}_{project_name}_OD_Distance_Checks_auto_approved.csv'
flagged_df = od_df[od_df['SUM_ALL_CHECKS'] == 1].copy()
if flagged_df.empty:
    keep_cols = list(od_df.columns) if len(od_df.columns) else ["id", "SUM_ALL_CHECKS"]
    flagged_df = pd.DataFrame(columns=keep_cols)
    print(
        f"No OD distance check flags for {project_name} (zero flagged records). "
        "Writing header-only CSV and continuing to combining_distance_flags_auto_approval."
    )
flagged_df.to_csv(od_output_file, index=False)

print("#####################################################################")
print("File Created SuccessFully")
print("#####################################################################")

df = flagged_df

# Calculate total flagged records
total_flagged = flagged_df.shape[0]

# Reasons for flagging: count of each specified column where the value is 1
specified_columns = [
    'O_B_Dist_Check1', 'O_B_Dist_Check2', 'O_B_Dist_Check3',
    'A_D_Dist_Check1', 'A_D_Dist_Check2', 'A_D_Dist_Check3',
    'O_D_Dist_Check1', 'O_D_Dist_Check2', 'O_D_Dist_Check3',
    'B_A_Dist_Check1', 'B_A_Dist_Check2'
]
present_columns = [col for col in specified_columns if col in flagged_df.columns]
reasons_counts = (
    flagged_df[present_columns].apply(lambda x: (x == 1).sum()).to_dict()
    if present_columns and not flagged_df.empty
    else {col: 0 for col in specified_columns}
)

# Print summary
print("#####################################################################")
print("Analysis Summary:")
print(f"Total Flagged Records: {total_flagged}")
print("Reasons for Flagging:")
for reason, count in reasons_counts.items():
    print(f"  - {reason}: {count}")

print("#####################################################################")

# Optionally, save the summary to a text file
with open('analysis_summary(od_distance_checks).txt', 'w') as f:
    f.write("Analysis Summary:\n")
    f.write(f"Total Flagged Records: {total_flagged}\n")
    f.write("Reasons for Flagging:\n")
    for reason, count in reasons_counts.items():
        f.write(f"  - {reason}: {count}\n")

print("#####################################################################")

# === Enhanced Missing Data Reporting ===

# def report_missing_data(df):
#     print("\n=== Missing Data Summary ===")

#     # 1. Per-column missing counts & percentages
#     missing_counts = df.isna().sum()
#     total_rows = len(df)
#     missing_percentages = (missing_counts / total_rows) * 100

#     column_missing_df = (
#         pd.DataFrame({
#             'Missing Count': missing_counts,
#             'Missing %': missing_percentages
#         })
#         .sort_values(by='Missing Count', ascending=False)
#     )

#     print("\n--- Missing Values by Column ---")
#     print(column_missing_df.to_string())

#     # 2. Rows with missing in multiple critical columns
#     critical_columns = [
#         'STOP_ON_LAT', 'STOP_ON_LONG', 
#         'STOP_OFF_LAT', 'STOP_OFF_LONG'
#     ]
#     multi_missing_mask = df[critical_columns].isna().sum(axis=1) > 1
#     multi_missing_rows = df[multi_missing_mask]

#     if not multi_missing_rows.empty:
#         print(f"\n--- Rows with Multiple Missing Critical Fields ({len(multi_missing_rows)}) ---")
#         print(multi_missing_rows[critical_columns + ['id']].head(20).to_string())
#     else:
#         print("\nNo rows found with multiple missing critical fields.")

# # Call the report function
# report_missing_data(df)
