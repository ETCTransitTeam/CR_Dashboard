import pandas as pd
import geopy.distance
import copy
import math
from datetime import date
from pprint import pprint

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


today_date = date.today()
today_date=''.join(str(today_date).split('-'))

# df=pd.read_csv('elviscota2023obweekday_export_odbc(V2).csv')
# elvis_df=pd.read_csv('COTA_KINGElvis.csv')

project_name='PARK_CITY'

# file_name='DENVER_OB_KINGElvis.xlsx'
# detail_df=pd.read_excel("details_project_od_excel_DENVER.xlsx",sheet_name='STOPS')
# df=pd.read_csv('elvisdenverco2024obweekday_export_odbc.csv')
# elvis_df=pd.read_excel(file_name,sheet_name='Elvis_Review')

# file_name='BART_CA_KINGElvis.xlsx'
# detail_df=pd.read_excel("details_project_od_BART.xlsx",sheet_name='STOPS')
# df=pd.read_csv('elvisbartca2024interceptNEW_main_export_odbc.csv')
# elvis_df=pd.read_excel(file_name,sheet_name='Elvis_Review')

# file_name='VTA_CA_OB_KINGElvis.xlsx'
# detail_df=pd.read_excel("details_vta_CA_od_excel.xlsx",sheet_name='STOPS')
# df=pd.read_csv('elvisvtaca2024obweekday_export_odbc.csv')
# elvis_df=pd.read_excel(file_name,sheet_name='Elvis_Review')

# file_name='VTA_WEEKEND_KINGElvis.xlsx'
# detail_df=pd.read_excel("details_vta_CA_od_excel.xlsx",sheet_name='STOPS')
# df=pd.read_csv('elvisvtaca2024weekend_export_odbc.csv')
# elvis_df=pd.read_excel(file_name,sheet_name='in')

# file_name='ANCHORAGE_AK_KINGElvis.xlsx'
# detail_df=pd.read_excel("details_anchorage_ak_od_excel.xlsx",sheet_name='STOPS')
# df=pd.read_csv('elvisanchorageak2024obweekday_export_odbc.csv')
# elvis_df=pd.read_excel(file_name,sheet_name='Elvis_Review')

# file_name='CARTA_OB_KINGElvis.xlsx'
# detail_df=pd.read_excel("details_CARTA_SC_od_excel.xlsx",sheet_name='STOPS')
# df=pd.read_csv('elvischarleston_sc_obweekday_export_odbc.csv')
# elvis_df=pd.read_excel(file_name,sheet_name='Elvis_Review')

# file_name='CulverCity_CA_KINGElvis.xlsx'
# detail_df=pd.read_excel("details_CulverCity_CA_od_excel.xlsx",sheet_name='STOPS')
# df=pd.read_csv('elvisculver_city_ca_2024_obweekday_export_odbc.csv')
# elvis_df=pd.read_excel(file_name,sheet_name='Elvis_Review')

# file_name='Buffalo_NY_OB_KINGElvis.xlsx'
# detail_df=pd.read_excel("details_buffalo_excel_template.xlsx",sheet_name='STOPS')
# df=pd.read_csv('elvisbuffalony2024obweekday_export_odbc.csv')
# elvis_df=pd.read_excel(file_name,sheet_name='Elvis_Review')

# file_name='EMBARK_OK_KINGElvis.xlsx'
# detail_df=pd.read_excel("details_project_od_excel_EMBARK.xlsx",sheet_name='STOPS')
# df=pd.read_csv('elvisembark2024obweekday_export_odbc.csv')
# elvis_df=pd.read_excel(file_name,sheet_name='Elvis_Review')

# file_name='SEATTLE_WA_KINGElvis.xlsx'
# detail_df=pd.read_excel("details_project_od_Seattle2.xlsx",sheet_name='STOPS')
# df=pd.read_csv('elvisseattlewa2024obweekday_export_odbc.csv')
# elvis_df=pd.read_excel(file_name,sheet_name='Elvis_Review')
 
# file_name = 'COTA_KINGElvis (4).xlsx'
# elvis_df=pd.read_excel('COTA_KINGElvis (4).xlsx',sheet_name='Elvis_Review')
# detail_df=pd.read_excel("details_project_od_excel_COTA (1).xlsx",sheet_name='AIR')
# df=pd.read_csv('elviscota2023obweekday_export_odbc(V3).csv')

# file_name = 'SantaClarita_KINGElvis.xlsx'
# elvis_df=pd.read_excel(file_name,sheet_name='Elvis_Review')
# detail_df=pd.read_excel("details_project_od_SantaClarita (1).xlsx",sheet_name='AIR')
# df=pd.read_csv('elvissantaclarita2023obweekday_export_odbc(v3).csv')

# elvis_df=pd.read_excel('2023_GCRTA_KINGElvis (2).xlsx',sheet_name='Elvis_Review')

# file_name = 'BCT_KINGElvis.xlsx'
# detail_df=pd.read_excel("details_project_od_excel_BCT.xlsx",sheet_name='STOPS')


# file_name = 'OKI_KINGElvis (2).xlsx'
# detail_df=pd.read_excel("details_project_od_excel_OKI.xlsx",sheet_name='STOPS')
# df=pd.read_csv('elviscincinnatioh2023obweekday_export_odbc(v2).csv')
# elvis_df=pd.read_excel(file_name,sheet_name='Elvis_Review')

# file_name='MUNI_CA_KINGElvis.xlsx'
# detail_df=pd.read_excel("details_project_od_MUNI.xlsx",sheet_name='STOPS')
# df=pd.read_csv('elvismunica2024obweekday_export_odbc.csv')
# elvis_df=pd.read_excel(file_name,sheet_name='Elvis_Review')


# file_name='Tucson_az_2025_KINGElvis.xlsx'
# detail_df=pd.read_excel("details_TUCSON_AZ_od_excel.xlsx",sheet_name='STOPS')
# df=pd.read_csv('elvistucson2025obweekday_export_odbc.csv')
# elvis_df=pd.read_excel(file_name,sheet_name='Elvis_Review')

# file_name='BUFFALO_RAIL_KINGElvis.xlsx'
# detail_df=pd.read_excel("details_buffalo_excel_template.xlsx",sheet_name='STOPS')
# df=pd.read_csv('elvisbuffalonyrail2024obweekday_export_odbc.csv')
# elvis_df=pd.read_excel(file_name,sheet_name='Elvis_Review')

# file_name='FRESNO_CA_KINGElvis (6).xlsx'
# detail_df=pd.read_excel("details_project_od_FRESNO.xlsx",sheet_name='STOPS')
# df=pd.read_csv('elvisfresno2024obweekday_export_odbc(v5).csv')
# elvis_df=pd.read_excel(file_name,sheet_name='Elvis_Review')


# file_name='SALEMOR_KINGElvis.xlsx'
# detail_df=pd.read_excel("details_Salem_ORE_excel (1).xlsx",sheet_name='STOPS')
# df=pd.read_csv('elvissalemor2024obweekday_export_odbc.csv')
# elvis_df=pd.read_excel(file_name,sheet_name='Elvis_Review')

# UTA
# file_name='UTA_SLC_KINGElvis (6).xlsx'
# detail_df=pd.read_excel("details_project_od_excel_UTA (3).xlsx",sheet_name='STOPS')
# df=pd.read_csv('elvisutaobweekday2024_export_odbc(v5).csv')
# elvis_df=pd.read_excel(file_name,sheet_name='Elvis_Review')

# file_name = 'LACMTA_KINGElvis (11).xlsx'
# detail_df=pd.read_excel("details_LACMTA_CA_excel (5).xlsx",sheet_name='STOPS')
# df=pd.read_csv('elvislacmta2023obweekday_export_odbc.csv')
# df=df.tail(500)
# elvis_df=pd.read_excel(file_name,sheet_name='Elvis_Review')

# file_name = 'RENO_KINGElvis (7).xlsx'
# elvis_df=pd.read_excel(file_name,sheet_name='Elvis_Review')
# detail_df=pd.read_excel("details_project_od_RENO (4).xlsx",sheet_name='STOPS')
# df=pd.read_csv('elvisreno_od2023_weekday_export_odbc(V4).csv')

# file_name='KCATA_2025_KINGElvis.xlsx'
# detail_df=pd.read_excel("details_KCATA_od_excel.xlsx",sheet_name='STOPS')
# df1=pd.read_csv('elvis_transit_ls6_224559_export_odbc.csv')
# elvis_df=pd.read_excel(file_name,sheet_name='Elvis_Review')

# file_name='ACT_2025_KINGElvis.xlsx'
# detail_df=pd.read_excel("details_AC_Transit_od_excel.xlsx",sheet_name='STOPS')
# df1=pd.read_csv('elvis_transit_ls6_348879_export_odbc.csv')
# elvis_df=pd.read_excel(file_name,sheet_name='Elvis_Review')

# file_name='SALEM_OR_2025_KINGElvis.xlsx'
# detail_df=pd.read_excel("details_project_od_Salem.xlsx",sheet_name='STOPS')
# df1=pd.read_csv('elvis_transit_ls6_179191_export_odbc.csv')
# elvis_df=pd.read_excel(file_name,sheet_name='Elvis_Review')

# file_name='LACMTA_FEEDER_2025_KINGElvis.xlsx'
# file_path="details_lacmta-feeder_733524_od_excel (2).xlsx"
# df1=pd.read_csv('elvis_transit_ls6_733524_export_odbc.csv')
# elvis_df=pd.read_excel(file_name,sheet_name='Elvis_Review')

file_name='PARK_CITY_UT_2026_KINGElvis.xlsx'
file_path="details_ParkCity_154732_od_excel.xlsx"
df1=pd.read_csv('elvis_transit_ls6_154732_export_odbc.csv')
elvis_df=pd.read_excel(file_name,sheet_name='Elvis_Review')

# file_name='INDYGO_BRT_2026_KINGElvis.xlsx'
# file_path="details_lndyGO_574774_od_excel.xlsx"
# df1=pd.read_csv('elvis_transit_ls6_574774_export_odbc.csv')
# elvis_df=pd.read_excel(file_name,sheet_name='Elvis_Review')


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

df['PREV_TRANSFERSCode'] = df['PREV_TRANSFERSCode'].fillna(0).astype(int)
df['NEXT_TRANSFERSCode'] = df['NEXT_TRANSFERSCode'].fillna(0).astype(int)

if file_name.split('_')[0].isdigit():
    file_first_name=file_name.split('_')[0]+'_'+file_name.split('_')[1]
else:
    file_first_name=file_name.split('_')[0]

small_columns=['reviewreviewer', 'reviewusage', 'finalusage', 'elvisdate', 'id', 'routesurveyedcode', 'prevtransferscode', 'prevtransfers', 'tripfirstroutecode', 'tripsecondroutecode', 'tripthirdroutecode', 'tripfourthroutecode', 'nexttransferscode', 'nexttransfers', 'tripnextroutecode', 'tripafterroutecode', 'trip3rdroutecode', 'triplast4thrtecode', 'stoponlat', 'stoponlong', 'stopofflat', 'stopofflong',  'latprvon1rad', 'lonprvon1rad', 'latprvon2rad', 'lonprvon2rad', 'latprvon3rad', 'lonprvon3rad', 'latprvon4rad', 'lonprvon4rad', 'latprvoff1rad', 'lonprvoff1rad', 'latprvoff2rad', 'lonprvoff2rad', 'latprvoff3rad', 'lonprvoff3rad', 'latprvoff4rad', 'lonprvoff4rad', 'latboardrad', 'lonboardrad', 'latalightrad', 'lonalightrad', 'latnxton1rad', 'lonnxton1rad', 'latnxton2rad', 'lonnxton2rad', 'latnxton3rad', 'lonnxton3rad', 'latnxton4rad', 'lonnxton4rad', 'latnxtoff1rad', 'lonnxtoff1rad', 'latnxtoff2rad', 'lonnxtoff2rad', 'latnxtoff3rad', 'lonnxtoff3rad', 'latnxtoff4rad', 'lonnxtoff4rad', 'idview','distanceapproved', 'distancetransfercheck', 'idconfirm']

# df=pd.read_csv('elvishrtva2023obweekday_export_odbc(new3) (2).csv')

elvis_date_check=['elvisdate']
elvis_date=check_all_characters_present(elvis_df,elvis_date_check)

df = df.merge(elvis_df[[elvis_date[0], 'id', 'Final_Usage','FINAL_REVIEWER']], on='id', how='left')


df=df[df['Final_Usage'].str.lower()=='use']
# df=df[(df['Elvis_Date']==elvis_date)& (df['Final_Usage'].str.lower()=='use')]


matched_columns=check_all_characters_present(df,small_columns)

transfer_columns_checks=['prevtran1onbuslat', 'prevtran1onbuslong',
                  'prevtran1offbuslat', 'prevtran1offbuslong', 'prevtran2onbuslat', 
                  'prevtran2onbuslong', 'prevtran2offbuslat', 'prevtran2offbuslong',
                  'prevtran3onbuslat', 'prevtran3onbuslong', 'prevtran3offbuslat',
                  'prevtran3offbuslong', 'prevtran4onbuslat', 'prevtran4onbuslong', 
                  'prevtran4offbuslat', 'prevtran4offbuslong','nexttran1onbuslat',
                  'nexttran1onbuslong', 'nexttran1offbuslat', 'nexttran1offbuslong', 
                  'nexttran2onbuslat', 
                  'nexttran2onbuslong', 'nexttran2offbuslat', 'nexttran2offbuslong', 
                  'nexttran3onbuslat', 'nexttran3onbuslong',
                  'nexttran3offbuslat', 'nexttran3offbuslong', 'nexttran4onbuslat', 'nexttran4onbuslong',
                  'nexttran4offbuslat', 'nexttran4offbuslong']

transfer_columns=check_all_characters_present(df,transfer_columns_checks)

distance_flags=df.loc[:,transfer_columns]
transfer_data_list=distance_flags.values.tolist()


id_transformer_columns = copy.deepcopy(transfer_columns)
id_transformer_columns.insert(0,'id')
df1=df.loc[:,id_transformer_columns]

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
        # Handle the exception here
        print(f"Error calculating distance: {e}")  # Change to the desired distance unit


def compute_no_of_transfer_gps(row):
    transfer_pairs = [
    (f"PREV_TRAN_{i}_ON_BUS_LAT", f"PREV_TRAN_{i}_ON_BUS_LONG", f"PREV_TRAN_{i}_OFF_BUS_LAT", f"PREV_TRAN_{i}_OFF_BUS_LONG") 
    for i in range(1, 5)
] + [
    (f"NEXT_TRAN_{i}_ON_BUS_LAT", f"NEXT_TRAN_{i}_ON_BUS_LONG", f"NEXT_TRAN_{i}_OFF_BUS_LAT", f"NEXT_TRAN_{i}_OFF_BUS_LONG") 
    for i in range(1, 5)
]

# transfer_pairs    
    count = 0
    for lat_on, lon_on, lat_off, lon_off in transfer_pairs:
        on_present = not pd.isna(row[lat_on]) and not pd.isna(row[lon_on])
        off_present = not pd.isna(row[lat_off]) and not pd.isna(row[lon_off])
        
        if on_present and off_present:      
            count += 1
        elif on_present or off_present:
            count += 0.5
    return count

# # Apply the function to each row
df1['noOfTransferGPS'] = df1.apply(compute_no_of_transfer_gps, axis=1)

def compute_no_of_transfer_gps_distance(row):
    transfer_pairs = [
        (f"PREV_TRAN_{i}_ON_BUS_LAT", f"PREV_TRAN_{i}_ON_BUS_LONG", f"PREV_TRAN_{i}_OFF_BUS_LAT", f"PREV_TRAN_{i}_OFF_BUS_LONG") 
        for i in range(1, 5)
    ] + [
        (f"NEXT_TRAN_{i}_ON_BUS_LAT", f"NEXT_TRAN_{i}_ON_BUS_LONG", f"NEXT_TRAN_{i}_OFF_BUS_LAT", f"NEXT_TRAN_{i}_OFF_BUS_LONG") 
        for i in range(1, 5)
    ]

    distances = {}  # Dictionary to store distances for each transfer route
    for i, (lat_on, lon_on, lat_off, lon_off) in enumerate(transfer_pairs, start=1):
        on_present = not pd.isna(row[lat_on]) and not pd.isna(row[lon_on])
        off_present = not pd.isna(row[lat_off]) and not pd.isna(row[lon_off])

        if on_present and off_present:
            distance = get_distance_between_coordinates(row[lat_on], row[lon_on], row[lat_off], row[lon_off])
            distances[f"Transfer{i}_onroute_distance"] = distance
        elif on_present or off_present:
            distances[f"Transfer{i}_onroute_distance"] = None  # Set to None if only one point is present

    return pd.Series(distances)  # Convert the dictionary to a Series
# Apply the function to each row and add the resulting columns to df1
df1 = pd.concat([df1, df1.apply(compute_no_of_transfer_gps_distance, axis=1)], axis=1)


# df1=df1.merge(elvis_df[['elvis_date', 'id', 'FINAL_REVIEWER','Final_Usage']], on='id', how='left')
df1=df1.merge(elvis_df[[elvis_date[0], 'id', 'FINAL_REVIEWER','Final_Usage']], on='id', how='left')

df2=df[matched_columns]




df1 = df2.merge(df1, on='id', how='left')
# df1 = df1.merge(df[matched_columns], on='id', how='right')

# To calculate Transfer1_Distance Columns

def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calculate the Haversine distance between two points on the earth (specified in decimal degrees) in miles.
    """
    # Convert decimal degrees to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])

    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    r = 3956  # Radius of Earth in miles
    return r * c

def calculate_distances_with_adjusted_long(row_list):
    """
    Calculate distances for a given row list after adjusting the longitudes with additional type checks.
    """
    distances = []

    # Adjusting the longitudes and ensuring the values are floats
    adjusted_row_list = []
    for i, value in enumerate(row_list):
        try:
            value = float(value)
        except ValueError:
            continue

        if i % 2 != 0 and value < 0:
            adjusted_row_list.append(-value)
        else:
            adjusted_row_list.append(value)

    # If there are only 4 or fewer values, we don't calculate any distance
    if len(adjusted_row_list) <= 4:
        return distances

    # Skipping the first 2 and last 2 indices
    adjusted_row_list = adjusted_row_list[2:-2]

    # Calculating distances for every consecutive pair of latitude and longitude 
    # with a step of 4 to match the provided requirements
    for i in range(0, len(adjusted_row_list) - 3, 4):
        lat1, lon1 = adjusted_row_list[i], adjusted_row_list[i+1]
        lat2, lon2 = adjusted_row_list[i+2], adjusted_row_list[i+3]
        distances.append(haversine_distance(lat1, lon1, lat2, lon2))

    return distances


def transfer_distance_columns_with_ids(df):
    columns_specified = [
        'PREV_TRAN_1_ON_BUS_LAT', 'PREV_TRAN_1_ON_BUS_LONG', 'PREV_TRAN_1_OFF_BUS_LAT', 'PREV_TRAN_1_OFF_BUS_LONG',
        'PREV_TRAN_2_ON_BUS_LAT', 'PREV_TRAN_2_ON_BUS_LONG', 'PREV_TRAN_2_OFF_BUS_LAT', 'PREV_TRAN_2_OFF_BUS_LONG',
        'PREV_TRAN_3_ON_BUS_LAT', 'PREV_TRAN_3_ON_BUS_LONG', 'PREV_TRAN_3_OFF_BUS_LAT', 'PREV_TRAN_3_OFF_BUS_LONG',
        'PREV_TRAN_4_ON_BUS_LAT', 'PREV_TRAN_4_ON_BUS_LONG', 'PREV_TRAN_4_OFF_BUS_LAT', 'PREV_TRAN_4_OFF_BUS_LONG',
        'STOP_ON_LAT', 'STOP_ON_LONG', 'STOP_OFF_LAT', 'STOP_OFF_LONG',
        'NEXT_TRAN_1_ON_BUS_LAT', 'NEXT_TRAN_1_ON_BUS_LONG', 'NEXT_TRAN_1_OFF_BUS_LAT', 'NEXT_TRAN_1_OFF_BUS_LONG',
        'NEXT_TRAN_2_ON_BUS_LAT', 'NEXT_TRAN_2_ON_BUS_LONG', 'NEXT_TRAN_2_OFF_BUS_LAT', 'NEXT_TRAN_2_OFF_BUS_LONG',
        'NEXT_TRAN_3_ON_BUS_LAT', 'NEXT_TRAN_3_ON_BUS_LONG', 'NEXT_TRAN_3_OFF_BUS_LAT', 'NEXT_TRAN_3_OFF_BUS_LONG',
        'NEXT_TRAN_4_ON_BUS_LAT', 'NEXT_TRAN_4_ON_BUS_LONG', 'NEXT_TRAN_4_OFF_BUS_LAT', 'NEXT_TRAN_4_OFF_BUS_LONG'
    ]
    
    lists_per_row = df[columns_specified].apply(lambda row: [x for x in row if pd.notnull(x)], axis=1)


    distance_lists = lists_per_row.apply(calculate_distances_with_adjusted_long)
    max_distances = distance_lists.apply(len).max()
    df1 = pd.DataFrame(distance_lists.tolist(), columns=[f"Distance{i+1}" for i in range(max_distances)])
    
    return df1


def assign_distances_based_on_trip_columns(df, distance_df):
    trip_columns = [
        'TRIP_FIRST_ROUTECode', 'TRIP_SECOND_ROUTECode', 'TRIP_THIRD_ROUTECode', 'TRIP_FOURTH_ROUTECode',
        'TRIP_NEXT_ROUTECode', 'TRIP_AFTER_ROUTECode', 'TRIP_3RD_ROUTECode', 'TRIP_LAST4TH_RTECode'
    ]

    distance_columns = [f"Transfer{i+1}_Distance" for i in range(8)]

    for idx, row in df.iterrows():
        # Retrieve the distances for the row using the 'id' column
        matched_rows = distance_df.loc[distance_df['id'] == row['id']]
        
        matched_value = matched_rows.iloc[0]
        if isinstance(matched_value, pd.Series):
            distances = matched_value.drop('id', errors='ignore').dropna().tolist()
        else:
            # Assuming matched_value is the distance (or NaN)
            distances = [matched_value] if pd.notna(matched_value) else []

        # For each trip column, if a value is present, assign the next available distance value
        for trip_col, dist_col in zip(trip_columns, distance_columns):
            if pd.notna(row[trip_col]) and distances:
                df.at[idx, dist_col] = distances.pop(0)
            else:
                df.at[idx, dist_col] = None

    return df


# --- TRIP vs GPS consistency: flag when a TRIP route has value but GPS lat/long for that transfer is missing ---
# Mapping: each TRIP_* column -> (lat_on, lon_on, lat_off, lon_off) required for that transfer
TRIP_TO_GPS_COLUMNS = [
    ('TRIP_FIRST_ROUTECode', ('PREV_TRAN_1_ON_BUS_LAT', 'PREV_TRAN_1_ON_BUS_LONG', 'PREV_TRAN_1_OFF_BUS_LAT', 'PREV_TRAN_1_OFF_BUS_LONG')),
    ('TRIP_SECOND_ROUTECode', ('PREV_TRAN_2_ON_BUS_LAT', 'PREV_TRAN_2_ON_BUS_LONG', 'PREV_TRAN_2_OFF_BUS_LAT', 'PREV_TRAN_2_OFF_BUS_LONG')),
    ('TRIP_THIRD_ROUTECode', ('PREV_TRAN_3_ON_BUS_LAT', 'PREV_TRAN_3_ON_BUS_LONG', 'PREV_TRAN_3_OFF_BUS_LAT', 'PREV_TRAN_3_OFF_BUS_LONG')),
    ('TRIP_FOURTH_ROUTECode', ('PREV_TRAN_4_ON_BUS_LAT', 'PREV_TRAN_4_ON_BUS_LONG', 'PREV_TRAN_4_OFF_BUS_LAT', 'PREV_TRAN_4_OFF_BUS_LONG')),
    ('TRIP_NEXT_ROUTECode', ('NEXT_TRAN_1_ON_BUS_LAT', 'NEXT_TRAN_1_ON_BUS_LONG', 'NEXT_TRAN_1_OFF_BUS_LAT', 'NEXT_TRAN_1_OFF_BUS_LONG')),
    ('TRIP_AFTER_ROUTECode', ('NEXT_TRAN_2_ON_BUS_LAT', 'NEXT_TRAN_2_ON_BUS_LONG', 'NEXT_TRAN_2_OFF_BUS_LAT', 'NEXT_TRAN_2_OFF_BUS_LONG')),
    ('TRIP_3RD_ROUTECode', ('NEXT_TRAN_3_ON_BUS_LAT', 'NEXT_TRAN_3_ON_BUS_LONG', 'NEXT_TRAN_3_OFF_BUS_LAT', 'NEXT_TRAN_3_OFF_BUS_LONG')),
    ('TRIP_LAST4TH_RTECode', ('NEXT_TRAN_4_ON_BUS_LAT', 'NEXT_TRAN_4_ON_BUS_LONG', 'NEXT_TRAN_4_OFF_BUS_LAT', 'NEXT_TRAN_4_OFF_BUS_LONG')),
]


def _trip_has_value(val):
    """True if the value indicates a transfer route is present (avoids false positives for empty/zero)."""
    if pd.isna(val):
        return False
    if isinstance(val, str) and str(val).strip() == '':
        return False
    if isinstance(val, (int, float)) and val == 0:
        return False
    return True


def _gps_complete_for_transfer_vectorized(df, gps_cols):
    """True where all four GPS columns are present and numeric (vectorized)."""
    sub = df[list(gps_cols)]
    non_null = sub.notna().all(axis=1)
    numeric = sub.apply(pd.to_numeric, errors="coerce")
    numeric_ok = numeric.notna().all(axis=1)
    return non_null & numeric_ok


def compute_missing_gps_flag(df):
    """
    For each row: set Missing_GPS_Flag True if ANY TRIP_* has a value but the
    corresponding GPS lat/long set for that transfer is missing. Uses only
    columns that exist in df to support varying schemas.
    """
    cols = set(df.columns)
    flags = pd.Series(False, index=df.index)
    for trip_col, gps_cols in TRIP_TO_GPS_COLUMNS:
        if trip_col not in cols or not all(c in cols for c in gps_cols):
            continue
        trip_has_val = df[trip_col].apply(_trip_has_value)
        gps_ok = _gps_complete_for_transfer_vectorized(df, gps_cols)
        flags = flags | (trip_has_val & ~gps_ok)
    return flags

# Usage:
df2 = transfer_distance_columns_with_ids(df1)
df2['id'] = df1['id']

df1 = assign_distances_based_on_trip_columns(df1, df2)

# Flag records where a TRIP route has a value but required GPS for that transfer is missing
df1['Missing_GPS_Flag'] = compute_missing_gps_flag(df1)

no_of_gps_column=['nooftransfergps']
transfer_onroute_distance_checks=['transfer1onroutedistance','transfer2onroutedistance',
                 'transfer3onroutedistance','transfer4onroutedistance','transfer5onroutedistance',
                 'transfer6onroutedistance','transfer7onroutedistance','transfer8onroutedistance']
transfer_distance_checks=['transfer1distance','transfer2distance',
                 'transfer3distance','transfer4distance','transfer5distance',
                 'transfer6distance','transfer7distance','transfer8distance']


number_of_gps=check_all_characters_present(df1,no_of_gps_column)
transfer_onroute_distance=check_all_characters_present(df1,transfer_onroute_distance_checks)
transfer_dsistance_columns=check_all_characters_present(df1,transfer_distance_checks)
distance_flags=df1.loc[:,transfer_columns]
transfer_distance_flags=df1.loc[:,transfer_dsistance_columns]
transfergps_flags=df1.loc[:,number_of_gps]
route_flags=df1.loc[:,transfer_onroute_distance]
transfer_data_list=distance_flags.values.tolist()
distance_data_list=route_flags.values.tolist()
transfer_distance_data_list=transfer_distance_flags.values.tolist()
transfer_gps_data_list=transfergps_flags.values.tolist()



def transfer_distance_flags(transfer_gps_data_list, transfer_distance_data_list, distance_data_list):
    new_column = []  # 0=Good Values 1=Flagged values 
    
    for i, transfer_row in enumerate(transfer_gps_data_list):
        for transfer_value in transfer_row:
            fractional_part = transfer_value % 1
            
            if fractional_part == 0:
                # non_zero_values = [value for value in distance_data_list[i] if not pd.isna(value) and value != 0 and value != 0.0]
                non_zero_values = [value for value in distance_data_list[i] if not pd.isna(value)]
                # non_zero_distance_values = [value for value in transfer_distance_data_list[i] if not pd.isna(value) and value != 0 and value != 0.0]
                non_zero_distance_values = [value for value in transfer_distance_data_list[i] if not pd.isna(value)]
                if not non_zero_values or not non_zero_distance_values:
                    new_column.append(0)
                else:
                    value_approval = all(value > 0.075 for value in non_zero_values)
                    distance_approval = all(value <= 0.25 or value==0 or value==0.0 for value in non_zero_distance_values)

                    if value_approval and distance_approval:
                        new_column.append(0)
                    else:
                        new_column.append(1)
            else:
                new_column.append(1)
    
    return new_column


new_column=transfer_distance_flags(transfer_gps_data_list,transfer_distance_data_list,distance_data_list)

_pre_flag_columns = list(df1.columns)
df1['Flaged'] = new_column
# Also flag records where TRIP has value but GPS for that transfer is missing
df1.loc[df1['Missing_GPS_Flag'], 'Flaged'] = 1
df1 = df1[df1['Flaged'] == 1]

# df2 = df2.merge(elvis_df[['Elvis_Date', 'id', 'Final_Usage']], on='id', how='left')
# Your agency list
agency_list = ["MDT_1_999", 'TRI_1_TR', 'PLM_1_999', 'BRI_1_B1']

# Check if any value in agency_list is present in any cell of a row and drop those rows
df1 = df1[~df1.apply(lambda row: any(val in agency_list for val in row), axis=1)]
df1.drop_duplicates(subset='id',inplace=True)
# df2.to_excel("123_distances_output_merged.xlsx", index=False)
# df1.to_csv(f'reviewtool_{today_date}_{project_name}_Distance_Transfer_Flags(v{version}).csv', index=False)
output_file = f'reviewtool_{today_date}_{project_name}_Distance_Transfer_Flags.csv'
if df1.empty:
    keep_cols = _pre_flag_columns if _pre_flag_columns else ["id"]
    df1 = pd.DataFrame(columns=keep_cols)
    print(
        f"No transfer distance flags for {project_name} (zero flagged records). "
        "Writing header-only CSV and continuing pipeline."
    )
df1.to_csv(output_file, index=False)

print("File GENERATED SUCCESSFULLY")


def generate_summary(df1):
    # Initialize summary dictionary
    summary = {
        "Total Records": len(df1),
        "Total Flagged Records": 0,
        "Reasons for Flagging": {},
        "Average Transfer Distances": {},
        "Number of GPS Transfers Summary": {},
    }

    if df1.empty:
        pprint(summary)
        return summary

    # Calculate total flagged records based on certain conditions
    # Here, assuming records are flagged if the number of transfers or distances exceed a threshold
    distance_cols = [c for c in [f"Transfer{i}_Distance" for i in range(1, 5)] if c in df1.columns]
    flagged_mask = df1["noOfTransferGPS"] > 2 if "noOfTransferGPS" in df1.columns else pd.Series(False, index=df1.index)
    if distance_cols:
        flagged_mask = flagged_mask | (df1[distance_cols].max(axis=1) > 10)
    flagged_records = df1[flagged_mask]
    summary["Total Flagged Records"] = len(flagged_records)
    
    # Reasons for flagging
    if len(flagged_records) > 0:
        for index, row in flagged_records.iterrows():
            reason = []
            if "noOfTransferGPS" in row and row["noOfTransferGPS"] > 2:
                reason.append(f"High number of GPS transfers: {row['noOfTransferGPS']}")
            if distance_cols and row[distance_cols].max() > 10:
                reason.append("Large transfer distance detected.")
            summary["Reasons for Flagging"][row["id"]] = "; ".join(reason)
    
    # Average distances for transfer distances
    transfer_distance_columns = [f"Transfer{i}_Distance" for i in range(1, 9)]
    for col in transfer_distance_columns:
        if col in df1.columns:
            summary["Average Transfer Distances"][col] = df1[col].mean()

    # Summary of number of GPS transfers
    if "noOfTransferGPS" in df1.columns:
        gps_transfer_stats = df1["noOfTransferGPS"].describe()
        summary["Number of GPS Transfers Summary"] = gps_transfer_stats.to_dict()

    # Output summary
    pprint(summary)
    return summary


summary = generate_summary(df1)