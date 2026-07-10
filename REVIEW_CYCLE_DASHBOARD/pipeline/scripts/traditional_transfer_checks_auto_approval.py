import pandas as pd
from math import sin, cos, sqrt, atan2, radians
import os
import numpy as np
from datetime import date
from collections import Counter
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

project_name='INDY_GO'


# file_name='STL_MO_2025_KINGElvis.xlsx'
# detail_df=pd.read_excel("details_saint_louis_MO_od_excel.xlsx",sheet_name='STOPS')
# df=pd.read_csv('elvisstlouis2025obweekday_export_odbc.csv')
# elvis_df=pd.read_excel(file_name,sheet_name='Elvis_Review')

# file_name='PARK_CITY_KINGElvis_auto_approval_20260404.csv'
# file_path="details_ParkCity_154732_od_excel.xlsx"
# df1=pd.read_csv('elvis_transit_ls6_154732_export_odbc.csv')

file_name='INDY_GO_KINGElvis_auto_approval_20260411.csv'
file_path="details_lndyGO_574774_od_excel.xlsx"
df1=pd.read_csv('elvis_transit_ls6_574774_export_odbc.csv')
elvis_df=pd.read_csv(file_name)

df1 = df1.drop(0).reset_index(drop=True)

# Normalize LBT route versions: convert _2 to _3 only for LBT routes in df1 (safe for mixed dtypes)
for col in df1.columns:
    if df1[col].dtype == object:
        df1[col] = df1[col].apply(
            lambda x: x.replace('LBT_2_', 'LBT_3_') if isinstance(x, str) and x.startswith('LBT_2_') else x
        )



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

elvis_date_check=['elvisdate']
elvis_date=check_all_characters_present(elvis_df,elvis_date_check)

# df = df.merge(elvis_df[['elvis_date', 'id', 'Final_Usage']], on='id', how='left')
df = df.merge(elvis_df[[elvis_date[0], 'id', 'Final_Usage','FINAL_REVIEWER']], on='id', how='left')

# latest_date = pd.to_datetime(df['Elvis_Date']).max()

# df = df[df['id'] > 14165]
df=df[df['Final_Usage'].str.lower()=='use']
# df=df[(df['Elvis_Date']==latest_date)& (df['Final_Usage'].str.lower()=='use')]

# Check the duplicates
# ROUTE_SURVEYED[Code]
duplicate_records_checks=['routesurveyedcode','prevtran1onbuslat', 'prevtran1onbuslong',
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
duplicate_columns=check_all_characters_present(df,duplicate_records_checks)
duplicates = df.drop_duplicates(subset=duplicate_columns)


# PREV and NEXT transfer should match the route name
prev_transfer_codes=['prevtransferscode','tripfirstroutecode','tripsecondroutecode','tripthirdroutecode','tripfourthroutecode']
next_transfer_codes=['nexttransferscode','tripnextroutecode','tripafterroutecode','trip3rdroutecode','triplast4thrtecode']
prev_transfer_columns=check_all_characters_present(df,prev_transfer_codes)
next_transfer_columns=check_all_characters_present(df,next_transfer_codes)
prev_transfer_columns,next_transfer_columns

prev_df=df.loc[:,prev_transfer_columns]
next_df=df.loc[:,next_transfer_columns]
prev_df_list=prev_df.values.tolist()
next_df_list=next_df.values.tolist()


def transfer_route_check(df,prev_df_list,next_df_list):
    # Assuming both lists have the same length
    status=[]
    for i in range(len(prev_df_list)):
        prev_data = prev_df_list[i]
        next_data = next_df_list[i]
        prev_count = sum(1 for j in range(1, len(prev_data)) if not pd.isna(prev_data[j]))
        next_count = sum(1 for j in range(1, len(next_data)) if not pd.isna(next_data[j]))
        if prev_count == prev_data[0] and next_count == next_data[0]:
            status.append(0)            
        else:
            status.append(1)
    return status
status_transfers=transfer_route_check(df,prev_df_list,next_df_list)

df['Status']=status_transfers

# desired_columns = ['elvis_date', 'Final_Usage', 'id']  # Specify the desired column order
desired_columns = [elvis_date[0], 'Final_Usage', 'id']  # Specify the desired column order

# Ensure that all desired columns exist in your DataFrame
missing_columns = [col for col in desired_columns if col not in df.columns]
if missing_columns:
    raise ValueError(f"Columns {missing_columns} do not exist in df1.")

# Reorder the columns to make them first
df = df[desired_columns + [col for col in df.columns if col not in desired_columns]]
# df.to_csv("Traditional Transfer Checks.csv",index=False)



# Good Transfer Flags
# def split_etc_route_id(value):
#     value_list = value.split('_')
#     etc_id = '_'.join(value_list[:-1])
#     return etc_id

# df['ETC_ROUTE_ID_New']=df['ETC_ROUTE_ID'].apply(split_etc_route_id)
# print('ETC_ROUTE_ID Splitted Successfully')
# df.drop_duplicates(subset=['ETC_ROUTE_ID_New'],inplace=True)

stops_columns_to_check=['stoplat','stoplon','xferrouteid']
stops_columns=check_all_characters_present(detail_df,stops_columns_to_check)

stops_df=detail_df.loc[:,stops_columns]
stops_df_list=stops_df.values.tolist()

# Approximate radius of earth in km
R = 6373.0
R_MILES = R * 0.621371


def get_distance_between_coordinates(start_lat, start_lng, end_lat, end_lng):
    lat1 = radians(start_lat)
    lon1 = radians(start_lng)
    lat2 = radians(end_lat)
    lon2 = radians(end_lng)

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance_km = R * c
    distance_in_miles = distance_km * 0.621371  # Convert km to miles
    return distance_in_miles


def _haversine_vector_miles(lat0, lon0, lats, lons):
    """Vectorized: distance from (lat0, lon0) to each (lats[i], lons[i]) in miles."""
    lat0_r = radians(lat0)
    lon0_r = radians(lon0)
    lats_r = np.radians(np.asarray(lats, dtype=float))
    lons_r = np.radians(np.asarray(lons, dtype=float))
    dlon = lons_r - lon0_r
    dlat = lats_r - lat0_r
    a = np.sin(dlat / 2) ** 2 + np.cos(lat0_r) * np.cos(lats_r) * np.sin(dlon / 2) ** 2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    return R_MILES * c


def calculate_and_print_distance(stops_df_list):
    """Optimized: vectorized haversine per row, set for O(1) pair lookup."""
    n = len(stops_df_list)
    lats = np.array([s[0] for s in stops_df_list], dtype=float)
    lons = np.array([s[1] for s in stops_df_list], dtype=float)
    routes = [s[-1] for s in stops_df_list]

    results = []
    success_set = set()  # O(1) lookup instead of list

    for i in range(n):
        route_i = routes[i]
        if not route_i:
            continue
        dist_i = _haversine_vector_miles(lats[i], lons[i], lats, lons)
        for j in range(n):
            if i == j:
                continue
            route_j = routes[j]
            if not route_j or route_i == route_j:
                continue
            pair_key = f'{route_i} to {route_j}'
            if pair_key in success_set:
                continue
            if dist_i[j] <= 0.25:
                success_set.add(pair_key)
                results.append(f'{route_i}>>{route_j}\n')
    return results

print("Calculating Distances...................")
print("........................................")
print("........................................")
print("........................................")
print("........................................")

file_name = f'{file_first_name}_distances_success.txt'

# Check if the file exists
if os.path.exists(file_name):
    print(f"File '{file_name}' exists. Reading results from the file...")
    
    # Read the file contents
    with open(file_name, 'r') as file:
        results = file.read()

else:
    print("stops_df_list contains:", stops_df_list)

    results = calculate_and_print_distance(stops_df_list)

    print(".....................Distance Calculated")
    # Now results is an iterator of strings. 
    # We'll join these strings together with an empty separator to get the final text.
    final_text = ''.join(results)
    print(f"Results before writing to file: {final_text}")

    # Write the distances to a text file
    with open(f'{file_first_name}_distances_success.txt', 'w') as file:
        file.write(final_text)

    # print("#####################################################################")
    print(f'File: {file_first_name}_distances_success.txt Created SuccessFully')
    # print("#####################################################################")

# Normalize results to a set for O(1) lookup in the transfer loop
if isinstance(results, str):
    good_transfer_set = set(line.strip() for line in results.split('\n') if line.strip())
else:
    good_transfer_set = set(r.strip() for r in results if r.strip())

# Good Transfer Combo Logic Starts Here
prev_trip_codes_checks=['tripfirstroutecode','tripsecondroutecode','tripthirdroutecode','tripfourthroutecode']
route_survey_checks=['routesurveyedcode']
next_trip_codes_checks=['tripnextroutecode','tripafterroutecode','trip3rdroutecode','triplast4thrtecode']
prev_trip_codes_columns=check_all_characters_present(df,prev_trip_codes_checks)
route_survey_column=check_all_characters_present(df,route_survey_checks)
next_trip_codes_columns=check_all_characters_present(df,next_trip_codes_checks)
transfer_list_columns=[*prev_trip_codes_columns,*route_survey_column,*next_trip_codes_columns]
transfer_list_columns


def process_route_surveyed_code(value):
    splited_list = str(value).split('_')  # Note the [0] to access the string in the inner list
    splited_value = '_'.join(splited_list[:-1])
    return splited_value

# To split/remove _00/_01 from RouteSurveyCode Column
route_survey_values = df[route_survey_column].values.tolist()

route_survey_splited_values=[]
for i in range(0,len(route_survey_values)):
    for value in route_survey_values[i]:
        route_survey_splited_values.append(process_route_surveyed_code(value))

# Set the values without _00/_01 to the Original Route Survey Column
df[route_survey_column[0]]=route_survey_splited_values

# create 'transfers_list' Logic Here 
good_transfer_df=df[transfer_list_columns]
good_transfer_values=good_transfer_df.values.tolist()
good_transfer_values
result_list = []
for values in good_transfer_values:
    non_na_values = [str(value) for value in values if not pd.isna(value)]
    result = '>>'.join(non_na_values)
    result_list.append(result)

df['transfers_list']=result_list

prev_trip_codes_checks=['id','tripfirstroutecode','tripsecondroutecode','tripthirdroutecode','tripfourthroutecode']
route_survey_checks=['routesurveyedcode']
next_trip_codes_checks=['tripnextroutecode','tripafterroutecode','trip3rdroutecode','triplast4thrtecode','transferslist']
# next_trip_codes_checks=['tripnextroutecode','tripafterroutecode','trip3rdroutecode','triplast4thrtecode']
final_reviewer_column_checks=['finalreviewer']
final_reviewer_column=check_all_characters_present(df,final_reviewer_column_checks)


prev_trip_codes_columns=check_all_characters_present(df,prev_trip_codes_checks)
route_survey_column=check_all_characters_present(df,route_survey_checks)
next_trip_codes_columns=check_all_characters_present(df,next_trip_codes_checks)
transfers_new_columns=[*prev_trip_codes_columns,*route_survey_column,*next_trip_codes_columns,*final_reviewer_column]
transfers_new_columns

df1=df[transfers_new_columns]

df1['Duplicate Transfers']=None

# Add Transfer1 to Transfer8 and Check1.GoodTransfer to Check8.GoodTransfer columns in DataFrame
for i in range(1, 9):
    df1[f'Transfer{i}'] = None
    df1[f'Check{i}.GoodTransfer'] = 0


def _process_transfer_row(transfers_list_str, good_transfer_set):
    """Process one row: same logic as original iterrows, returns (dup, transfers[8], checks[8])."""
    splited_list = transfers_list_str.split('>>')
    dup = 1 if any(c > 1 for c in Counter(splited_list).values()) else 0
    transfers = [None] * 8
    checks = [0] * 8
    for i in range(len(splited_list)):
        if i + 1 < len(splited_list):
            item_to_find = f"{splited_list[i]}>>{splited_list[i + 1]}"
            if '-oth-' in splited_list:
                checks[i] = 0
                transfers[i] = item_to_find
            elif item_to_find in good_transfer_set:
                checks[i] = 0
                transfers[i] = item_to_find
            else:
                checks[i] = 1
                transfers[i] = item_to_find
        else:
            checks[i] = 0
            transfers[i] = f"{splited_list[i]}"
    return dup, transfers, checks


# Good Transfer Values added based on condition (vectorized apply instead of iterrows)
rows_data = df1['transfers_list'].apply(lambda s: _process_transfer_row(s, good_transfer_set))
df1['Duplicate Transfers'] = [r[0] for r in rows_data]
for k in range(8):
    df1[f'Transfer{k + 1}'] = [r[1][k] for r in rows_data]
    df1[f'Check{k + 1}.GoodTransfer'] = [r[2][k] for r in rows_data]

df1['real flags']=df['Status']
df1['Checkall.GoodTransfer'] = np.where(df1[['Check1.GoodTransfer', 'Check2.GoodTransfer', 'Check3.GoodTransfer', 'Check4.GoodTransfer', 'Check5.GoodTransfer', 'Check6.GoodTransfer', 'Check7.GoodTransfer', 'Check8.GoodTransfer']].any(axis=1), 1, 0)
condition = ((df1['Checkall.GoodTransfer'] == 1) | (df1['real flags'] == 1))
# condition = ((df1['Checkall.GoodTransfer'] == 1))
df1 = df1[condition]


# Your agency list
agency_list = ["MDT_1_999", 'TRI_1_TR', 'PLM_1_999', 'BRI_1_B1']

# Check if any value in agency_list is present in any cell of a row and drop those rows (vectorized)
df1 = df1[~(df1.isin(agency_list)).any(axis=1)]
df1.drop_duplicates(subset='id', keep='first', inplace=True)

trad_output = f"reviewtool_{today_date}_{project_name}_TraditionalTransferFlags_auto_approved.csv"
if df1.empty:
    keep_cols = list(df1.columns) if len(df1.columns) else ["id"]
    if not keep_cols or keep_cols == []:
        keep_cols = ["id"]
    df1 = pd.DataFrame(columns=keep_cols)
    print(
        f"No traditional transfer flags for {project_name} (zero flagged records). "
        "Writing header-only CSV and continuing pipeline."
    )
df1.to_csv(trad_output, index=False)
print("#####################################################################")
print(f'File: {file_first_name}_Good_Transfer_Combo.csv Created SuccessFully')
print("#####################################################################")



