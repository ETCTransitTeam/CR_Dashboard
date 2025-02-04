import pandas as pd
import numpy as np
from datetime import date,datetime
from rapidfuzz import process, fuzz
from geopy.distance import geodesic
import warnings
import copy

warnings.filterwarnings('ignore')

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

project_name='TUCSON'

# database File
df=pd.read_csv('elvistucson2025obweekday_export_odbc.csv')
# KingElvis Dataframe
# ke_df=pd.read_excel("VTA_CA_OB_KINGElvis.xlsx",sheet_name='Elvis_Review')
# Details File Stops Sheet
detail_df=pd.read_excel('details_TUCSON_AZ_od_excel.xlsx',sheet_name='STOPS')

df=df[df['INTERV_INIT']!='999']
df=df[df['HAVE_5_MIN_FOR_SURVECode']==1]
# ke_df=ke_df[ke_df['INTERV_INIT']!='999']
# ke_df=ke_df[ke_df['INTERV_INIT']!=999]
# ke_df=ke_df[ke_df['HAVE_5_MIN_FOR_SURVECode']==1]
df=df[df['HAVE_5_MIN_FOR_SURVECode']==1]
df=df[df['INTERV_INIT']!='999']
df=df[df['INTERV_INIT']!=999]

stop_on_column_check=['stoponaddr']
stop_off_column_check=['stopoffaddr']
stop_on_id_column_check=['stoponclntid']
stop_off_id_column_check=['stopoffclntid']
stop_on_id_column=check_all_characters_present(df,stop_on_id_column_check)
stop_off_id_column=check_all_characters_present(df,stop_off_id_column_check)
stop_on_column=check_all_characters_present(df,stop_on_column_check)
stop_off_column=check_all_characters_present(df,stop_off_column_check)

stop_on_lat_lon_columns_check=['stoponlat','stoponlong']
stop_off_lat_lon_columns_check=['stopofflat','stopofflong']
stop_on_lat_lon_columns=check_all_characters_present(df,stop_on_lat_lon_columns_check)
stop_off_lat_lon_columns=check_all_characters_present(df,stop_off_lat_lon_columns_check)
stop_off_lat_lon_columns.sort()
stop_on_lat_lon_columns.sort()

origin_address_lat_column=['originaddresslat']
origin_address_long_column=['originaddresslong']
origin_address_lat=check_all_characters_present(df,origin_address_lat_column)
origin_address_long=check_all_characters_present(df,origin_address_long_column)

route_surveyed_column_check=['routesurveyedcode']
route_surveyed_column=check_all_characters_present(df,route_surveyed_column_check)



columns_to_add = ['id', *route_surveyed_column,*stop_off_column, *stop_on_column, *stop_off_id_column, *stop_on_id_column,*stop_off_lat_lon_columns,*stop_on_lat_lon_columns]

# ke_df.rename(columns={'ROUTE_SURVEYEDCode': 'ROUTE_SURVEYEDCode_KE'}, inplace=True)


# Merge without prefixes or suffixes

# ke_df = pd.merge(ke_df, df[columns_to_add], on='id', how='left')

# For VTA there are some ids which are presenr in database but not present in KINGElvis so have to merge dataframes keeping database on the left 
# ke_df = pd.merge(df[columns_to_add],ke_df, on='id', how='left')

# ke_df = ke_df.dropna(subset=[origin_address_lat[0], origin_address_long[0]])


# ke_df['ROUTE_SURVEYEDCode_SPLITED']=ke_df['ROUTE_SURVEYEDCode'].apply(lambda x : '_'.join(str(x).split('_')[0:-1]))
# ke_df[['ROUTE_SURVEYEDCode_SPLITED']]

df['ROUTE_SURVEYEDCode_SPLITED']=df['ROUTE_SURVEYEDCode'].apply(lambda x : '_'.join(str(x).split('_')[0:-1]))
df[['ROUTE_SURVEYEDCode_SPLITED']]

detail_df['ETC_ROUTE_ID_SPLITED']=detail_df['ETC_ROUTE_ID'].apply(lambda x : '_'.join(str(x).split('_')[0:-1]))
detail_df[['ETC_ROUTE_ID_SPLITED']].head(2)

# ke_df=ke_df[ke_df['id']==8492]


detail_df['ETC_STOP_DIRECTION']=detail_df['ETC_STOP_ID'].apply(lambda x : str(x).split('_')[-2])
detail_df[['ETC_STOP_DIRECTION']].head(2)

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

        
# Iterate through df rows to get the STOP_ON points
for _, row in df.iterrows():
    nearest_stop_seq = []    
    
    stop_on_id=row[stop_on_id_column[0]]    
    
    stop_on_lat = row[stop_on_lat_lon_columns[0]]
    stop_on_long = row[stop_on_lat_lon_columns[1]]
#     if pd.isna(origin_lat) or pd.isna(origin_long):
#         continue 
    
    # Filtered data if you want to change the comparison based on DIRECTION/DIRECTIONLess
    
#     filtered_df = detail_df[detail_df['ETC_ROUTE_ID_SPLITED'] == row['ROUTE_SURVEYEDCode_SPLITED']][
#         ['stop_lat6', 'stop_lon6', 'seq_fixed', 'ETC_STOP_ID','ETC_STOP_NAME']
#     ]
    filtered_df = detail_df[detail_df['ETC_ROUTE_ID'] == row['ROUTE_SURVEYEDCode']][
        ['stop_lat6', 'stop_lon6', 'seq_fixed', 'ETC_STOP_ID','ETC_STOP_NAME']
    ]
    
    # List to store distances
    distances = []
    
    # Calculate distances for all rows in filtered_df
    for _, detail_row in filtered_df.iterrows():
        stop_lat6 = detail_row['stop_lat6']
        stop_lon6 = detail_row['stop_lon6']
        
        # Compute distance
        distance = get_distance_between_coordinates(stop_on_lat, stop_on_long, stop_lat6, stop_lon6)
        
        # Skip distance if it is 0
#         if distance == 0:
#             continue
        
        distances.append((distance, detail_row['seq_fixed'], detail_row['ETC_STOP_ID'],detail_row['ETC_STOP_NAME'],detail_row['stop_lat6'],detail_row['stop_lon6']))
    
    # Find the nearest stop (minimum distance)
    if distances:
        nearest_stop = min(distances, key=lambda x: x[0])  # x[0] is the distance
        nearest_stop_seq.append(nearest_stop)
    
    # Process nearest_stop_seq as needed

    if nearest_stop_seq:
        df.loc[row.name, 'STOP_ON_ADDR_NEW'] = nearest_stop_seq[0][3]  # ETC_STOP_NAME
        df.loc[row.name, 'STOP_ON_SEQ'] = nearest_stop_seq[0][1]      # seq_fixed
        df.loc[row.name, 'STOP_ON_CLINTID_NEW'] = nearest_stop_seq[0][2]  # ETC_STOP_ID
        df.loc[row.name, 'STOP_ON_LAT_NEW'] = nearest_stop_seq[0][4]      # stop_lat6
        df.loc[row.name, 'STOP_ON_LONG_NEW'] = nearest_stop_seq[0][5]     # stop_lon6

# Iterate through df rows to get the STOP_OFF points
# Iterate through new_df rows
for _, row in df.iterrows():
    nearest_stop_seq = []
    
#     stop_on_id=row[stop_on_id_column[0]]    
    stop_off_lat = row[stop_off_lat_lon_columns[0]]
    stop_off_long = row[stop_off_lat_lon_columns[1]]

#     stop_on_lat = row['STOP_ON_LAT_NEW']
#     stop_on_long = row['STOP_ON_LONG_NEW']
    if pd.isna(stop_off_lat) or pd.isna(stop_off_long):
        continue
    stop_on_direction = str(row['STOP_ON_CLINTID_NEW']).split('_')[-2] if len(str(row['STOP_ON_CLINTID_NEW']).split('_')) >= 2 else None
    if stop_on_direction is None:
        # Skip the current iteration if the direction cannot be determined
        continue

    # Filtered data if you want to change the comparison based on DIRECTION/DIRECTIONLess
#     filtered_df = detail_df[(detail_df['ETC_ROUTE_ID_SPLITED'] == row['ROUTE_SURVEYEDCode_SPLITED'])&(detail_df['ETC_STOP_DIRECTION']==stop_on_direction)][
#         ['stop_lat6', 'stop_lon6', 'seq_fixed', 'ETC_STOP_ID','ETC_STOP_NAME']
#     ]
    filtered_df = detail_df[(detail_df['ETC_ROUTE_ID'] == row['ROUTE_SURVEYEDCode'])&(detail_df['ETC_STOP_DIRECTION']==stop_on_direction)][
        ['stop_lat6', 'stop_lon6', 'seq_fixed', 'ETC_STOP_ID','ETC_STOP_NAME']
    ]
#     filtered_df = detail_df[detail_df['ETC_ROUTE_ID'] == row['ROUTE_SURVEYEDCode']][
#         ['stop_lat6', 'stop_lon6', 'seq_fixed', 'ETC_STOP_ID','ETC_STOP_NAME']

    # List to store distances
    distances = []
    
    # Calculate distances for all rows in filtered_df
    for _, detail_row in filtered_df.iterrows():
        stop_lat6 = detail_row['stop_lat6']
        stop_lon6 = detail_row['stop_lon6']
        
        # Compute distance
        distance = get_distance_between_coordinates(stop_off_lat, stop_off_long, stop_lat6, stop_lon6)
        # Skip distance if it is 0
#         if distance == 0:
#             continue
#         if distance>0.5:
        distances.append((distance, detail_row['seq_fixed'], detail_row['ETC_STOP_ID'],detail_row['ETC_STOP_NAME'],detail_row['stop_lat6'],detail_row['stop_lon6']))
    
    # Find the nearest stop (minimum distance)
    if distances:
        nearest_stop = min(distances, key=lambda x: x[0])  # x[0] is the distance
        nearest_stop_seq.append(nearest_stop)

    # Process nearest_stop_seq as needed
    if nearest_stop_seq:
        df.loc[row.name, 'STOP_OFF_ADDRESS_NEW'] = nearest_stop_seq[0][3]  # ETC_STOP_NAME
        df.loc[row.name, 'STOP_OFF_SEQ'] = nearest_stop_seq[0][1]      # seq_fixed
        df.loc[row.name, 'STOP_OFF_CLINTID_NEW'] = nearest_stop_seq[0][2]  # ETC_STOP_ID
        df.loc[row.name, 'STOP_OFF_LAT_NEW'] = nearest_stop_seq[0][4]      # stop_lat6
        df.loc[row.name, 'STOP_OFF_LONG_NEW'] = nearest_stop_seq[0][5]     # stop_lon6

df['SEQ_DIFFERENCE']=df['STOP_OFF_SEQ']-df['STOP_ON_SEQ']

ids_list = []
for _,row in df.iterrows():
    nearest_stop_on_seq=[]
    nearest_stop_off_seq=[]
    route_code = row[route_surveyed_column[0]]
    if row['SEQ_DIFFERENCE'] < 0:
        ids_list.append(row['id'])
        stop_on_lat = row['STOP_ON_LAT_NEW']
        stop_on_long = row['STOP_ON_LONG_NEW']

        stop_off_lat = row['STOP_OFF_LAT_NEW']
        stop_off_long = row['STOP_OFF_LONG_NEW']
        
        stop_on_direction = row[ 'STOP_ON_CLINTID_NEW'].split('_')[-2]
        stop_off_direction = row[ 'STOP_OFF_CLINTID_NEW'].split('_')[-2]
        new_route_code = (
            f"{'_'.join(route_code.split('_')[:-1])}_01" 
            if route_code.split('_')[-1] == '00' 
            else f"{'_'.join(route_code.split('_')[:-1])}_00"
        )
        df.loc[row.name, 'ROUTE_SURVEYEDCode_New'] = route_code
        df.loc[row.name, 'ROUTE_SURVEYED_NEW'] = df.loc[row.name, 'ROUTE_SURVEYED']
        new_route_name_row = detail_df[detail_df['ETC_ROUTE_ID'] == new_route_code]
        if not new_route_name_row.empty:
            new_route_name = new_route_name_row['ETC_ROUTE_NAME'].iloc[0]
            
            df.loc[row.name, 'ROUTE_SURVEYEDCode_New'] = new_route_code
            df.loc[row.name, 'ROUTE_SURVEYED_NEW'] = new_route_name

            filtered_stop_on_df = detail_df[(detail_df['ETC_ROUTE_ID_SPLITED'] == row['ROUTE_SURVEYEDCode_SPLITED'])&(detail_df['ETC_STOP_DIRECTION']!=stop_on_direction)][
            ['stop_lat6', 'stop_lon6', 'seq_fixed', 'ETC_STOP_ID','ETC_STOP_NAME']
        ]
            filtered_stop_off_df = detail_df[(detail_df['ETC_ROUTE_ID_SPLITED'] == row['ROUTE_SURVEYEDCode_SPLITED'])&(detail_df['ETC_STOP_DIRECTION']!=stop_off_direction)][
            ['stop_lat6', 'stop_lon6', 'seq_fixed', 'ETC_STOP_ID','ETC_STOP_NAME']
        ]

            stop_on_distances = []

            # Calculate distances for all rows in filtered_df
            for _, detail_row in filtered_stop_on_df.iterrows():
                stop_lat6 = detail_row['stop_lat6']
                stop_lon6 = detail_row['stop_lon6']

                # Compute distance
                stop_on_distance = get_distance_between_coordinates(stop_on_lat, stop_on_long,stop_lat6, stop_lon6)

                # Skip distance if it is 0
    #             if stop_on_distance == 0:
    #                 continue

                stop_on_distances.append((stop_on_distance, detail_row['seq_fixed'], detail_row['ETC_STOP_ID'],detail_row['ETC_STOP_NAME'],detail_row['stop_lat6'],detail_row['stop_lon6']))
            # Find the nearest stop (minimum distance)
            if stop_on_distances:
                nearest_stop_on = min(stop_on_distances, key=lambda x: x[0])  # x[0] is the distance
                nearest_stop_on_seq.append(nearest_stop_on)
#             print(f"Nearest stop details for row: {nearest_stop_on_seq}")
            if nearest_stop_on_seq:
                df.loc[row.name, 'STOP_ON_ADDRESS_NEW'] = nearest_stop_on_seq[0][3]  # ETC_STOP_NAME
                df.loc[row.name, 'STOP_ON_SEQ'] = nearest_stop_on_seq[0][1]      # seq_fixed
                df.loc[row.name, 'STOP_ON_CLINTID_NEW'] = nearest_stop_on_seq[0][2]  # ETC_STOP_ID
                df.loc[row.name, 'STOP_ON_LAT_NEW'] = nearest_stop_on_seq[0][4]      # stop_lat6
                df.loc[row.name, 'STOP_ON_LONG_NEW'] = nearest_stop_on_seq[0][5]     # stop_lon6
            stop_off_distances = []

            # Calculate distances for all rows in filtered_df
            for _, detail_row in filtered_stop_off_df.iterrows():
                stop_lat6 = detail_row['stop_lat6']
                stop_lon6 = detail_row['stop_lon6']

                # Compute distance
                stop_off_distance = get_distance_between_coordinates(stop_off_lat, stop_off_long,stop_lat6, stop_lon6)

    #             Skip distance if it is 0
    #             if stop_off_distance == 0:
    #                 continue

                stop_off_distances.append((stop_off_distance, detail_row['seq_fixed'], detail_row['ETC_STOP_ID'],detail_row['ETC_STOP_NAME'],detail_row['stop_lat6'],detail_row['stop_lon6']))
            # Find the nearest stop (minimum distance)0
            
            if stop_off_distances:
                nearest_stop_off = min(stop_off_distances, key=lambda x: x[0])  # x[0] is the distance
                nearest_stop_off_seq.append(nearest_stop_off)

            if nearest_stop_off_seq:
                df.loc[row.name, 'STOP_OFF_ADDRESS_NEW'] = nearest_stop_off_seq[0][3]  # ETC_STOP_NAME
                df.loc[row.name, 'STOP_OFF_SEQ'] = nearest_stop_off_seq[0][1]      # seq_fixed
                df.loc[row.name, 'STOP_OFF_CLINTID_NEW'] = nearest_stop_off_seq[0][2]  # ETC_STOP_ID
                df.loc[row.name, 'STOP_OFF_LAT_NEW'] = nearest_stop_off_seq[0][4]      # stop_lat6
                df.loc[row.name, 'STOP_OFF_LONG_NEW'] = nearest_stop_off_seq[0][5]
    else:
        df.loc[row.name, 'ROUTE_SURVEYEDCode_New'] = route_code
        df.loc[row.name, 'ROUTE_SURVEYED_NEW'] = df.loc[row.name, 'ROUTE_SURVEYED']

with open(f'{project_name}_SEQUENCE_DIFFERENCEIDS.txt','w') as f:
    for item in ids_list:
        f.write(f"{item}\n")

df.drop(columns=['ROUTE_SURVEYEDCode_SPLITED','SEQ_DIFFERENCE'],inplace=True)
df.drop_duplicates(subset=['id'],inplace=True)
df.to_csv(f'reviewtool_{today_date}_{project_name}_ROUTE_DIRECTION_CHECk.csv',index=False)

print("FILE GENERATED SUCCESSFULLY")