import pandas as pd
import numpy as np
from geopy.distance import geodesic
from datetime import date
import warnings

warnings.filterwarnings('ignore')

# KingELvis File
project_name='LACMTA_FEEDER'
# file_name="DENVER_OB_KINGElvis.xlsx"
# file_name="BART_CA_KINGElvis.xlsx"
# file_name="ANCHORAGE_AK_KINGElvis.xlsx"
# file_name="Buffalo_NY_OB_KINGElvis.xlsx"
# file_name="BUFFALO_RAIL_KINGElvis.xlsx"
# file_name="VTA_CA_OB_KINGElvis.xlsx"
# file_name="SEATTLE_WA_KINGElvis.xlsx"
# file_name="CulverCity_CA_KINGElvis.xlsx"
# file_name="CARTA_OB_KINGElvis.xlsx"
# file_name="VTA_WEEKEND_KINGElvis.xlsx"
# file_name="MUNI_CA_KINGElvis.xlsx"
# file_name="Tucson_az_2025_KINGElvis.xlsx"
# file_name="STL_MO_2025_KINGElvis.xlsx"
# file_name="KCATA_2025_KINGElvis.xlsx"
# file_name="ACT_2025_KINGElvis.xlsx"
# file_name="SALEM_OR_2025_KINGElvis.xlsx"
file_name="LACMTA_FEEDER_2025_KINGElvis.xlsx"
ke_df=pd.read_excel(file_name, sheet_name='Elvis_Review')
today_date = date.today()
today_date=''.join(str(today_date).split('-'))



ke_df=ke_df[ke_df['INTERV_INIT']!=999]
ke_df=ke_df[ke_df['HAVE_5_MIN_FOR_SURVECode']==1]

# old_removal_filename="reviewtool_20241216_DENVER_reviewerstats.xlsx"
# old_removal_filename="reviewtool_20250217_BART_reviewerstats.xlsx"
# old_removal_filename="reviewtool_20241216_ANCHORAGE_reviewerstats.xlsx"
# old_removal_filename="reviewtool_20241216_VTA_reviewerstats.xlsx"
# old_removal_filename="reviewtool_20241216_BUFFALO_reviewerstats.xlsx"
# old_removal_filename="reviewtool_20241216_SEATTLE_reviewerstats.xlsx"
# old_removal_filename="reviewtool_20241216_CULVER_CITY_reviewerstats.xlsx"
# old_removal_filename="reviewtool_20241216_CARTA_SC_reviewerstats.xlsx"
# old_removal_filename="reviewtool_20241216_MUNI_reviewerstats.xlsx"
# old_removal_filename="reviewtool_20250428_TUCSON_reviewerstats.xlsx"
# old_removal_filename="reviewtool_20250903_KCATA_reviewerstats.xlsx"
# old_removal_filename="reviewtool_20251230_ACT_reviewerstats.xlsx"        #ACT
# old_removal_filename="reviewtool_20251223_SALEM_reviewerstats.xlsx"        #Salem
old_removal_filename="reviewtool_20260310_LACMTA_FEEDER_reviewerstats.xlsx"        #LACMTA

# Elvis DataBase File
# df=pd.read_csv('elvisdenverco2024obweekday_export_odbc.csv')
# df=pd.read_csv('elvisbartca2024interceptNEW_main_export_odbc.csv')
# df=pd.read_csv('elvisanchorageak2024obweekday_export_odbc.csv')
# df=pd.read_csv('elvisbuffalony2024obweekday_export_odbc.csv')
# df=pd.read_csv('elvisvtaca2024obweekday_export_odbc.csv')
# df=pd.read_csv('elvisseattlewa2024obweekday_export_odbc.csv')
# df=pd.read_csv('elvisculver_city_ca_2024_obweekday_export_odbc.csv')
# df=pd.read_csv('elvischarleston_sc_obweekday_export_odbc.csv')
# df=pd.read_csv('elvisbuffalony2024obweekday_export_odbc.csv')
# df=pd.read_csv('elvisvtaca2024weekend_export_odbc.csv')
# df=pd.read_csv('elvismunica2024obweekday_export_odbc.csv')
# df=pd.read_csv('elvistucson2025obweekday_export_odbc.csv')
# df=pd.read_csv('elvisstlouis2025obweekday_export_odbc.csv')
# elvis_df=pd.read_csv('elvis_transit_ls6_224559_export_odbc.csv')
# elvis_df=pd.read_csv('elvis_transit_ls6_348879_export_odbc.csv')      #ACT
# elvis_df=pd.read_csv('elvis_transit_ls6_179191_export_odbc.csv')           #Salem
elvis_df=pd.read_csv('elvis_transit_ls6_733524_export_odbc.csv')           #LACMTA


# Main/Non-Elvis Database File
# main_df=pd.read_csv('denverco2024obweekday_export_odbc.csv')
# main_df=pd.read_csv('bartca2024interceptNEW_main_export_odbc.csv')
# main_df=pd.read_csv('anchorageak2024obweekday_export_odbc.csv')
# # main_df=pd.read_csv('buffalony2024obweekday_export_odbc.csv')
# main_df=pd.read_csv('vtaca2024obweekday_export_odbc.csv')
# main_df=pd.read_csv('seattlewa2024obweekday_export_odbc.csv')
# main_df=pd.read_csv('culver_city_ca_2024_obweekday_export_odbc.csv')
# main_df=pd.read_csv('charleston_sc_obweekday_export_odbc.csv')
# main_df=pd.read_csv('buffalonyrail2024obweekday_export_odbc.csv')
# main_df=pd.read_csv('vtaca2024weekend_export_odbc.csv')
# main_df=pd.read_csv('munica2024obweekday_export_odbc.csv')
# main_df=pd.read_csv('tucson2025obweekday_export_odbc.csv')
# main_df=pd.read_csv('stlouis2025obweekday_export_odbc.csv')
# baby_df=pd.read_csv('transit_ls6_224559_export_odbc.csv')
# baby_df=pd.read_csv('transit_ls6_348879_export_odbc.csv')       #ACT
# baby_df=pd.read_csv('transit_ls6_179191_export_odbc.csv')          #Salem
baby_df=pd.read_csv('transit_ls6_733524_export_odbc.csv')          #LACMTA

# recovery_sheet_df=pd.read_excel('UTA_survey_recovery_2024-03-21.xlsx')

mapping_file = "request_20250708_ls6tols2-headers.xlsx"
sheet_name = "Example"

header_df = pd.read_excel(mapping_file, sheet_name=sheet_name)

# Create a dictionary: {old_name: new_name}
header_mapping = dict(zip(header_df["Headers-ls6"], header_df["FormattedHeader-ls2"]))

# Step 2: Rename df1 columns to get df2
df = elvis_df.rename(columns=header_mapping)
main_df = baby_df.rename(columns=header_mapping)


# print("Renamed df columns:")
# print(df.columns.tolist())

# print("Renamed main_df columns:")
# print(main_df.columns.tolist())

if file_name.split('_')[0].isdigit():
    file_first_name=file_name.split('_')[0]+'_'+file_name.split('_')[1]
else:
    file_first_name=file_name.split('_')[0]

# for new generated file version
version=3


def check_all_characters_present(df, columns_to_check):
    # Function to clean a string by removing underscores and   brackets and converting to lowercase
    def clean_string(s):
        return s.replace('_', '').replace('[', '').replace(']', '').replace(' ','').replace('#','').lower()

    # Clean and convert all column names in df to lowercase for case-insensitive comparison
    df_columns_lower = [clean_string(column) for column in df.columns]

    # Clean and convert the columns_to_check list to lowercase for case-insensitive comparison 
    columns_to_check_lower = [clean_string(column) for column in columns_to_check]

    # Use a list comprehension to filter columns
    matching_columns = [column for column in df.columns if clean_string(column) in columns_to_check_lower]

    return matching_columns

# Debugging - check available columns
# print("df columns:", df.columns.tolist())
# print("ke_df columns:", ke_df.columns.tolist())
# print("First few rows of df:", df.head(2))
# print("First few rows of ke_df:", ke_df.head(2))
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
        pass


def duplicate_route_information(df,main_df,ke_df):

    # ke_df=ke_df[ke_df['INTERV_INIT']!=999]
    # ke_df=ke_df[ke_df['HAVE_5_MIN_FOR_SURVECode']==1]

    # Rename ID column in df to match
    # df = df.rename(columns={'ID': 'id'})
    df = pd.merge(df, ke_df[['id','Final_Usage']], on='id', how='left', indicator=True)
    df['Matched'] = (df['_merge'] == 'both').astype(int)
    df.drop(columns=['_merge'])
    df=df[df['Matched']==1]
    df.drop_duplicates(subset=['id'],keep='first',inplace=True)

    # getting data from non-elvis database file which matches with KingElvis 
    # df = df.rename(columns={'ID': 'id'})
    main_df=pd.merge(main_df,ke_df[['id','Final_Usage']],on='id',how='left',indicator=True)
    main_df['Matched'] = (main_df['_merge'] == 'both').astype(int)
    main_df.drop(columns=['_merge'])
    main_df=main_df[main_df['Matched']==1]
    main_df.drop_duplicates(subset=['id'],keep='first',inplace=True)

    # columns from elvis database file
    prev_trip_codes_checks=['tripfirstroutecode','tripsecondroutecode','tripthirdroutecode','tripfourthroutecode']
    route_survey_checks=['routesurveyedcode']
    next_trip_codes_checks=['tripnextroutecode','tripafterroutecode','trip3rdroutecode','triplast4thrtecode']
    prev_trip_codes_columns=check_all_characters_present(df,prev_trip_codes_checks)
    route_survey_column=check_all_characters_present(df,route_survey_checks)
    next_trip_codes_columns=check_all_characters_present(df,next_trip_codes_checks)
    transfer_list_columns=[*prev_trip_codes_columns,*route_survey_column,*next_trip_codes_columns]
    
    # columns from non-elvis database file
    main_prev_trip_codes_checks=['tripfirstroutecode','tripsecondroutecode','tripthirdroutecode','tripfourthroutecode']
    main_route_survey_checks=['routesurveyedcode']
    main_next_trip_codes_checks=['tripnextroutecode','tripafterroutecode','trip3rdroutecode','triplast4thrtecode']
    main_prev_trip_codes_columns=check_all_characters_present(main_df,main_prev_trip_codes_checks)
    main_route_survey_column=check_all_characters_present(main_df,main_route_survey_checks)
    main_next_trip_codes_columns=check_all_characters_present(main_df,main_next_trip_codes_checks)
    main_transfer_list_columns=[*main_prev_trip_codes_columns,*main_route_survey_column,*main_next_trip_codes_columns]
    # print("Columns in main_df:", list(new_df.columns))

    # Missing transfer/route columns or no overlapping ids — return an empty but schema-complete frame.
    if (
        not route_survey_column
        or not main_route_survey_column
        or df.empty
        or main_df.empty
    ):
        return pd.DataFrame(columns=['id', 'Final_Usage', 'Duplicate_Route'])
    
    new_df=pd.merge(df[['id',*transfer_list_columns,'Final_Usage']],main_df[['id',*main_transfer_list_columns,'Final_Usage']],on='id',suffixes=('_Elvis', '_Non_Elvis'))
    print("new_df_columns: ", new_df.columns)
    new_df.reset_index(inplace=True,drop=True)
    # print("Columns in new_df:", list(new_df.columns))

    if new_df.empty:
        new_df['Duplicate_Route'] = 0
        return new_df

    # Update the variables to point to the new column names
    main_route_survey_column[0] = f"{main_route_survey_column[0]}_Elvis"
    route_survey_column[0] = f"{route_survey_column[0]}_Non_Elvis"

    new_df[main_route_survey_column[0]] = new_df[main_route_survey_column[0]].apply(
    lambda x: '_'.join(str(x).split('_')[:-1]) if isinstance(x, str) else x
)
    new_df[route_survey_column[0]] = new_df[route_survey_column[0]].apply(
    lambda x: '_'.join(x.split('_')[:-1]) if isinstance(x, str) else x
)

    main_transfer_list_columns = [f"{col}_Non_Elvis" for col in main_transfer_list_columns]
    transfer_list_columns = [f"{col}_Elvis" for col in transfer_list_columns]

    for index, row in new_df.iterrows():
        main_transferlist=row[main_transfer_list_columns].values
        elvis_transferlist=row[transfer_list_columns].values
        elvis_non_nan_values = [value for value in elvis_transferlist if pd.notna(value)]   
        non_nan_values = [value for value in main_transferlist if pd.notna(value)]
        has_duplicates = len(non_nan_values) != len(set(non_nan_values))
        if has_duplicates:
            new_df.loc[index,'Duplicate_Route']=1
        else:
            new_df.loc[index,'Duplicate_Route']=0

    if 'Duplicate_Route' not in new_df.columns:
        new_df['Duplicate_Route'] = 0

    # excel_file_path = 'new_df_output.xlsx'
    # new_df.to_excel(excel_file_path, index=False)
    return new_df
    


duplicate_df=duplicate_route_information(df,main_df,ke_df)
print("===============================")
print("Duplicate ROUTE INFORMATION")
print("===============================")


def general_information_cleaning(ke_df):
    
    columns_to_check_for_post_process=['have5minforsurvecode', 'intervinit','elvisdate']

    columns=check_all_characters_present(ke_df,columns_to_check_for_post_process)
    columns.sort()


    ke_df['Final_Usage'] = ke_df['Final_Usage'].str.lower()

    # Number of how many have been downloaded  
    total_downloaded = len(ke_df)

    # Count surveys that are 5+ minutes and INTER is not 999
    # Number of how many are 5+ minutes surveys [Useable: 5+ = 1 & INTER<>999] 
    useable_data = len(ke_df[(ke_df[columns[1]] == 1)& (ke_df[columns[2]] != '999')])
 
    # Number of how many have been cleaned [Use or Remove] from Download 
    cleaned_data=len(ke_df[ke_df['Final_Usage'].isin(['use', 'remove'])])

    uncleaned_data=len(ke_df[~ke_df['Final_Usage'].isin(['use', 'remove'])])

    # Count surveys that are marked as 'Use' or 'Remove' in 'Final_Usage' from Download
    cleaned_from_download = len(ke_df[ke_df['Final_Usage'].isin(['use', 'remove'])])

    # Count surveys that are marked as 'Use' or 'Remove' in 'Final_Usage' from Download
    remove_from_download = len(ke_df[ke_df['Final_Usage'].isin(['remove'])])
 
    # Count surveys that are marked as 'Use' or 'Remove' in 'Final_Usage' from Useable
    # Number of how many have been cleaned [Use or Remove] from Useable 
    cleaned_from_useable = len(ke_df[(ke_df['Final_Usage'].isin(['use', 'remove'])) & (ke_df[columns[0]] == 1) & (ke_df[columns[1]] != '999')])

    # Number of how many are left to be cleaned [Blank instead of Use or Remove] 
    blank_from_useable_data=useable_data-cleaned_from_useable
    

    # Count surveys marked as 'Use' from Useable
    # Number of how many have been marked Use [FROM USEABLE count
    use_from_useable = len(ke_df[(ke_df['Final_Usage'] == 'use') & (ke_df[columns[0]] == 1) & (ke_df[columns[1]] != '999')])
    

    # Percentage of how many have been "Removed" from Cleaned
    removed_from_cleaned_per = (remove_from_download / cleaned_from_download) * 100 if cleaned_from_download != 0 else 0

    # Count surveys that are marked as 'Use' or 'Remove' in 'Final_Usage' from Download
    use_from_download = len(ke_df[ke_df['Final_Usage'].isin(['use'])])

    # Percentage of how many have been "Use" from Cleaned
    use_from_cleaned_per = (use_from_download / cleaned_from_download) * 100 if cleaned_from_download != 0 else 0

    # Percentage of how many have been HereAPI Approved 
    # google_approved_using_2nd_cleaner=len(ke_df[(ke_df['2nd Cleaner']=='Google Approved')|(ke_df['2nd Cleaner']=='HereAPI Approved')])
    # google_approved_per=(google_approved_using_2nd_cleaner/len(ke_df['2nd Cleaner']))*100


    # last_completed_date = ke_df['Completed'].max()

    # last_elvis_date = ke_df[columns[0]].max()

    columns = [
        "total_downloaded",
        "useable_data",
        "cleaned_from_download",
        "cleaned_from_useable",
        "blank_from_useable_data",
        "use_from_useable",
        "remove_from_download",
        "removed_from_cleaned_per",
        "use_from_download",
        "use_from_cleaned_per",
        # "google_approved_per",
        # "last_completed_date",
        # "last_elvis_date"
    ]
    # data = [total_downloaded, useable_data, cleaned_from_download, cleaned_from_useable, blank_from_useable_data, use_from_useable, remove_from_download, removed_from_cleaned_per, use_from_download, use_from_cleaned_per, google_approved_per, last_completed_date, last_elvis_date]
    data = [total_downloaded, useable_data, cleaned_from_download, cleaned_from_useable, blank_from_useable_data, use_from_useable, remove_from_download, removed_from_cleaned_per, use_from_download, use_from_cleaned_per]
    data = pd.DataFrame([data], columns=columns)
    return data


general_information=general_information_cleaning(ke_df)
print("===============================")
print("GENERAL INFORMATION")
print("===============================")

def reviewer_level_information(ke_df):
    # ke_df=ke_df[ke_df['INTERV_INIT']!=999]
    # ke_df=ke_df[ke_df['HAVE_5_MIN_FOR_SURVECode']==1]

    columns_to_check_for_review_processing=['finalreviewer','have5minforsurvecode', 'intervinit']
    columns=check_all_characters_present(ke_df,columns_to_check_for_review_processing)
    columns.sort()
    
    def unique_reviewers_values(value):
        names=[name.strip() for name in str(value).replace('.', ',').split(',')]
        return names[0] 

    ke_df[columns[0]]=ke_df[columns[0]].apply(unique_reviewers_values)
    remove_counts = ke_df[(ke_df['Final_Usage'].str.lower()== 'remove')][columns[0]].value_counts()
 

    # Step 2: Count how many times each unique reviewer appears in the 'FINAL_REVIEWER' column.
            
    unique_reviewers = ke_df[columns[0]].unique()
    value_counts = ke_df[columns[0]].value_counts()

    # Step 3: Calculate the removal rate for each reviewer.
    removal_rate = {}
    for reviewer in unique_reviewers:
        removed = remove_counts.get(reviewer, 0) # Get the removed count for the reviewer, default to 0 if not found
        total_reviews = value_counts.get(reviewer, 0)
        removal_rate[reviewer] = (removed / total_reviews) * 100 if total_reviews != 0 else 0

    # Building the data for DataFrame
    data = []
    for reviewer in unique_reviewers:
        total_reviews = value_counts.get(reviewer, 0)
        removed = remove_counts.get(reviewer, 0)
        rate = removal_rate.get(reviewer, 0)
        percentage_reviews = (total_reviews / len(ke_df)) * 100
        data.append([reviewer, total_reviews, percentage_reviews, removed, rate])

    data = pd.DataFrame(data, columns=[
        "Reviewer",
        "Total Reviewed",
        "Percentage Reviewed (%)",
        "Total Removed",
        "Removal Rate (%)"
    ])
    # data = data[~data['Reviewer'].isin(['NO 5 MIN/TEST', 'Deleted'])]

    return data
reviewer_information=reviewer_level_information(ke_df)
print("===============================")
print("REVIEWER LEVEL INFORMATION")
print("===============================")


def _clean_col_name(s: str) -> str:
    return (
        str(s)
        .replace("_", "")
        .replace("[", "")
        .replace("]", "")
        .replace(" ", "")
        .replace("#", "")
        .lower()
    )


def _ensure_ke_columns(ke_df: pd.DataFrame) -> pd.DataFrame:
    """Map elvis lat/long/transfer columns to KE_* names regardless of source casing."""
    targets = {
        "destinaddresslat": "KE_DESTIN_ADDRESS_LAT",
        "destinaddresslong": "KE_DESTIN_ADDRESS_LONG",
        "originaddresslat": "KE_ORIGIN_ADDRESS_LAT",
        "originaddresslong": "KE_ORIGIN_ADDRESS_LONG",
        "stopofflat": "KE_STOP_OFF_LAT",
        "stopofflong": "KE_STOP_OFF_LONG",
        "stoponlat": "KE_STOP_ON_LAT",
        "stoponlong": "KE_STOP_ON_LONG",
        "prevtransferscode": "KE_PREV_TRANSFERSCode",
        "nexttransferscode": "KE_NEXT_TRANSFERSCode",
    }
    mapping = {}
    for col in ke_df.columns:
        if str(col).startswith("KE_"):
            continue
        cleaned = _clean_col_name(col)
        if cleaned in targets:
            mapping[col] = targets[cleaned]
    if mapping:
        ke_df = ke_df.rename(columns=mapping)
    for target in targets.values():
        if target not in ke_df.columns:
            ke_df[target] = 0
    return ke_df


def _ensure_change_metric_columns(ke_df: pd.DataFrame) -> pd.DataFrame:
    """Guarantee downstream summary columns exist even when comparisons are skipped."""
    defaults = {
        "Origin Change": 0,
        "Destin Change": 0,
        "PrevTrans Change": 0,
        "NextTrans Change": 0,
        "Route Changes": 0,
        "Change Count": 0,
        "Origin Distance": 0.0,
        "Destin Distance": 0.0,
        "Movement_Distance": 0.0,
        "Low Count": "Yes",
    }
    for col, default in defaults.items():
        if col not in ke_df.columns:
            ke_df[col] = default
    return ke_df


def review_the_reviewer(df,ke_df,main_df):
    # ke_df=ke_df[ke_df['INTERV_INIT']!=999]
    # ke_df=ke_df[ke_df['HAVE_5_MIN_FOR_SURVECode']==1]
    # getting data from elvis database file which matches with KingElvis 
    df=pd.merge(df,ke_df[['id']],on='id',how='left',indicator=True)
    df['Matched'] = (df['_merge'] == 'both').astype(int)
    df.drop(columns=['_merge'])
    df=df[df['Matched']==1]
    df.drop_duplicates(subset=['id'],keep='first',inplace=True)

    # getting data from non-elvis database file which matches with KingElvis 
    main_df=pd.merge(main_df,ke_df[['id']],on='id',how='left',indicator=True)
    main_df['Matched'] = (main_df['_merge'] == 'both').astype(int)
    main_df.drop(columns=['_merge'])
    main_df=main_df[main_df['Matched']==1]
    main_df.drop_duplicates(subset=['id'],keep='first',inplace=True)

    origin_home_columns_check=['originaddresslat', 'originaddresslong','originplacetype','homeaddresslat','homeaddresslong']
    origin_home_columns=check_all_characters_present(df,origin_home_columns_check)
    origin_home_columns.sort()
    origin_home_columns

    destin_home_columns_check=['destinaddresslat', 'destinaddresslong','destinplacetype','homeaddresslat','homeaddresslong']
    destin_home_columns=check_all_characters_present(df,destin_home_columns_check)
    destin_home_columns.sort()
    destin_home_columns
    # if origin lat/long not present in elvis database file then add home lat/long to those values
    if len(origin_home_columns) >= 5 and len(destin_home_columns) >= 5:
        for index, row in df.iterrows():
            # if (pd.isna(row[origin_home_columns[2]]) or pd.isna(row[origin_home_columns[3]])) and 'home' in row[origin_home_columns[4]].lower():
            if (pd.isna(row[origin_home_columns[2]]) or pd.isna(row[origin_home_columns[3]])) and isinstance(row[origin_home_columns[4]], str) and 'home' in row[origin_home_columns[4]].lower():

                df.loc[index, origin_home_columns[2]] = row[origin_home_columns[0]]
                df.loc[index, origin_home_columns[3]] = row[origin_home_columns[1]]

            if (pd.isna(row[destin_home_columns[0]]) or pd.isna(row[destin_home_columns[1]])) and 'home' in str(row[destin_home_columns[2]]).lower():
                df.loc[index, destin_home_columns[0]] = row[destin_home_columns[3]]
                df.loc[index, destin_home_columns[1]] = row[destin_home_columns[4]]

    non_origin_home_columns_check=['originaddresslat', 'originaddresslong','originplacetype','homeaddresslat','homeaddresslong']
    non_origin_home_columns=check_all_characters_present(main_df,non_origin_home_columns_check)
    non_origin_home_columns.sort()
    non_origin_home_columns

    non_destin_home_columns_check=['destinaddresslat', 'destinaddresslong','destinplacetype','homeaddresslat','homeaddresslong']
    non_destin_home_columns=check_all_characters_present(main_df,non_destin_home_columns_check)
    non_destin_home_columns.sort()
    non_destin_home_columns

    # if origin lat/long not present in non elvis database file then add home lat/long to those values
    if len(non_origin_home_columns) >= 5 and len(non_destin_home_columns) >= 5:
        for index, row in main_df.iterrows():
            if (pd.isna(row[non_origin_home_columns[2]]) or pd.isna(row[non_origin_home_columns[3]])):
                home_field = row[non_origin_home_columns[4]]
                if isinstance(home_field, str) and 'home' in home_field.lower():
                    main_df.loc[index, non_origin_home_columns[2]] = row[non_origin_home_columns[0]]
                    main_df.loc[index, non_origin_home_columns[3]] = row[non_origin_home_columns[1]]

            # Handling non_destin_home_columns
            if (pd.isna(row[non_destin_home_columns[0]]) or pd.isna(row[non_destin_home_columns[1]])):
                destination_field = row[non_destin_home_columns[2]]
                if isinstance(destination_field, str) and 'home' in destination_field.lower():
                    main_df.loc[index, non_destin_home_columns[0]] = row[non_destin_home_columns[3]]
                    main_df.loc[index, non_destin_home_columns[1]] = row[non_destin_home_columns[4]]

    # get columns from elvis file
    origin_destin_prev_next_columns_check=['originaddresslat', 'originaddresslong', 'stoponlat', 'stoponlong', 'stopofflat', 'stopofflong','destinaddresslat', 'destinaddresslong','prevtransferscode','nexttransferscode']
    origin_destin_prev_next_columns=check_all_characters_present(df,origin_destin_prev_next_columns_check)
    origin_destin_prev_next_columns.sort()

    # get columns form non-elvis file
    non_origin_destin_prev_next_columns_check=['originaddresslat', 'originaddresslong', 'stoponlat', 'stoponlong', 'stopofflat', 'stopofflong','destinaddresslat', 'destinaddresslong','prevtransferscode','nexttransferscode']
    non_origin_destin_prev_next_columns=check_all_characters_present(main_df,non_origin_destin_prev_next_columns_check)
    non_origin_destin_prev_next_columns.sort()

    # add elvis database columns in KINGElvis file and rename them to KE_ColumnsName
    if origin_destin_prev_next_columns:
        ke_df = pd.merge(ke_df, df[['id', *origin_destin_prev_next_columns]], on='id', how='left')
    ke_df = _ensure_ke_columns(ke_df)

    # add non-elvis database columns in KINGElvis file
    if non_origin_destin_prev_next_columns:
        ke_df = pd.merge(ke_df, main_df[['id', *non_origin_destin_prev_next_columns]], on='id', how='left')

    non_origin_destin_columns_check = ['originaddresslat', 'originaddresslong', 'destinaddresslat', 'destinaddresslong']
    non_prev_next_columns_check = ['prevtransferscode', 'nexttransferscode']
    non_origin_destin_columns = check_all_characters_present(ke_df, non_origin_destin_columns_check)
    non_prev_next_columns = check_all_characters_present(ke_df, non_prev_next_columns_check)
    # Prefer non-KE source columns for the main-table side of the comparison.
    non_origin_destin_columns = [c for c in non_origin_destin_columns if not str(c).startswith('KE_')]
    non_prev_next_columns = [c for c in non_prev_next_columns if not str(c).startswith('KE_')]
    non_origin_destin_columns.sort()
    non_prev_next_columns.sort()

    ke_required = [
        'KE_PREV_TRANSFERSCode', 'KE_NEXT_TRANSFERSCode',
        'KE_ORIGIN_ADDRESS_LAT', 'KE_ORIGIN_ADDRESS_LONG',
        'KE_DESTIN_ADDRESS_LAT', 'KE_DESTIN_ADDRESS_LONG',
    ]
    for col in ke_required:
        if col not in ke_df.columns:
            ke_df[col] = 0

    fill_cols = [c for c in [*non_prev_next_columns, *ke_required[:2]] if c in ke_df.columns]
    if fill_cols:
        ke_df[fill_cols] = ke_df[fill_cols].fillna(0)

    fill_cols = [c for c in [*non_origin_destin_columns, *ke_required[2:]] if c in ke_df.columns]
    if fill_cols:
        ke_df[fill_cols] = ke_df[fill_cols].fillna(0)

    # Not enough comparable fields — still return a usable workbook frame.
    if len(non_origin_destin_columns) < 4 or len(non_prev_next_columns) < 2 or ke_df.empty:
        ke_df = _ensure_change_metric_columns(ke_df)
        distance_analysis = pd.DataFrame(
            columns=['<0.15mi', '0.15-0.5mi', '0.5-1mi', '>1mi', 'High_Change_Rate']
        )
        return ke_df, distance_analysis

    for index, row in ke_df.iterrows():
        num_changes = 0
        elvis_origin_lat = row['KE_ORIGIN_ADDRESS_LAT']
        elvis_origin_long = row['KE_ORIGIN_ADDRESS_LONG']
        non_elvis_origin_lat = row[non_origin_destin_columns[2]]
        non_elvis_origin_long = row[non_origin_destin_columns[3]]

        elvis_destin_lat = row['KE_DESTIN_ADDRESS_LAT']
        elvis_destin_long = row['KE_DESTIN_ADDRESS_LONG']
        non_elvis_destin_lat = row[non_origin_destin_columns[0]]
        non_elvis_destin_long = row[non_origin_destin_columns[1]]

        if all([elvis_origin_lat, elvis_origin_long, non_elvis_origin_lat, non_elvis_origin_long]):
            distance = get_distance_between_coordinates(
                elvis_origin_lat, elvis_origin_long, non_elvis_origin_lat, non_elvis_origin_long
            )
            ke_df.loc[index, 'Origin Distance'] = distance
            if distance is not None and distance > 0.15:
                ke_df.loc[index, 'Origin Change'] = 1
                num_changes += 1
            else:
                ke_df.loc[index, 'Origin Change'] = 0
        elif elvis_origin_lat and elvis_origin_long:
            ke_df.loc[index, 'Origin Distance'] = 0
            num_changes += 1
            ke_df.loc[index, 'Origin Change'] = 1
        elif non_elvis_origin_lat and non_elvis_origin_long:
            ke_df.loc[index, 'Origin Distance'] = 0
            num_changes += 1
            ke_df.loc[index, 'Origin Change'] = 1
        else:
            ke_df.loc[index, 'Origin Distance'] = 0
            ke_df.loc[index, 'Origin Change'] = 0

        if all([elvis_destin_lat, elvis_destin_long, non_elvis_destin_lat, non_elvis_destin_long]):
            distance = get_distance_between_coordinates(
                elvis_destin_lat, elvis_destin_long, non_elvis_destin_lat, non_elvis_destin_long
            )
            ke_df.loc[index, 'Destin Distance'] = distance
            if distance is not None and distance > 0.15:
                ke_df.loc[index, 'Destin Change'] = 1
                num_changes += 1
            else:
                ke_df.loc[index, 'Destin Change'] = 0
        elif elvis_destin_lat and elvis_destin_long:
            ke_df.loc[index, 'Destin Distance'] = 0
            ke_df.loc[index, 'Destin Change'] = 1
            num_changes += 1
        elif non_elvis_destin_lat and non_elvis_destin_long:
            ke_df.loc[index, 'Destin Distance'] = 0
            ke_df.loc[index, 'Destin Change'] = 1
            num_changes += 1
        else:
            ke_df.loc[index, 'Destin Distance'] = 0
            ke_df.loc[index, 'Destin Change'] = 0

        if row[non_prev_next_columns[0]] == row['KE_NEXT_TRANSFERSCode']:
            ke_df.loc[index, 'NextTrans Change'] = 0
        else:
            ke_df.loc[index, 'NextTrans Change'] = 1
            num_changes += 1

        if row[non_prev_next_columns[1]] == row['KE_PREV_TRANSFERSCode']:
            ke_df.loc[index, 'PrevTrans Change'] = 0
        else:
            ke_df.loc[index, 'PrevTrans Change'] = 1
            num_changes += 1

        ke_df.at[index, 'Change Count'] = num_changes

    # Empty ke_df never enters the loop, so ensure the column always exists.
    if 'Change Count' not in ke_df.columns:
        ke_df['Change Count'] = 0
    else:
        ke_df['Change Count'] = ke_df['Change Count'].fillna(0)

    ke_df['Low Count'] = np.where(ke_df['Change Count'] <= 1, 'Yes', 'No')


    stop_on_off_columns_check=['stoponlat', 'stoponlong', 'stopofflat', 'stopofflong']
    stop_on_off_columns=check_all_characters_present(ke_df,stop_on_off_columns_check)
    stop_on_off_columns = [c for c in stop_on_off_columns if not str(c).startswith('KE_')]
    stop_on_off_columns.sort()

    ke_stop_on_off_column_check=['kestopofflat','kestopofflong','kestoponlat','kestoponlong']
    ke_stop_on_off_column=check_all_characters_present(ke_df,ke_stop_on_off_column_check)
    ke_stop_on_off_column.sort()

    stop_fill_cols = [
        c for c in [
            *stop_on_off_columns,
            'KE_STOP_OFF_LAT', 'KE_STOP_OFF_LONG', 'KE_STOP_ON_LAT', 'KE_STOP_ON_LONG',
        ]
        if c in ke_df.columns
    ]
    if stop_fill_cols:
        ke_df[stop_fill_cols] = ke_df[stop_fill_cols].fillna(0)

    # Ensure numeric conversion before the comparison loop
    for col in stop_on_off_columns + ke_stop_on_off_column:
        if col in ke_df.columns:
            ke_df[col] = pd.to_numeric(ke_df[col], errors='coerce')

    if len(stop_on_off_columns) >= 4 and len(ke_stop_on_off_column) >= 4:
        for index, row in ke_df.iterrows():
            if (
                (round(row[stop_on_off_columns[0]], 3) == round(row[ke_stop_on_off_column[0]], 3)) and
                (round(row[stop_on_off_columns[1]], 3) == round(row[ke_stop_on_off_column[1]], 3))
            ) and (
                (round(row[stop_on_off_columns[2]], 3) == round(row[ke_stop_on_off_column[2]], 3)) and
                (round(float(row[stop_on_off_columns[3]]), 3) == round(float(row[ke_stop_on_off_column[3]]), 3))
            ):
                ke_df.loc[index, 'Route Changes'] = 0
            else:
                ke_df.loc[index, 'Route Changes'] = 1
    else:
        ke_df['Route Changes'] = 0

    # Distance pattern summary must run after the per-row loop (and when ke_df is empty).
    ke_df = _ensure_change_metric_columns(ke_df)
    ke_df['Movement_Distance'] = ke_df[['Origin Distance', 'Destin Distance']].max(axis=1)

    distance_bins = pd.cut(
        ke_df['Movement_Distance'],
        bins=[0, 0.15, 0.5, 1, np.inf],
        labels=['<0.15mi', '0.15-0.5mi', '0.5-1mi', '>1mi'],
        include_lowest=True
    )

    route_col = 'ROUTE_SURVEYEDCode' if 'ROUTE_SURVEYEDCode' in ke_df.columns else None
    if route_col is None or ke_df.empty:
        distance_analysis = pd.DataFrame(
            columns=['<0.15mi', '0.15-0.5mi', '0.5-1mi', '>1mi', 'High_Change_Rate']
        )
    else:
        distance_analysis = (
            ke_df.groupby([route_col, distance_bins], observed=False)
            .size()
            .unstack(fill_value=0)
        )
        for col in ['<0.15mi', '0.15-0.5mi', '0.5-1mi', '>1mi']:
            if col not in distance_analysis.columns:
                distance_analysis[col] = 0
        row_sums = distance_analysis.sum(axis=1).replace(0, np.nan)
        distance_analysis = distance_analysis.assign(
            High_Change_Rate=lambda x: (x['>1mi'] + x['0.5-1mi']) / row_sums
        ).fillna({'High_Change_Rate': 0})

    return ke_df, distance_analysis

ke_df, distance_analysis =review_the_reviewer(df,ke_df,main_df)
ke_df = _ensure_change_metric_columns(ke_df)

# Classify record severity
ke_df['Change_Severity'] = np.select(
    [
        (ke_df['Change Count'] == 0),
        (ke_df['Change Count'] > 0) & (ke_df['Change Count'] <= 5),
        (ke_df['Change Count'] > 5)
    ],
    ['None', 'Moderate', 'Severe'],
    default='None'  # Make sure default is also a string
)

# Calculate severity rates by route
severity_rates = (
    ke_df.groupby(['ROUTE_SURVEYEDCode', 'Change_Severity'])
    .size()
    .unstack(fill_value=0)
)
# Add missing severity categories if they don't exist
for severity in ['None', 'Moderate', 'Severe']:
    if severity not in severity_rates.columns:
        severity_rates[severity] = 0

# Now calculate the severe rate
severity_rates['Severe_Rate'] = (severity_rates['Severe'] / severity_rates.sum(axis=1)).round(3)

# Flag routes needing re-survey
resurvey_candidates = severity_rates[severity_rates['Severe_Rate'] > 0.15]

print("===============================")
print("Review The Reviewer")
print("===============================")

def duplicate_route_perecentage_df(ke_df, duplicate_df):
    ke_df = _ensure_change_metric_columns(ke_df)
    if duplicate_df is None or duplicate_df.empty:
        return pd.DataFrame([{
            'Records Reviewed': 0,
            'Transfer Updates %': 0,
            'Duplicate Routes Of Transfer Updates %': 0,
        }])
    if 'Duplicate_Route' not in duplicate_df.columns:
        duplicate_df = duplicate_df.copy()
        duplicate_df['Duplicate_Route'] = 0
    # print("duplicate_df", duplicate_df.columns)  # Check if 'Duplicate_Route' exists in duplicate_df
    merged_df=pd.merge(duplicate_df,ke_df[['id','PrevTrans Change','NextTrans Change']],on='id')
    # print("merged_df",merged_df.columns)  # Check if 'Duplicate_Route' exists in merged_df after merging
    # print(merged_df.columns)
    if merged_df.empty:
        return pd.DataFrame([{
            'Records Reviewed': 0,
            'Transfer Updates %': 0,
            'Duplicate Routes Of Transfer Updates %': 0,
        }])
    transfer_change_total=merged_df[((merged_df['PrevTrans Change']==1)|(merged_df['NextTrans Change']==1))].shape[0]
    duplicate_transfer_total=merged_df[((merged_df['PrevTrans Change']==1)|(merged_df['NextTrans Change']==1))&(merged_df['Duplicate_Route']==1)].shape[0]
    total_records=merged_df.shape[0]
    transfer_change_percentage=(transfer_change_total/total_records)*100
    if transfer_change_total != 0:
        duplicate_transfer_percentage = (duplicate_transfer_total / transfer_change_total) * 100
    else:
        duplicate_transfer_percentage = 0
    percentage_dict = {'Records Reviewed': total_records,
                   'Transfer Updates %': transfer_change_percentage,
                   'Duplicate Routes Of Transfer Updates %': duplicate_transfer_percentage}
    percentage_df = pd.DataFrame([percentage_dict])
    return percentage_df

percentage_df=duplicate_route_perecentage_df(ke_df,duplicate_df)

print("===============================")
print("Duplicate Route Percentage")
print("===============================")

def route_level_summary(ke_df):
    # ke_df=ke_df[ke_df['INTERV_INIT']!=999]
    # ke_df=ke_df[ke_df['HAVE_5_MIN_FOR_SURVECode']==1]
    summary_cols = [
        'Route', 'Total_Reviews', 'Total_Removals', 'Removal_Rate_Percentage',
        'Route_Reviewed_Percentage', 'Removed_Survey_ids',
        'Sum of Origin Change', 'Sum of Destin Change',
        'Sum of NextTrans Change', 'Sum of PrevTrans Change', 'Sum of Record Change',
        'Origin Change Percentage', 'Destin Change Percentage',
        'NextTrans Change Percentage', 'PrevTrans Change Percentage',
        'Record Change Percentage',
    ]
    summary_df=pd.DataFrame(columns=summary_cols)

    route_surveyed_column_check=['routesurveyedcode']
    route_surveyed_column=check_all_characters_present(ke_df,route_surveyed_column_check)
    if not route_surveyed_column or ke_df.empty:
        return summary_df

    def route_splited(value):
        if isinstance(value, str):  # Only apply split to string values
            return '_'.join(value.split('_')[:-1])
        else:
            return value  # Return the original value if it's not a string


    ke_df['ROUTE_SURVEYEDCode_Splited']=ke_df[route_surveyed_column[0]].apply(route_splited)

    route_surveyed_column_splited_check=['routesurveyedcodesplited']
    route_surveyed_column_splited=check_all_characters_present(ke_df,route_surveyed_column_splited_check)
    if not route_surveyed_column_splited:
        return summary_df

    route_values=ke_df[route_surveyed_column_splited[0]].unique()
    summary_df=pd.DataFrame({'Route': route_values})

    for index, row in summary_df.iterrows():
        overall_reviews=ke_df.shape[0]
        
        route_surveyed_value = row['Route']  # Assuming 'Route' is the correct column name in summary_df
        review_filter_condition = (
            (ke_df[route_surveyed_column_splited[0]] == route_surveyed_value) &
            ((ke_df['Final_Usage'].str.lower() == 'use') | (ke_df['Final_Usage'].str.lower() == 'remove'))
        )
        total_reviews = ke_df[review_filter_condition].shape[0]  # Adjust as needed
        removal_filter_condition = (
            (ke_df[route_surveyed_column_splited[0]] == route_surveyed_value) &
            (ke_df['Final_Usage'].str.lower() == 'remove')
        )
        total_removals = ke_df[removal_filter_condition].shape[0]
        removal_survey_ids_filter_condition=(
            (ke_df[route_surveyed_column_splited[0]]==route_surveyed_value)&
            (ke_df['Final_Usage'].str.lower()=='remove')
        )
        sum_of_origin_change_filter=(
                (ke_df[route_surveyed_column_splited[0]]==route_surveyed_value)&
                (ke_df['Origin Change']==1)
        )
        sum_of_destin_change_filter=(
                (ke_df[route_surveyed_column_splited[0]]==route_surveyed_value)&
                (ke_df['Destin Change']==1)
        )
        
        sum_of_prev_change_filter=(
                (ke_df[route_surveyed_column_splited[0]]==route_surveyed_value)&
                (ke_df['PrevTrans Change']==1)
        )
        
        sum_of_next_change_filter=(
                (ke_df[route_surveyed_column_splited[0]]==route_surveyed_value)&
                (ke_df['NextTrans Change']==1)
        )
        sum_of_record_change_filter=(
                (ke_df[route_surveyed_column_splited[0]]==route_surveyed_value)&
                (ke_df['Change Count']!=0)
        )
        removal_survey_ids=ke_df['id'][removal_survey_ids_filter_condition].values
        summary_df.loc[index, 'Total_Reviews'] = total_reviews
        summary_df.loc[index, 'Total_Removals'] = total_removals
        sum_origin_change=ke_df[sum_of_origin_change_filter].shape[0]
        sum_destin_change=ke_df[sum_of_destin_change_filter].shape[0]
        sum_nexttrans_change=ke_df[sum_of_next_change_filter].shape[0]
        sum_prevtrans_change=ke_df[sum_of_prev_change_filter].shape[0]
        sum_record_change=ke_df[sum_of_record_change_filter].shape[0]
        if total_reviews:
            summary_df.loc[index,'Removal_Rate_Percentage']=(total_removals*100)/total_reviews
        else:
            summary_df.loc[index,'Removal_Rate_Percentage']=0
        if overall_reviews:
            summary_df.loc[index,'Route_Reviewed_Percentage']=(total_reviews*100)/overall_reviews
        else:
            summary_df.loc[index,'Route_Reviewed_Percentage']=0
        summary_df.loc[index, 'Removed_Survey_ids'] = ', '.join(map(str, removal_survey_ids))
        summary_df.loc[index,'Sum of Origin Change']=sum_origin_change
        summary_df.loc[index,'Sum of Destin Change']=sum_destin_change
        summary_df.loc[index,'Sum of NextTrans Change']=sum_nexttrans_change
        summary_df.loc[index,'Sum of PrevTrans Change']=sum_prevtrans_change
        summary_df.loc[index,'Sum of Record Change']=sum_record_change
        if total_reviews:    
            summary_df.loc[index,'Origin Change Percentage']=f"{round((sum_origin_change*100)/total_reviews,2)}%"
            summary_df.loc[index,'Destin Change Percentage']=f"{round((sum_destin_change*100)/total_reviews,2)}%"
            summary_df.loc[index,'NextTrans Change Percentage']=f"{round((sum_nexttrans_change*100)/total_reviews,2)}%"
            summary_df.loc[index,'PrevTrans Change Percentage']=f"{round((sum_prevtrans_change*100)/total_reviews,2)}%"
            summary_df.loc[index,'Record Change Percentage']=f"{round((sum_record_change*100)/total_reviews,2)}%"
        else:
            summary_df.loc[index,'Origin Change Percentage']=0
            summary_df.loc[index,'Destin Change Percentage']=0
            summary_df.loc[index,'NextTrans Change Percentage']=0
            summary_df.loc[index,'PrevTrans Change Percentage']=0
            summary_df.loc[index,'Record Change Percentage']=0

    for col in summary_cols:
        if col not in summary_df.columns:
            if col in ('Route', 'Removed_Survey_ids'):
                summary_df[col] = ''
            else:
                summary_df[col] = 0
    return summary_df

route_summary = route_level_summary(ke_df)

print("===============================")
print("ROUTE LEVEL INFORMATION")
print("===============================")

def reviewer_level_summary(ke_df):
    reviewer_cols = [
        'Row Labels', 'Count of Elvis_ID',
        'Sum of Origin Change', 'Sum of Destin Change',
        'Sum of PrevTrans Change', 'Sum of NextTrans Change', 'Sum of Record Change',
        'Origin Change Percentage', 'Destin Change Percentage',
        'NextTrans Change Percentage', 'PrevTrans Change Percentage',
        'Record Change Percentage',
    ]
    reviewer_df=pd.DataFrame(columns=reviewer_cols)
    final_reviewer_column_check=['finalreviewer']
    final_reviewer_column=check_all_characters_present(ke_df,final_reviewer_column_check)
    if not final_reviewer_column or ke_df.empty:
        return reviewer_df

    def unique_reviewers_values(value):
        names=[name.strip() for name in str(value).replace('.', ',').split(',')]
        return names[0] 

    ke_df['FINAL_REVIEWER_Splited']=ke_df[final_reviewer_column[0]].apply(unique_reviewers_values)

    reviewer_values=ke_df['FINAL_REVIEWER_Splited'].unique()
    reviewer_df=pd.DataFrame({'Row Labels': reviewer_values})
    for index,row in reviewer_df.iterrows():
        reviewer_reviewed_value = row['Row Labels']  # Assuming 'Route' is the correct column name in summary_df
        review_filter_condition = (
            (ke_df['FINAL_REVIEWER_Splited'] == reviewer_reviewed_value) &
            ((ke_df['Final_Usage'].str.lower() == 'use') | (ke_df['Final_Usage'].str.lower() == 'remove'))
        )
        origin_change_filter_condition = (
            (ke_df['FINAL_REVIEWER_Splited'] == reviewer_reviewed_value) &
            (ke_df['Origin Change']==1)
        )
        destin_change_filter_condition = (
            (ke_df['FINAL_REVIEWER_Splited'] == reviewer_reviewed_value) &
            (ke_df['Destin Change']==1)
        )
        sum_of_prev_change_filter=(
            (ke_df['FINAL_REVIEWER_Splited'] == reviewer_reviewed_value)&
            (ke_df['PrevTrans Change']==1)
        )
        
        sum_of_next_change_filter=(
            (ke_df['FINAL_REVIEWER_Splited'] == reviewer_reviewed_value)&
            (ke_df['NextTrans Change']==1)
        )
        sum_of_record_change_filter=(
            (ke_df['FINAL_REVIEWER_Splited'] == reviewer_reviewed_value)&
            (ke_df['Change Count']!=0)
        )
        count_elvis_id = ke_df[review_filter_condition].shape[0]  
        origin_change_sum = ke_df[origin_change_filter_condition].shape[0]  
        destin_change_sum = ke_df[destin_change_filter_condition].shape[0]  
        prevtrans_change_sum = ke_df[sum_of_prev_change_filter].shape[0]  
        nexttrans_change_sum = ke_df[sum_of_next_change_filter].shape[0]  
        record_change_sum = ke_df[sum_of_record_change_filter].shape[0]  
        reviewer_df.loc[index,'Count of Elvis_ID']=count_elvis_id
        reviewer_df.loc[index,'Sum of Origin Change']=origin_change_sum
        reviewer_df.loc[index,'Sum of Destin Change']=destin_change_sum
        reviewer_df.loc[index,'Sum of PrevTrans Change']=prevtrans_change_sum
        reviewer_df.loc[index,'Sum of NextTrans Change']=nexttrans_change_sum
        reviewer_df.loc[index,'Sum of Record Change']=record_change_sum
        if count_elvis_id:
            reviewer_df.loc[index,'Origin Change Percentage']=f"{round((origin_change_sum*100)/count_elvis_id,2)}%"
            reviewer_df.loc[index,'Destin Change Percentage']=f"{round((destin_change_sum*100)/count_elvis_id,2)}%"
            reviewer_df.loc[index,'NextTrans Change Percentage']=f"{round((nexttrans_change_sum*100)/count_elvis_id,2)}%"
            reviewer_df.loc[index,'PrevTrans Change Percentage']=f"{round((prevtrans_change_sum*100)/count_elvis_id,2)}%"
            reviewer_df.loc[index,'Record Change Percentage']=f"{round((record_change_sum*100)/count_elvis_id,2)}%"
        else:
            reviewer_df.loc[index,'Origin Change Percentage']=0
            reviewer_df.loc[index,'Destin Change Percentage']=0
            reviewer_df.loc[index,'NextTrans Change Percentage']=0
            reviewer_df.loc[index,'PrevTrans Change Percentage']=0
            reviewer_df.loc[index,'Record Change Percentage']=0
    for col in reviewer_cols:
        if col not in reviewer_df.columns:
            reviewer_df[col] = '' if col == 'Row Labels' else 0
    return reviewer_df

def _normalize_record_change_percentage(series: pd.Series) -> pd.Series:
    """Coerce '12.3%' / 0 / missing values to float for threshold filters."""
    if series is None:
        return pd.Series(dtype=float)
    return (
        series.astype(str)
        .str.replace('%', '', regex=False)
        .replace({'nan': '0', 'None': '0', '': '0'})
        .astype(float)
    )

def calculate_percentage_changes(route_summary, reviewer_summary):
    # Calculate Route Record % Change over 15%
    
    # print("route_summary columns:", route_summary.columns.tolist())
    # print(route_summary.head())

    if route_summary is None or route_summary.empty:
        high_route_changes = pd.DataFrame(columns=['Route', 'Record Change Percentage'])
    else:
        if 'Record Change Percentage' not in route_summary.columns:
            route_summary = route_summary.copy()
            route_summary['Record Change Percentage'] = 0
        route_summary = route_summary.copy()
        route_summary['Record Change Percentage'] = _normalize_record_change_percentage(
            route_summary['Record Change Percentage']
        )
        high_route_changes = route_summary[
            (route_summary['Record Change Percentage'] > 15)
        ]

    if reviewer_summary is None or reviewer_summary.empty:
        high_reviewer_changes = pd.DataFrame(columns=['Row Labels', 'Record Change Percentage'])
    else:
        if 'Record Change Percentage' not in reviewer_summary.columns:
            reviewer_summary = reviewer_summary.copy()
            reviewer_summary['Record Change Percentage'] = 0
        reviewer_summary = reviewer_summary.copy()
        reviewer_summary['Record Change Percentage'] = _normalize_record_change_percentage(
            reviewer_summary['Record Change Percentage']
        )
        high_reviewer_changes = reviewer_summary[
            (reviewer_summary['Record Change Percentage'] > 15)
        ]
    
    return high_route_changes, high_reviewer_changes

reviewer_summary=reviewer_level_summary(ke_df)

high_route_changes, high_reviewer_changes = calculate_percentage_changes(route_summary, reviewer_summary)

print("\n===============================")
print("ROUTE RECORD % CHANGE OVER 15%")
print("===============================")
if not high_route_changes.empty:
    for i, row in high_route_changes.iterrows():
        print(f"Route: {row['Route']} - Record Change %: {row['Record Change Percentage']}")
else:
    print("No routes with record change percentage over 15%")

print("\n===============================")
print("REVIEWER RECORD % CHANGE OVER 15%")
print("===============================")
if not high_reviewer_changes.empty:
    for i, row in high_reviewer_changes.iterrows():
        print(f"Reviewer: {row['Row Labels']} - Record Change %: {row['Record Change Percentage']}")
else:
    print("No reviewers with record change percentage over 15%")

review_reviewer_summary=pd.concat([route_summary,reviewer_summary],axis=1)



ke_df_columns_check=['routesurveyedcodesplited','finalreviewersplited']
ke_df_columns=check_all_characters_present(ke_df,ke_df_columns_check)
if ke_df_columns:
    ke_df.drop(columns=[*ke_df_columns],inplace=True)
    # ke_df.drop(columns=['ROUTE_SURVEYEDCode_Splited','FINAL_REVIEWER_Splited'],inplace=True)

# Raw Table Sheet Data
# print("df columns:", df.columns)
raw_table = df[['ElvisStatus', 'ROUTE_SURVEYEDCode', 'id']]

# Create the Pivot Table using pandas
pivot_table = pd.pivot_table(df, values='id', index=['ElvisStatus', 'ROUTE_SURVEYEDCode'], aggfunc='count')

# Define a dataframe called id_updates with columns ID, REVIEWER, TYPE, and FINAL_USAGE
id_updates = pd.DataFrame(columns=['ID', 'REVIEWER', 'TYPE', 'FINAL_USAGE'])

# supervisor_removals
supervisor_removals = pd.DataFrame()

# ====== ADD THIS CODE BLOCK AFTER ke_df CREATION ======
# Convert dates to weekly periods
_completed_source = (
    "Completed"
    if "Completed" in ke_df.columns
    else next(
        (c for c in ("DATE_SUBMITTED", "Date_submitted", "DATE", "Elvis_Date") if c in ke_df.columns),
        None,
    )
)
if _completed_source:
    ke_df["Review_Week"] = pd.to_datetime(ke_df[_completed_source], errors="coerce").dt.to_period("W")
else:
    ke_df["Review_Week"] = pd.NaT

# Calculate weekly change rates
weekly_trends = ke_df.groupby('Review_Week').agg({
    'Change Count': 'mean',
    'id': 'count'
}).rename(columns={'id': 'Surveys_Count'})

# Identify spikes (1.5x median)
spike_threshold = weekly_trends['Change Count'].median() * 1.5
weekly_trends['Is_Spike'] = weekly_trends['Change Count'] > spike_threshold

# Sample spike report
spike_report = weekly_trends[weekly_trends['Is_Spike']].copy()
spike_report['Change_Rate'] = spike_report['Change Count'].round(2)
# ====== END OF CODE BLOCK ======

# f'{file_first_name}_multi_sheet_dataframes(v{version}).xlsx'
with pd.ExcelWriter(f'reviewtool_{today_date}_{project_name}_reviewerstats.xlsx') as writer:
    ke_df.to_excel(writer, sheet_name="Review The Reviewer", index=False)
    general_information.to_excel(writer, sheet_name="General Information", index=False)
    reviewer_information.to_excel(writer, sheet_name="Reviewer Information", index=False)

    # route_summary.to_excel(writer, sheet_name="Route Summary", index=False)

    # this is one step furhter from route summary it also calculate the % of  reviewers who have reviewed that route
    review_reviewer_summary.to_excel(writer, sheet_name="Route Summary", index=False)
    
    duplicate_df.to_excel(writer, sheet_name="Duplicate Route Summary", index=False)
    percentage_df.to_excel(writer, sheet_name="Duplicate Route Percentage", index=False)

    # recovery_sheet_df.to_excel(writer,sheet_name='Recovery Opportunities',index=False)
    raw_table.to_excel(writer, sheet_name="Raw Table", index=False)
    supervisor_removals.to_excel(writer, sheet_name="Supervisor Removals", index=False)
    id_updates.to_excel(writer, sheet_name="ID Updates", index=False)
    pivot_table.to_excel(writer, sheet_name="Route Removal Pivot - Auto")
    high_route_changes.to_excel(writer, sheet_name="High Route Changes", index=False)
    high_reviewer_changes.to_excel(writer, sheet_name="High Reviewer Changes", index=False)

    weekly_trends.to_excel(writer, sheet_name='Weekly_Trends')
    severity_rates.to_excel(writer, sheet_name='Severity_Rates')
    distance_analysis.to_excel(writer, sheet_name='Distance_Patterns')
    print("file generated")
try:
    old_general_information = pd.read_excel(old_removal_filename, sheet_name='General Information')
    old_reviewer_information = pd.read_excel(old_removal_filename, sheet_name='Reviewer Information')
    old_route_summary = pd.read_excel(old_removal_filename, sheet_name='Route Summary')
    old_supervisor_removals = pd.read_excel(old_removal_filename, sheet_name='Supervisor Removals')
    old_id_updates = pd.read_excel(old_removal_filename, sheet_name='ID Updates')


    # Calculate the percentage change in usable data
    old_usable_data = old_general_information.loc[0, 'use_from_cleaned_per']
    usable_data = general_information.loc[0, 'use_from_cleaned_per']
    usable_data_change = usable_data - old_usable_data

    # # For all floats in the dataframes, round to 2 decimal places
    usable_data = usable_data.round(2)
    old_usable_data = old_usable_data.round(2)
    # old_general mmary = old_route_summary.round(2)

    old_supervisor_removals = old_supervisor_removals.round(2)
    old_id_updates = old_id_updates.round(2)

    general_information = general_information.round(2)
    reviewer_information = reviewer_information.round(2)
    route_summary = route_summary.round(2)
    supervisor_removals = supervisor_removals.round(2)
    id_updates = id_updates.round(2)

    print("\n===============================")
    print("SUMMARY STATISTICS")
    print("===============================")

    print(f"- Usable data from cleaned data: {usable_data}%")
    print(f"- Usable data change since last week: {usable_data_change}%")


    if usable_data_change < 0:
        print(f"- Relative decrease in usable data since last week ({old_usable_data}% -> {usable_data}%)")
    # Print the percentage change in usable data
    if usable_data_change > 0 and usable_data_change < 5:
        print(f"- Slight relative increase in usable data since last week ({old_usable_data}% -> {usable_data}%)")

    elif usable_data_change >= 5 and usable_data_change < 10:
        print(f"- Moderate relative increase in usable data since last week ({old_usable_data}% -> {usable_data}%)")

    elif usable_data_change >= 10:
        print(f"- Large relative increase in usable data since last week ({old_usable_data}% -> {usable_data}%)")

except Exception as e:
    print("EXCEPTION")
    print(e)
    usable_data = general_information.loc[0, 'use_from_cleaned_per']
  

    # For all floats in the dataframes, round to 2 decimal places
    usable_data = usable_data.round(2)


    general_information = general_information.round(2)
    reviewer_information = reviewer_information.round(2)
    route_summary = route_summary.round(2)
    supervisor_removals = supervisor_removals.round(2)
    id_updates = id_updates.round(2)

print("- Reviewer Information")
high_removal_reviewers = reviewer_information[
    (reviewer_information['Removal Rate (%)'] > 10) &  
    (~reviewer_information['Reviewer'].fillna('').str.contains('Brian', case=False, na=False))
]


if len(high_removal_reviewers) < 1:
    print("\t -No high removal reviewers found.")
else:
    print("\t- High removal reviewers:")
    for i, row in high_removal_reviewers.iterrows():
        reviewer = row['Reviewer']
        removal_rate = row['Removal Rate (%)']
        total_reviews = row['Total Reviewed']
        print(f"\t\t- {reviewer}: {removal_rate}% removal ({total_reviews} records)")

print("- Route Record Percentage Change over 15%")
if not high_route_changes.empty:
    for i, row in high_route_changes.iterrows():
        print(f"Route: {row['Route']} - Record Change %: {row['Record Change Percentage']}")
else:
    print("No routes with record change percentage over 15%")

print("- Reviewer Record Percentage Change over 15%")
if not high_reviewer_changes.empty:
    for i, row in high_reviewer_changes.iterrows():
        print(f"Reviewer: {row['Row Labels']} - Record Change %: {row['Record Change Percentage']}")
else:
    print("No reviewers with record change percentage over 15%")


# Print the removal rate percentage
print("- Removal Rate Percentage over 15%")
sorted_route_summary = route_summary.sort_values(by='Removal_Rate_Percentage', ascending=False)
# sorted_route_summary_head should be equal to the rows with Removal Rate percentage > 10 
sorted_route_summary_head = sorted_route_summary[sorted_route_summary['Removal_Rate_Percentage'] > 15]  
for i, row in sorted_route_summary_head.iterrows():
    route_name = row['Route']
    removal_rate = row['Removal_Rate_Percentage']
    num_records = row['Total_Reviews']
    # print(f"- {i+1}. {route_name}: {removal_rate}% removal ({num_records} records)")
    print(f"\t- {route_name}: {removal_rate}% removal ({num_records} records)")

# Print Weekly Trends
print("\n===============================")
print("WEEKLY TRENDS")
print("===============================")
print("Average changes per week:")
print(weekly_trends[['Change Count', 'Surveys_Count']].to_string())

# Print Severity Rates
print("\n===============================")
print("SEVERITY RATES BY ROUTE")
print("===============================")
print("Total count of records by severity level:")
severity_totals = severity_rates[['None', 'Moderate', 'Severe']].sum()
print(severity_totals.to_string())
total_records = severity_totals.sum()
severity_percent = (severity_totals / total_records * 100).round(1)
print("\nPercentage breakdown:")
print(severity_percent.to_string())

# Print Problematic Distance Patterns
print("\n===============================")
print("HIGHER DISTANCE PATTERNS")
print("===============================")
print("Routes with distance change issues:")
problematic_routes = distance_analysis[distance_analysis['High_Change_Rate'] > 0]
if not problematic_routes.empty:
    print(problematic_routes[['<0.15mi', '0.15-0.5mi', '0.5-1mi', '>1mi', 'High_Change_Rate']].to_string())
else:
    print("No routes with significant distance change issues found")

# Print Spike Report
if not spike_report.empty:
    print("\n===============================")
    print("WEEKLY CHANGE RATE SPIKES")
    print("===============================")
    print("Weeks with unusually high change rates:")
    print(spike_report[['Change_Rate', 'Surveys_Count']].to_string())