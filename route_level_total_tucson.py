import pandas as pd
import numpy as np
import random
import copy
import math
from datetime import date
from datetime import datetime
import warnings

warnings.filterwarnings('ignore')

# KINGElvis FileName
file_name="VTA_CA_OB_KINGElvis.xlsx"

if file_name.split('_')[0].isdigit():
    file_first_name=file_name.split('_')[0]+'_'+file_name.split('_')[1]
else:
    file_first_name=file_name.split('_')[0]

# in some Compeletion Report LSNAMECODE is splited in some it is not so have to check that
def edit_ls_code_column(x):
    value=x.split('_')
    if len(value)>3:
        route_value="_".join(value[:-1])
    else:
        route_value="_".join(value)
    return route_value

# for generated file version
version=2
project_name='TUCSON'
today_date = date.today()
today_date=''.join(str(today_date).split('-'))

# ke_df=pd.read_excel(file_name,sheet_name='Elvis_Review')
# detail_df_stops=pd.read_excel('details_vta_CA_od_excel.xlsx',sheet_name='STOPS')
# detail_df_xfers=pd.read_excel('details_vta_CA_od_excel.xlsx',sheet_name='XFERS')

# wkend_overall_df=pd.read_excel('VTA_CA_CR.xlsx',sheet_name='WkEND-Overall')
# # wkend_overall_df['LS_NAME_CODE']=wkend_overall_df['LS_NAME_CODE'].apply(edit_ls_code_column)
# wkend_route_df=pd.read_excel('VTA_CA_CR.xlsx',sheet_name='WkEND-RouteTotal')
# df=pd.read_csv("reviewtool_20241223_VTA_ROUTE_DIRECTION_CHECk.csv")

# # if we have generated route_direction_database file using route_direction_refator_database.py file then have to replace and rename the columns
# df.drop(columns=['ROUTE_SURVEYEDCode','ROUTE_SURVEYED'],inplace=True)
# df.rename(columns={'ROUTE_SURVEYEDCode_New':'ROUTE_SURVEYEDCode','ROUTE_SURVEYED_NEW':'ROUTE_SURVEYED'},inplace=True) 

# wkday_overall_df=pd.read_excel('VTA_CA_CR.xlsx',sheet_name='WkDAY-Overall')
# # wkday_overall_df['LS_NAME_CODE']=wkday_overall_df['LS_NAME_CODE'].apply(edit_ls_code_column)
# wkday_route_df=pd.read_excel('VTA_CA_CR.xlsx',sheet_name='WkDAY-RouteTotal')


detail_df_stops=pd.read_excel('details_TUCSON_AZ_od_excel.xlsx',sheet_name='STOPS')
detail_df_xfers=pd.read_excel('details_TUCSON_AZ_od_excel.xlsx',sheet_name='XFERS')

wkend_overall_df=pd.read_excel('TUCSON_AZ_CR.xlsx',sheet_name='WkEND-Overall')
# wkend_overall_df['LS_NAME_CODE']=wkend_overall_df['LS_NAME_CODE'].apply(edit_ls_code_column)
wkend_route_df=pd.read_excel('TUCSON_AZ_CR.xlsx',sheet_name='WkEND-RouteTotal')
df=pd.read_csv("reviewtool_20250127_TUCSON_ROUTE_DIRECTION_CHECk.csv")

# if we have generated route_direction_database file using route_direction_refator_database.py file then have to replace and rename the columns
df.drop(columns=['ROUTE_SURVEYEDCode','ROUTE_SURVEYED'],inplace=True)
df.rename(columns={'ROUTE_SURVEYEDCode_New':'ROUTE_SURVEYEDCode','ROUTE_SURVEYED_NEW':'ROUTE_SURVEYED'},inplace=True) 

wkday_overall_df=pd.read_excel('TUCSON_AZ_CR.xlsx',sheet_name='WkDAY-Overall')
# wkday_overall_df['LS_NAME_CODE']=wkday_overall_df['LS_NAME_CODE'].apply(edit_ls_code_column)
wkday_route_df=pd.read_excel('TUCSON_AZ_CR.xlsx',sheet_name='WkDAY-RouteTotal')

df['ROUTE_SURVEYEDCode_Splited']=df['ROUTE_SURVEYEDCode'].apply(lambda x:('_').join(str(x).split('_')[:-1]) )


# ke_df=ke_df[ke_df['INTERV_INIT']!='999']
# ke_df=ke_df[ke_df['INTERV_INIT']!=999]
# ke_df=ke_df[ke_df['1st Cleaner']!='No 5 MIN']
# ke_df=ke_df[ke_df['1st Cleaner']!='Test']
# ke_df=ke_df[ke_df['1st Cleaner']!='Test/No 5 MIN']
# ke_df=ke_df[ke_df['Final_Usage'].str.lower()=='use']



# Getting Data from Database where the Final Usage is Use in KINGELVIS  
# df=pd.merge(df,ke_df['id'],on='id',how='inner')
df=df[df['INTERV_INIT']!='999']
df=df[df['INTERV_INIT']!=999]
df.drop_duplicates(subset='id',inplace=True)


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

date_columns_check=['completed','datestarted']
date_columns=check_all_characters_present(df,date_columns_check)

def determine_date(row):
    if not pd.isnull(row[date_columns[0]]):
        return row[date_columns[0]]
    elif not pd.isnull(row[date_columns[1]]):
        return row[date_columns[1]]
    else:
        return pd.NaT

df['Date'] = df.apply(determine_date, axis=1)

# def get_day_name(x):
#     date_object = datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
#     day_name = date_object.strftime('%A')
#     return day_name

# def get_day_name(x):
#     # Adjust the format to match your date string
#     date_object = datetime.strptime(x, '%d/%m/%Y %H:%M')
#     day_name = date_object.strftime('%A')
#     return day_name

def get_day_name(x):
    formats_to_check = ['%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M']
    
    for format_str in formats_to_check:
        try:
            date_object = datetime.strptime(x, format_str)
            day_name = date_object.strftime('%A')
            return day_name
        except ValueError:
            continue

df['Day']=df['Date'].apply(get_day_name)


# df['LAST_SURVEY_DATE'] = pd.to_datetime(df['Date'], format='%d/%m/%Y %H:%M')
# latest_date = df['LAST_SURVEY_DATE'].max()
# latest_date_df = pd.DataFrame({'Latest_Survey_Date': [latest_date]})

df['LAST_SURVEY_DATE'] = pd.to_datetime(df['Date'], format='%Y-%m-%d %H:%M:%S')
latest_date = df['LAST_SURVEY_DATE'].max()
latest_date_df = pd.DataFrame({'Latest_Survey_Date': [latest_date]})


weekend_df=df[df['Day'].isin(['Saturday','Sunday'])]

weekday_df=df[~(df['Day'].isin(['Saturday','Sunday']))]


# df.to_csv('Day Time SantaClarita.csv',index=False)

# exit()
#to get the TIMEON column
time_column_check=['timeoncode']
time_period_column_check=['timeon']
time_column=check_all_characters_present(df,time_column_check)
time_period_column=check_all_characters_present(df,time_period_column_check)
route_survey_column_check=['routesurveyedcode']
route_survey_column=check_all_characters_present(df,route_survey_column_check)
stopon_clntid_column_check=['stoponclntid']
stopon_clntid_column=check_all_characters_present(df,stopon_clntid_column_check)
stopoff_clntid_column_check=['stopoffclntid']
stopoff_clntid_column=check_all_characters_present(df,stopoff_clntid_column_check)


df[['id','Day',route_survey_column[0]]].to_csv('Checking Day Names.csv',index=False)


#values to compare AM, MIDDAY, PM and Evening values
# am_values=['AM1','AM2','AM3','MID1','MID2']
# am_values=[1,2,3,4,5]
# midday_values=['MID3','MID4','MID5','MID6','MID7','PM1']
# midday_values=[6,7,8,9,10,11]
# pm_values=['PM2','PM3','PM4','PM5']
# pm_values=[12,13,14]
# evening_values=['PM6','PM7','PM8','PM9']
# evening_values=[15,16,17,18]

wkend_overall_df.dropna(subset=['LS_NAME_CODE'],inplace=True)
wkday_overall_df.dropna(subset=['LS_NAME_CODE'],inplace=True)

# time_mapping = {
#     'AM1': 'Before 5:00 am',
#     'AM2': '5:00 am - 6:00 am',
#     'AM3': '6:00 am - 7:00 am',
#     'MID1': '7:00 am - 8:00 am',
#     'MID2': '8:00 am - 9:00 am',
#     'MID7': '9:00 am - 10:00 am',
#     'MID3': '10:00 am - 11:00 am',
#     'MID4': '11:00 am - 12:00 pm',
#     'MID5': '12:00 pm - 1:00 pm',
#     'MID6': '1:00 pm - 2:00 pm',
#     'PM1': '2:00 pm - 3:00 pm',
#     'PM2': '3:00 pm - 4:00 pm',
#     'PM3': '4:00 pm - 5:00 pm',
#     'PM4': '5:00 pm - 6:00 pm',
#     'PM5': '6:00 pm - 7:00 pm',
#     'PM6': '7:00 pm - 8:00 pm',
#     'PM7': '8:00 pm - 9:00 pm',
#     'PM8': '9:00 pm - 10:00 pm',
#     'PM9': 'After 10:00 pm'
# }
def create_time_value_df_with_display(df):
    """
    Create a time-value DataFrame summarizing counts and time ranges.

    Parameters:
        df (pd.DataFrame): Input DataFrame containing the time values.
        time_column (str): Name of the column in the input DataFrame containing the time values.

    Returns:
        pd.DataFrame: Processed DataFrame with counts, time ranges, and display text.
    """
    # Define time value groups
    # pre_early_am_values = ['AM1']
    # early_am_values = ['AM2']
    # am_values = ['AM3', 'AM4', 'MID1', 'MID2', 'MID7']
    # midday_values = ['MID3', 'MID4', 'MID5', 'MID6', 'PM1']
    # pm_values = ['PM2', 'PM3', 'PM4', 'PM5']
    # evening_values = ['PM6', 'PM7', 'PM8', 'PM9']

    # For TUCSIN PROJECT HAVE TO CHANGNE TIME-PERIODS values
    # pre_early_am_values = ['AM1']
    # early_am_values = ['AM2']
    am_values = ['AM1','AM2','AM3', 'AM4']
    midday_values = [ 'MID1', 'MID2','MID3', 'MID4', 'MID5', 'MID6' ]
    pm_values = ['PM1','PM2', 'PM3']
    evening_values = ['OFF1', 'OFF2', 'OFF3', 'OFF4','OFF5']

    # Mapping time groups to corresponding columns
    # time_group_mapping = {
    #     0: pre_early_am_values,
    #     1: early_am_values,
    #     2: am_values,
    #     3: midday_values,
    #     4: pm_values,
    #     5: evening_values,
    # }
    # for TUCSON have to change time groups too
    time_group_mapping = {
        1: am_values,
        2: midday_values,
        3: pm_values,
        4: evening_values,
    }

    # Mapping time values to time ranges
    # time_mapping = {
    #     'AM1': 'Before 5:00 am',
    #     'AM2': '5:00 am - 6:00 am',
    #     'AM3': '6:00 am - 7:00 am',
    #     'MID1': '7:00 am - 8:00 am',
    #     'MID2': '8:00 am - 9:00 am',
    #     'MID7': '9:00 am - 10:00 am',
    #     'MID3': '10:00 am - 11:00 am',
    #     'MID4': '11:00 am - 12:00 pm',
    #     'MID5': '12:00 pm - 1:00 pm',
    #     'MID6': '1:00 pm - 2:00 pm',
    #     'PM1': '2:00 pm - 3:00 pm',
    #     'PM2': '3:00 pm - 4:00 pm',
    #     'PM3': '4:00 pm - 5:00 pm',
    #     'PM4': '5:00 pm - 6:00 pm',
    #     'PM5': '6:00 pm - 7:00 pm',
    #     'PM6': '7:00 pm - 8:00 pm',
    #     'PM7': '8:00 pm - 9:00 pm',
    #     'PM8': '9:00 pm - 10:00 pm',
    #     'PM9': 'After 10:00 pm'
    # }

    time_mapping = {
        'AM1': 'Before 5:30 am',
        'AM2': '5:30 am - 6:30 am',
        'AM3': '6:30 am - 7:30 am',
        'AM4': '7:30 am - 8:30 am',
        'MID1': '8:30 am - 9:30 am',
        'MID2': '9:30 am - 10:30 am',
        'MID3': '10:30 am - 11:30 am',
        'MID4': '11:30 am - 12:30 pm',
        'MID5': '12:30 pm - 1:30 pm',
        'MID6': '1:30 pm - 2:30 pm',
        'PM1': '2:30 pm - 3:30 pm',
        'PM2': '3:30 pm - 4:30 pm',
        'PM3': '4:30 pm - 5:30 pm',
        'OFF1': '5:30 pm - 6:30 pm',
        'OFF2': '6:30 pm - 7:30 pm',
        'OFF3': '7:30 pm - 8:30 pm',
        'OFF4': '8:30 pm - 9:30 pm',
        'OFF5': 'After 9:30 pm'
    }

    # Initialize the new DataFrame
    new_df = pd.DataFrame(columns=["Original Text",  1, 2, 3, 4])

    # Populate the DataFrame with counts
    for col, values in time_group_mapping.items():
        for value in values:
            count = df[df[time_column[0]] == value].shape[0]
            row = {"Original Text": value}

            # Initialize all columns to 0
            for c in range(6):
                row[c] = 0

            # Update the corresponding column with the count
            row[col] = count
            new_df = pd.concat([new_df, pd.DataFrame([row])], ignore_index=True)

    # Map time values to time ranges
    new_df['Time Range'] = new_df['Original Text'].map(time_mapping)

    # Drop rows with missing time ranges
    new_df.dropna(subset=['Time Range'], inplace=True)

    # Add a display text column with sequential numbering
    new_df['Display_Text'] = range(1, len(new_df) + 1)

    return new_df

wkend_time_value_df=create_time_value_df_with_display(weekend_df)
wkday_time_value_df=create_time_value_df_with_display(weekday_df)

# To create Route_SurveyedCode Direction wise comparison in terms of time values
def create_route_direction_level_df(overalldf,df):
    #For Project other than TUCSON HAVE to change/uncomment the code 

    # pre_early_am_values=['AM1'] 
    # early_am_values=['AM2'] 
    # am_values=['AM3','AM4','MID1','MID2','MID7'] 
    # midday_values=['MID3','MID4','MID5','MID6','PM1']
    # pm_values=['PM2','PM3','PM4','PM5']
    # evening_values=['PM6','PM7','PM8','PM9']
    # pre_early_am_column=[0]  #0 is for Pre-Early AM header
    # early_am_column=[1]  #1 is for Early AM header
    # am_column=[2] #This is for AM header
    # midday_colum=[3] #this is for MIDDAY header
    # pm_column=[4] #this is for PM header
    # evening_column=[5] #this is for EVENING header

    # For Tucson PROJECT Have to change values TIME PERIOD VALUES

    am_values = ['AM1','AM2','AM3', 'AM4']
    midday_values = [ 'MID1', 'MID2','MID3', 'MID4', 'MID5', 'MID6' ]
    pm_values = ['PM1','PM2', 'PM3']
    evening_values = ['OFF1', 'OFF2', 'OFF3', 'OFF4','OFF5']
    am_column=[1] #This is for AM header
    midday_colum=[2] #this is for MIDDAY header
    pm_column=[3] #this is for PM header
    evening_column=[4] #this is for EVENING header

    def convert_string_to_integer(x):
        try:
            return float(x)
        except (ValueError, TypeError):
            return 0
        
    # Creating new dataframe for specifically AM, PM, MIDDAY, Evenving data and added values from Compeletion Report

    # For TUCSON PROJECT we are not using pre_early_am and early_am columns so have to comment the following code accordingly 
    new_df=pd.DataFrame()
    new_df['ROUTE_SURVEYEDCode']=overalldf['LS_NAME_CODE']
    # new_df['CR_PRE_Early_AM']=overalldf[pre_early_am_column[0]].apply(math.ceil)
    # new_df['CR_Early_AM']=overalldf[early_am_column[0]].apply(math.ceil)
    new_df['CR_AM_Peak']=overalldf[am_column[0]].apply(math.ceil)
    new_df['CR_Midday']=overalldf[midday_colum[0]].apply(math.ceil)
    new_df['CR_PM_Peak']=overalldf[pm_column[0]].apply(math.ceil)
    new_df['CR_Evening']=overalldf[evening_column[0]].apply(math.ceil)
    # print("new_df_columns",new_df.columns)
    # new_df[['CR_PRE_Early_AM','CR_Early_AM','CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']]=new_df[['CR_PRE_Early_AM','CR_Early_AM','CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']].applymap(convert_string_to_integer)
    new_df[['CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']]=new_df[['CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']].applymap(convert_string_to_integer)
    new_df.fillna(0,inplace=True)

    for index, row in new_df.iterrows():
        route_code = row['ROUTE_SURVEYEDCode']

        def get_counts_and_ids(time_values):
            # Just for SALEM
            # subset_df = df[(df['ROUTE_SURVEYEDCode_Splited'] == route_code) & (df[time_column[0]].isin(time_values))]
            subset_df = df[(df['ROUTE_SURVEYEDCode'] == route_code) & (df[time_column[0]].isin(time_values))]
            subset_df=subset_df.drop_duplicates(subset='id')
            count = subset_df.shape[0]
            ids = subset_df['id'].values
            return count, ids

        # pre_early_am_value, pre_early_am_value_ids = get_counts_and_ids(pre_early_am_values)
        # early_am_value, early_am_value_ids = get_counts_and_ids(early_am_values)
        am_value, am_value_ids = get_counts_and_ids(am_values)
        midday_value, midday_value_ids = get_counts_and_ids(midday_values)
        pm_value, pm_value_ids = get_counts_and_ids(pm_values)
        evening_value, evening_value_ids = get_counts_and_ids(evening_values)
        
        # new_df.loc[index, 'CR_Total'] = row['CR_PRE_Early_AM']+row['CR_Early_AM']+row['CR_AM_Peak'] + row['CR_Midday'] + row['CR_PM_Peak'] + row['CR_Evening']
        new_df.loc[index, 'CR_Total'] = row['CR_AM_Peak'] + row['CR_Midday'] + row['CR_PM_Peak'] + row['CR_Evening']
        new_df.loc[index, 'CR_AM_Peak'] =row['CR_AM_Peak']
        # new_df.loc[index, 'CR_AM_Peak'] =row['CR_PRE_EARLY_AM']+row['CR_EARLY_AM']+ row['CR_AM_Peak']

        # new_df.loc[index, 'DB_PRE_Early_AM_Peak'] = pre_early_am_value
        # new_df.loc[index, 'DB_Early_AM_Peak'] = early_am_value
        new_df.loc[index, 'DB_AM_Peak'] = am_value
        new_df.loc[index, 'DB_Midday'] = midday_value
        new_df.loc[index, 'DB_PM_Peak'] = pm_value
        new_df.loc[index, 'DB_Evening'] = evening_value
        # new_df.loc[index, 'DB_Total'] = evening_value + am_value + midday_value + pm_value+pre_early_am_value+early_am_value
        new_df.loc[index, 'DB_Total'] = evening_value + am_value + midday_value + pm_value
        
    #     new_df['ROUTE_SURVEYEDCode_Splited']=new_df['ROUTE_SURVEYEDCode'].apply(lambda x:('_').join(x.split('_')[:-1]) )
        route_code_level_df=pd.DataFrame()

        unique_routes=new_df['ROUTE_SURVEYEDCode'].unique()

        route_code_level_df['ROUTE_SURVEYEDCode']=unique_routes

        # weekend_df.rename(columns={'ROUTE_TOTAL':'CR_Overall_Goal','SURVEY_ROUTE_CODE':'ROUTE_SURVEYEDCode','LS_NAME_CODE':'ROUTE_SURVEYEDCode'},inplace=True)

        for index, row in new_df.iterrows():
            # pre_early_am_peak_diff=row['CR_PRE_Early_AM']-row['DB_PRE_Early_AM_Peak']
            # early_am_peak_diff=row['CR_Early_AM']-row['DB_Early_AM_Peak']
            am_peak_diff=row['CR_AM_Peak']-row['DB_AM_Peak']
            midday_diff=row['CR_Midday']-row['DB_Midday']    
            pm_peak_diff=row['CR_PM_Peak']-row['DB_PM_Peak']
            evening_diff=row['CR_Evening']-row['DB_Evening']
            total_diff=row['CR_Total']-row['DB_Total']
    #         overall_difference=row['CR_Overall_Goal']-row['DB_Total']
            # new_df.loc[index, 'PRE_Early_AM_DIFFERENCE'] = math.ceil(max(0, pre_early_am_peak_diff))
            # new_df.loc[index, 'Early_AM_DIFFERENCE'] = math.ceil(max(0, early_am_peak_diff))
            new_df.loc[index, 'AM_DIFFERENCE'] = math.ceil(max(0, am_peak_diff))
            new_df.loc[index, 'Midday_DIFFERENCE'] = math.ceil(max(0, midday_diff))
            new_df.loc[index, 'PM_DIFFERENCE'] = math.ceil(max(0, pm_peak_diff))
            new_df.loc[index, 'Evening_DIFFERENCE'] = math.ceil(max(0, evening_diff))
            # route_level_df.loc[index, 'Total_DIFFERENCE'] = math.ceil(max(0, total_diff))
            # new_df.loc[index, 'Total_DIFFERENCE'] =math.ceil(max(0, pre_early_am_peak_diff))+math.ceil(max(0, early_am_peak_diff))+math.ceil(max(0, am_peak_diff))+math.ceil(max(0, midday_diff))+math.ceil(max(0, pm_peak_diff))+math.ceil(max(0, evening_diff))
            new_df.loc[index, 'Total_DIFFERENCE'] =math.ceil(max(0, am_peak_diff))+math.ceil(max(0, midday_diff))+math.ceil(max(0, pm_peak_diff))+math.ceil(max(0, evening_diff))

    return new_df

wkend_route_direction_df=create_route_direction_level_df(wkend_overall_df,weekend_df)
wkday_route_direction_df=create_route_direction_level_df(wkday_overall_df,weekday_df)


def create_route_level_df(overall_df,route_df,df):
    # For EMBARK
    # am_values=['AM1','AM2','AM3','MID1','MID2','MID7'] 
    # midday_values=['MID3','MID4','MID5','MID6','PM1','PM2']
    # pm_values=['PM3','PM4','PM5']
    # evening_values=['PM9','PM6','PM7','PM8']
    # for SEATTLE
    # pre_early_am_values=['AM1'] 
    # early_am_values=['AM2'] 
    # am_values=['AM3','AM4','MID1','MID2','MID7'] 
    # midday_values=['MID3','MID4','MID5','MID6','PM1']
    # pm_values=['PM2','PM3','PM4','PM5']
    # evening_values=['PM6','PM7','PM8','PM9']

    # pre_early_am_column=[0]  #0 is for Pre-Early AM header
    # early_am_column=[1]  #1 is for Early AM header
    # am_column=[2] #This is for AM header
    # midday_colum=[3] #this is for MIDDAY header
    # pm_column=[4] #this is for PM header
    # evening_column=[5] #this is for EVENING header

    am_values = ['AM1','AM2','AM3', 'AM4']
    midday_values = [ 'MID1', 'MID2','MID3', 'MID4', 'MID5', 'MID6' ]
    pm_values = ['PM1','PM2', 'PM3']
    evening_values = ['OFF1', 'OFF2', 'OFF3', 'OFF4','OFF5']
    am_column=[1] #This is for AM header
    midday_colum=[2] #this is for MIDDAY header
    pm_column=[3] #this is for PM header
    evening_column=[4] #this is for EVENING header

    def convert_string_to_integer(x):
        try:
            return float(x)
        except (ValueError, TypeError):
            return 0

    # Creating new dataframe for specifically AM, PM, MIDDAY, Evenving data and added values from Compeletion Report
    new_df=pd.DataFrame()
    new_df['ROUTE_SURVEYEDCode']=overall_df['LS_NAME_CODE']
    # new_df['CR_PRE_Early_AM']=overall_df[pre_early_am_column[0]].apply(math.ceil)
    # new_df['CR_Early_AM']=overall_df[early_am_column[0]].apply(math.ceil)
    new_df['CR_AM_Peak']=overall_df[am_column[0]].apply(math.ceil)
    new_df['CR_Midday']=overall_df[midday_colum[0]].apply(math.ceil)
    new_df['CR_PM_Peak']=overall_df[pm_column[0]].apply(math.ceil)
    new_df['CR_Evening']=overall_df[evening_column[0]].apply(math.ceil)
    print("new_df_columns",new_df.columns)
    # new_df[['CR_PRE_Early_AM','CR_Early_AM','CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']]=new_df[['CR_PRE_Early_AM','CR_Early_AM','CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']].applymap(convert_string_to_integer)
    new_df[['CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']]=new_df[['CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']].applymap(convert_string_to_integer)
    #  new_df[['CR_EARLY_AM','CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']]=new_df[['CR_EARLY_AM','CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']].applymap(convert_string_to_integer)
    # new_df['Overall Goal']=cr_df[overall_goal_column[0]]
    new_df.fillna(0,inplace=True)
    # adding values for AM, PM, MIDDAY and Evening from Database file to new Dataframe
    for index, row in new_df.iterrows():
        route_code = row['ROUTE_SURVEYEDCode']

        # Define a function to get the counts and IDs
        def get_counts_and_ids(time_values):
            # Just for SALEM
            # subset_df = df[(df['ROUTE_SURVEYEDCode_Splited'] == route_code) & (df[time_column[0]].isin(time_values))]
            subset_df = df[(df['ROUTE_SURVEYEDCode'] == route_code) & (df[time_column[0]].isin(time_values))]
            subset_df=subset_df.drop_duplicates(subset='id')
            count = subset_df.shape[0]
            ids = subset_df['id'].values
            return count, ids
        
        # Calculate counts and IDs for each time slot
        # pre_early_am_value, pre_early_am_value_ids = get_counts_and_ids(pre_early_am_values)
        # early_am_value, early_am_value_ids = get_counts_and_ids(early_am_values)
        am_value, am_value_ids = get_counts_and_ids(am_values)
        midday_value, midday_value_ids = get_counts_and_ids(midday_values)
        pm_value, pm_value_ids = get_counts_and_ids(pm_values)
        evening_value, evening_value_ids = get_counts_and_ids(evening_values)
        
        # Assign values to new_df
        # new_df.loc[index, 'CR_Total'] = row['CR_EARLY_AM'] + row['CR_AM_Peak'] + row['CR_Midday'] + row['CR_PM_Peak'] + row['CR_Evening']
        # new_df.loc[index, 'CR_Total'] = row['CR_PRE_Early_AM']+row['CR_Early_AM']+row['CR_AM_Peak'] + row['CR_Midday'] + row['CR_PM_Peak'] + row['CR_Evening']
        new_df.loc[index, 'CR_Total'] = row['CR_AM_Peak'] + row['CR_Midday'] + row['CR_PM_Peak'] + row['CR_Evening']
        new_df.loc[index, 'CR_AM_Peak'] =row['CR_AM_Peak']
        # new_df.loc[index, 'CR_AM_Peak'] =row['CR_PRE_EARLY_AM']+row['CR_EARLY_AM']+ row['CR_AM_Peak']
        # new_df.loc[index, 'DB_PRE_Early_AM_Peak'] = pre_early_am_value
        # new_df.loc[index, 'DB_Early_AM_Peak'] = early_am_value
        new_df.loc[index, 'DB_AM_Peak'] = am_value
        new_df.loc[index, 'DB_Midday'] = midday_value
        new_df.loc[index, 'DB_PM_Peak'] = pm_value
        new_df.loc[index, 'DB_Evening'] = evening_value
        # new_df.loc[index, 'DB_Total'] = evening_value + am_value + midday_value + pm_value+pre_early_am_value+early_am_value
        new_df.loc[index, 'DB_Total'] = evening_value + am_value + midday_value + pm_value
        
        # Join the IDs as a comma-separated string
        # new_df.loc[index, 'DB_PRE_Early_AM_IDS'] = ', '.join(map(str, pre_early_am_value_ids))
        # new_df.loc[index, 'DB_Early_AM_IDS'] = ', '.join(map(str, early_am_value_ids))
        new_df.loc[index, 'DB_AM_IDS'] = ', '.join(map(str, am_value_ids))
        new_df.loc[index, 'DB_Midday_IDS'] = ', '.join(map(str, midday_value_ids))
        new_df.loc[index, 'DB_PM_IDS'] = ', '.join(map(str, pm_value_ids))
        new_df.loc[index, 'DB_Evening_IDS'] = ', '.join(map(str, evening_value_ids))

    # new_df.to_csv('Time Base Comparison(Over All).csv',index=False)

    # Route Level Comparison
    # Just for SALEM because in SALEM Code values are already splitted
    # new_df['ROUTE_SURVEYEDCode_Splited']=new_df['ROUTE_SURVEYEDCode']
    new_df['ROUTE_SURVEYEDCode_Splited']=new_df['ROUTE_SURVEYEDCode'].apply(lambda x:('_').join(x.split('_')[:-1]) )

    # creating new dataframe for ROUTE_LEVEL_Comparison
    route_level_df=pd.DataFrame()

    unique_routes=new_df['ROUTE_SURVEYEDCode_Splited'].unique()

    route_level_df['ROUTE_SURVEYEDCode']=unique_routes

    route_df.rename(columns={'ROUTE_TOTAL':'CR_Overall_Goal','SURVEY_ROUTE_CODE':'ROUTE_SURVEYEDCode','LS_NAME_CODE':'ROUTE_SURVEYEDCode'},inplace=True)

    route_df.dropna(subset=['ROUTE_SURVEYEDCode'],inplace=True)
    route_level_df=pd.merge(route_level_df,route_df[['ROUTE_SURVEYEDCode','CR_Overall_Goal']],on='ROUTE_SURVEYEDCode')

    # adding values from database file and compeletion report for Route_Level
    for index , row in route_level_df.iterrows():
        subset_df=new_df[new_df['ROUTE_SURVEYEDCode_Splited']==row['ROUTE_SURVEYEDCode']]
        # sum_per_route_cr = subset_df[['CR_AM_Peak', 'CR_Midday', 'CR_PM_Peak', 'CR_Evening','CR_Total','Overall Goal']].sum()
        


        # sum_per_route_cr = subset_df[['CR_PRE_Early_AM','CR_Early_AM','CR_AM_Peak', 'CR_Midday', 'CR_PM_Peak', 'CR_Evening','CR_Total']].sum()
        sum_per_route_cr = subset_df[['CR_AM_Peak', 'CR_Midday', 'CR_PM_Peak', 'CR_Evening','CR_Total']].sum()
        # sum_per_route_cr = subset_df[['CR_EARLY_AM','CR_AM_Peak', 'CR_Midday', 'CR_PM_Peak', 'CR_Evening','CR_Total']].sum()
        # sum_per_route_db = subset_df[['DB_PRE_Early_AM_Peak','DB_Early_AM_Peak','DB_AM_Peak', 'DB_Midday', 'DB_PM_Peak', 'DB_Evening','DB_Total']].sum()
        sum_per_route_db = subset_df[['DB_AM_Peak', 'DB_Midday', 'DB_PM_Peak', 'DB_Evening','DB_Total']].sum()
        
        # route_level_df.loc[index,'CR_PRE_Early_AM']=sum_per_route_cr['CR_PRE_Early_AM']
        # route_level_df.loc[index,'CR_Early_AM']=sum_per_route_cr['CR_Early_AM']
        route_level_df.loc[index,'CR_AM_Peak']=sum_per_route_cr['CR_AM_Peak']
        route_level_df.loc[index,'CR_Midday']=sum_per_route_cr['CR_Midday']
        route_level_df.loc[index,'CR_PM_Peak']=sum_per_route_cr['CR_PM_Peak']
        route_level_df.loc[index,'CR_Evening']=sum_per_route_cr['CR_Evening']
        route_level_df.loc[index,'CR_Total']=sum_per_route_cr['CR_Total']
        # route_level_df.loc[index,'CR_Overall_Goal']=sum_per_route_cr['Overall Goal']
        
        # route_level_df.loc[index,'DB_PRE_Early_AM_Peak']=sum_per_route_db['DB_PRE_Early_AM_Peak']
        # route_level_df.loc[index,'DB_Early_AM_Peak']=sum_per_route_db['DB_Early_AM_Peak']
        route_level_df.loc[index,'DB_AM_Peak']=sum_per_route_db['DB_AM_Peak']
        route_level_df.loc[index,'DB_Midday']=sum_per_route_db['DB_Midday']
        route_level_df.loc[index,'DB_PM_Peak']=sum_per_route_db['DB_PM_Peak']
        route_level_df.loc[index,'DB_Evening']=sum_per_route_db['DB_Evening']
        route_level_df.loc[index,'DB_Total']=sum_per_route_db['DB_Total']   
        # route_level_df.loc[index,'DB_PRE_Early_AM_IDS']=', '.join(str(value) for value in subset_df['DB_PRE_Early_AM_IDS'].values)    
        # route_level_df.loc[index,'DB_Early_AM_IDS']=', '.join(str(value) for value in subset_df['DB_Early_AM_IDS'].values)    
        route_level_df.loc[index,'DB_AM_IDS']=', '.join(str(value) for value in subset_df['DB_AM_IDS'].values)    
        route_level_df.loc[index,'DB_Midday_IDS']=', '.join(str(value) for value in subset_df['DB_Midday_IDS'].values)    
        route_level_df.loc[index,'DB_PM_IDS']=', '.join(str(value) for value in subset_df['DB_PM_IDS'].values)    
        route_level_df.loc[index,'DB_Evening_IDS']=', '.join(str(value) for value in subset_df['DB_Evening_IDS'].values)

    # route_level_df.to_csv('Route Level Comparison(Value_Check).csv',index=False)
        
    # calculating the difference between values of database and compeletion report for Route_Level
    for index, row in route_level_df.iterrows():
        # pre_early_am_peak_diff=row['CR_PRE_Early_AM']-row['DB_PRE_Early_AM_Peak']
        # early_am_peak_diff=row['CR_Early_AM']-row['DB_Early_AM_Peak']
        am_peak_diff=row['CR_AM_Peak']-row['DB_AM_Peak']
        midday_diff=row['CR_Midday']-row['DB_Midday']    
        pm_peak_diff=row['CR_PM_Peak']-row['DB_PM_Peak']
        evening_diff=row['CR_Evening']-row['DB_Evening']
        total_diff=row['CR_Total']-row['DB_Total']
        overall_difference=row['CR_Overall_Goal']-row['DB_Total']
        # route_level_df.loc[index, 'PRE_Early_AM_DIFFERENCE'] = math.ceil(max(0, pre_early_am_peak_diff))
        # route_level_df.loc[index, 'Early_AM_DIFFERENCE'] = math.ceil(max(0, early_am_peak_diff))
        route_level_df.loc[index, 'AM_DIFFERENCE'] = math.ceil(max(0, am_peak_diff))
        route_level_df.loc[index, 'Midday_DIFFERENCE'] = math.ceil(max(0, midday_diff))
        route_level_df.loc[index, 'PM_DIFFERENCE'] = math.ceil(max(0, pm_peak_diff))
        route_level_df.loc[index, 'Evening_DIFFERENCE'] = math.ceil(max(0, evening_diff))
        # route_level_df.loc[index, 'Total_DIFFERENCE'] = math.ceil(max(0, total_diff))
        # route_level_df.loc[index, 'Total_DIFFERENCE'] =math.ceil(max(0, pre_early_am_peak_diff))+math.ceil(max(0, early_am_peak_diff))+math.ceil(max(0, am_peak_diff))+math.ceil(max(0, midday_diff))+math.ceil(max(0, pm_peak_diff))+math.ceil(max(0, evening_diff))
        route_level_df.loc[index, 'Total_DIFFERENCE'] =math.ceil(max(0, am_peak_diff))+math.ceil(max(0, midday_diff))+math.ceil(max(0, pm_peak_diff))+math.ceil(max(0, evening_diff))
        route_level_df.loc[index, 'Overall_Goal_DIFFERENCE'] = math.ceil(max(0,overall_difference))

    return route_level_df


weekday_df.dropna(subset=[time_column[0]],inplace=True)
weekday_raw_df=weekday_df[['id',route_survey_column[0],'ROUTE_SURVEYED',stopon_clntid_column[0],stopoff_clntid_column[0],time_column[0],time_period_column[0],'Day','ELVIS_STATUS']]
weekend_df.dropna(subset=[time_column[0]],inplace=True)
weekend_raw_df=weekend_df[['id',route_survey_column[0],'ROUTE_SURVEYED',stopon_clntid_column[0],stopoff_clntid_column[0],time_column[0],time_period_column[0],'Day','ELVIS_STATUS']]
weekend_raw_df.rename(columns={stopon_clntid_column[0]:'BOARDING LOCATION',stopoff_clntid_column[0]:'ALIGHTING LOCATION'},inplace=True)
weekday_raw_df.rename(columns={stopon_clntid_column[0]:'BOARDING LOCATION',stopoff_clntid_column[0]:'ALIGHTING LOCATION'},inplace=True)


wkday_route_level =create_route_level_df(wkday_overall_df,wkday_route_df,weekday_df)
wkend_route_level =create_route_level_df(wkend_overall_df,wkend_route_df,weekend_df)
# wkday_route_df.to_csv("CHECk TOTAL_Difference.csv",index=False)
# wkend_route_df.to_csv("WKENDCHECk TOTAL_Difference.csv",index=False)
wkday_comparison_df=copy.deepcopy(wkday_route_level)
wkday_new_route_level_df=copy.deepcopy(wkday_route_level)

wkend_comparison_df=copy.deepcopy(wkend_route_level)
wkend_new_route_level_df=copy.deepcopy(wkend_route_level)

if not wkday_comparison_df.empty:
    for index , row in wkday_comparison_df.iterrows():
        wkday_comparison_df.loc[index,'Total_DIFFERENCE']=math.ceil(max(0,(row['CR_Total']-row['DB_Total'])))
else:
    wkday_comparison_df['Total_DIFFERENCE']=0


if not wkend_comparison_df.empty:
    for index , row in wkend_comparison_df.iterrows():
        wkend_comparison_df.loc[index,'Total_DIFFERENCE']=math.ceil(max(0,(row['CR_Total']-row['DB_Total'])))
else:
    wkend_comparison_df['Total_DIFFERENCE']=0

def create_reverse_df(df):
    trip_oppo_dir_column_check=['tripinoppodir']
    trip_oppo_dir_column=check_all_characters_present(df,trip_oppo_dir_column_check)

    route_survey_name_column_check=['routesurveyed']
    route_survey_name_column=check_all_characters_present(df,route_survey_name_column_check)

    oppo_dir_time_column_check=['oppodirtriptimecode']
    oppo_dir_time_column=check_all_characters_present(df,oppo_dir_time_column_check)

    trip_code_column_check=['prevtransferscode','nexttransferscode']
    trip_code_column=check_all_characters_present(df,trip_code_column_check)
    trip_code_column.sort()

    prev_trip_route_code_column_check=['tripfirstroutecode','tripsecondroutecode','tripthirdroutecode','tripfourthroutecode']
    next_trip_route_code_column_check=['tripnextroutecode','tripafterroutecode','trip3rdroutecode','triplast4thrtecode']
    prev_trip_route_code_column=check_all_characters_present(df,prev_trip_route_code_column_check)
    next_trip_route_code_column=check_all_characters_present(df,next_trip_route_code_column_check)

    values_to_replace = ['-oth-']
    df[[*prev_trip_route_code_column, *next_trip_route_code_column]] = df[
        [*prev_trip_route_code_column, *next_trip_route_code_column]
    ].replace(values_to_replace, np.nan)

    # reverse_df=df[df[trip_oppo_dir_column[0]].str.lower()=='yes'][['id',*route_survey_column,*route_survey_name_column]]
    reverse_df=df[df[trip_oppo_dir_column[0]].str.lower()=='yes'][['id',*route_survey_column,*route_survey_name_column,*trip_code_column,*prev_trip_route_code_column,*next_trip_route_code_column]]

    reverse_df[route_survey_column[0]]=reverse_df[route_survey_column[0]].apply(lambda x: '_'.join(x.split("_")[:-1]))

    reverse_df.reset_index(inplace=True,drop=True)

    reverse_df[[*prev_trip_route_code_column,*next_trip_route_code_column]].fillna('',inplace=True)

    return reverse_df

wkday_reverse_df=create_reverse_df(weekday_df)
wkend_reverse_df=create_reverse_df(weekend_df)

def create_all_type_values(reverse_df,route_level_df,df):
    trip_code_column_check=['prevtransferscode','nexttransferscode']
    trip_code_column=check_all_characters_present(df,trip_code_column_check)
    trip_code_column.sort()

    prev_trip_route_code_column_check=['tripfirstroutecode','tripsecondroutecode','tripthirdroutecode','tripfourthroutecode']
    next_trip_route_code_column_check=['tripnextroutecode','tripafterroutecode','trip3rdroutecode','triplast4thrtecode']
    prev_trip_route_code_column=check_all_characters_present(df,prev_trip_route_code_column_check)
    next_trip_route_code_column=check_all_characters_present(df,next_trip_route_code_column_check)

        
    def get_valid_routes(row, route_code_column):
        result_array = reverse_df[reverse_df['id'] == row['id']][route_code_column].values
        values_in_list = result_array[0, :]
        return [value for value in values_in_list if not (pd.isna(value) or value == '')]

    def process_route(route, counter_list, counter_prefix):
        counter_list[0] += 1
        rev_prefix=f'Rev-{counter_prefix}'
        random_choice = random.choice([counter_prefix,rev_prefix ])

        # Debug: Print available columns in route_level_df
        print("Available columns in route_level_df:", route_level_df.columns)
        
        # Check if 'Total_DIFFERENCE' column exists
        if 'Total_DIFFERENCE' not in route_level_df.columns:
            raise KeyError("'Total_DIFFERENCE' column is missing in route_level_df")

        values = route_level_df[route_level_df[route_survey_column[0]] == route]['Total_DIFFERENCE'].values  
        # value = int(route_level_df[route_level_df[route_survey_column[0]] == route]['Total_DIFFERENCE'].values)  
        # if value > 0:
        if len(values) > 0:
            value=int(values)
            reverse_df.loc[index, 'Type'] = f'{random_choice}{counter_list[0]}'
            route_level_df.loc[route_level_df[route_survey_column[0]] == route, 'Total_DIFFERENCE'] = value - 1
            return True
        return False


    for index, row in reverse_df.iterrows():
        random_value = random.choice([0, 1])
        # value = int(route_level_df[route_level_df[route_survey_column[0]] == row[route_survey_column[0]]]['Total_DIFFERENCE'].values)
        # value = int(route_level_df[route_level_df[route_survey_column[0]] == row[route_survey_column[0]]]['Total_DIFFERENCE'].values[0])
        total_difference_column_check=['totaldifference']
        total_difference_column=check_all_characters_present(route_level_df,total_difference_column_check)
        # print(total_difference_column)
        # print(route_level_df.columns)
        # exit()
        if total_difference_column:
            filtered_values = route_level_df[route_level_df[route_survey_column[0]] == row[route_survey_column[0]]]['Total_DIFFERENCE'].values
        else:
            filtered_values=[0]
        value = int(filtered_values[0]) if len(filtered_values) > 0 else 0
        # prev_trans_value = int(df[df['id'] == row['id']][trip_code_column[1]].values)
        prev_trans_values = df[df['id'] == row['id']][trip_code_column[0]].values

        # Check if value is NaN and set next_trans_value accordingly
        if pd.isna(prev_trans_values):
            prev_trans_value = 0
        else:
            prev_trans_value = int(prev_trans_values)

        # next_trans_value = int(df[df['id'] == row['id']][trip_code_column[0]].values)
        next_trans_values = df[df['id'] == row['id']][trip_code_column[0]].values

        # Check if value is NaN and set next_trans_value accordingly
        if pd.isna(next_trans_values):
            next_trans_value = 0
        else:
            next_trans_value = int(next_trans_values)
        counter = [0]  # Use a list to store the counter value

        if random_value:
            if value > 0:
                reverse_df.loc[index, 'Type'] = 'Reverse'
                route_level_df.loc[route_level_df[route_survey_column[0]] == row[route_survey_column[0]], 'Total_DIFFERENCE'] = value - 1
            elif prev_trans_value:
                for route in get_valid_routes(row, prev_trip_route_code_column):
                    result_value=process_route(route, counter, 'p')
                    if result_value:
                        break
                    else:
                        reverse_df.loc[index, 'Type'] = f'{random.choice(["p1","Rev-p1"])}'
                        route_level_df.loc[route_level_df[route_survey_column[0]] == row[route_survey_column[0]], 'Total_DIFFERENCE'] = value - 1
                        break
            elif next_trans_value:
                for route in get_valid_routes(row, next_trip_route_code_column):
                    result_value=process_route(route, counter, 'n')
                    if result_value:
                        break
                    else:
                        reverse_df.loc[index, 'Type'] = f'{random.choice(["n1","Rev-n1"])}'
                        route_level_df.loc[route_level_df[route_survey_column[0]] == row[route_survey_column[0]], 'Total_DIFFERENCE'] = value - 1
                        break
            else:
                reverse_df.loc[index, 'Type'] = 'Reverse'
        else:
            if prev_trans_value:
                for route in get_valid_routes(row, prev_trip_route_code_column):
                    result_value=process_route(route, counter, 'p')
                    if result_value:
                        break
                    else:
                        reverse_df.loc[index, 'Type'] = f'{random.choice(["p1","Rev-p1"])}'
                        route_level_df.loc[route_level_df[route_survey_column[0]] == row[route_survey_column[0]], 'Total_DIFFERENCE'] = value - 1
                        break
            elif next_trans_value:
                for route in get_valid_routes(row, next_trip_route_code_column):
                    result_value=process_route(route, counter, 'n')
                    if result_value:
                        break
                    else:
                        reverse_df.loc[index, 'Type'] = f'{random.choice(["n1","Rev-n1"])}'
                        route_level_df.loc[route_level_df[route_survey_column[0]] == row[route_survey_column[0]], 'Total_DIFFERENCE'] = value - 1
                        break
            else:
                reverse_df.loc[index, 'Type'] = 'Reverse'
    return reverse_df

wkday_reverse_df=create_all_type_values(wkday_reverse_df,wkday_route_level,weekday_df)

wkday_all_type_df=copy.deepcopy(wkday_reverse_df)

wkday_route_level=copy.deepcopy(wkday_new_route_level_df)


# exit()

def create_required_type_values(reverse_df,route_level_df,df):
    trip_code_column_check=['prevtransferscode','nexttransferscode']
    trip_code_column=check_all_characters_present(df,trip_code_column_check)
    trip_code_column.sort()

    prev_trip_route_code_column_check=['tripfirstroutecode','tripsecondroutecode','tripthirdroutecode','tripfourthroutecode']
    next_trip_route_code_column_check=['tripnextroutecode','tripafterroutecode','trip3rdroutecode','triplast4thrtecode']
    prev_trip_route_code_column=check_all_characters_present(df,prev_trip_route_code_column_check)
    next_trip_route_code_column=check_all_characters_present(df,next_trip_route_code_column_check)
    # implemented the logic for handling Type values for all the data where opossite direction is True and difference between database and compeletion report is greater than 0
    for index, row in reverse_df.iterrows():
        # value=int(route_level_df[route_level_df[route_survey_column[0]] == row[route_survey_column[0]]]['Total_DIFFERENCE'].values)
        total_difference_column_check=['totaldifference']
        total_difference_column=check_all_characters_present(route_level_df,total_difference_column_check)
        if total_difference_column:
            filtered_values = route_level_df[route_level_df[route_survey_column[0]] == row[route_survey_column[0]]]['Total_DIFFERENCE'].values
        else:
            filtered_values=[0]
        value = int(filtered_values[0]) if len(filtered_values) > 0 else 0
        
        # overall_value=int(route_level_df[route_level_df[route_survey_column[0]] == row[route_survey_column[0]]]['Overall_Goal_DIFFERENCE'].values)
        overall_values_check=['overallgoaldifference']
        overall_values=check_all_characters_present(route_level_df,overall_values_check)
        if overall_values:
            filtered_overall_values=route_level_df[route_level_df[route_survey_column[0]] == row[route_survey_column[0]]]['Overall_Goal_DIFFERENCE'].values
        else: 
            filtered_overall_values=[0]

        overall_value=int(filtered_overall_values[0]) if len(filtered_overall_values) > 0 else 0
        

        # prev_trans_value = int(reverse_df[reverse_df['id'] == row['id']][trip_code_column[1]].values)
        # next_trans_value = int(reverse_df[reverse_df['id'] == row['id']][trip_code_column[0]].values)
        prev_value_array = reverse_df[reverse_df['id'] == row['id']][trip_code_column[1]].values
        next_value_array = reverse_df[reverse_df['id'] == row['id']][trip_code_column[0]].values
        def safe_convert_to_int(value_array):
            if value_array.size == 0:  # If the array is empty
                return 0
            value = value_array[0]  # Get the first (and supposed only) element
            if pd.isna(value):  # Check if the value is NaN
                return 0
            else:
                return int(value)  # Convert to integer if it's not NaN
        prev_trans_value = safe_convert_to_int(prev_value_array)
        next_trans_value = safe_convert_to_int(next_value_array)

        if value>0: 
            if random.choice([0,1]):
                reverse_df.loc[index,'Type']='Reverse'
                route_level_df.loc[route_level_df[route_survey_column[0]] == row[route_survey_column[0]], 'Total_DIFFERENCE'] = value - 1
                route_level_df.loc[route_level_df[route_survey_column[0]] == row[route_survey_column[0]], 'Overall_Goal_DIFFERENCE'] = overall_value - 1
            else:
                if prev_trans_value:
                    result_array = reverse_df[reverse_df['id'] == row['id']][prev_trip_route_code_column].values
                    values_in_list = result_array[0, :]
                    valid_values = [value for value in values_in_list if not (pd.isna(value) or value == '')]
                    prev_counter=0
                    for route in valid_values:
                        prev_counter+=1
                        filtered_total_values=route_level_df[(route_level_df[route_survey_column[0]]==row[route_survey_column[0]])]['Total_DIFFERENCE'].values
                        value = int(filtered_total_values[0]) if len(filtered_total_values) > 0 else 0
                        # value=int(route_level_df[(route_level_df[route_survey_column[0]]==row[route_survey_column[0]])]['Total_DIFFERENCE'].values)
                        if value >0:
                            
                            reverse_df.loc[index,'Type']=f'{random.choice([f"p{prev_counter}",f"Rev-p{prev_counter}"])}'
                            route_level_df.loc[route_level_df[route_survey_column[0]] == row[route_survey_column[0]],'Total_DIFFERENCE'] = value - 1
                            route_level_df.loc[route_level_df[route_survey_column[0]] == row[route_survey_column[0]], 'Overall_Goal_DIFFERENCE'] = overall_value - 1
                            
                            break
                elif next_trans_value:
                    result_array = reverse_df[reverse_df['id'] == row['id']][next_trip_route_code_column].values
                    values_in_list = result_array[0, :]
                    valid_values = [value for value in values_in_list if not (pd.isna(value) or value == '')]
                    next_counter=0
                    for route in valid_values:
                        next_counter+=1
                        filtered_total_values=route_level_df[(route_level_df[route_survey_column[0]]==row[route_survey_column[0]])]['Total_DIFFERENCE'].values
                        value = int(filtered_total_values[0]) if len(filtered_total_values) > 0 else 0
                        # value=int(route_level_df[(route_level_df[route_survey_column[0]]==row[route_survey_column[0]])]['Total_DIFFERENCE'].values)
                        if value >0:
                            reverse_df.loc[index,'Type']=f'{random.choice([f"n{next_counter}",f"Rev-n{next_counter}"])}'
                            route_level_df.loc[route_level_df[route_survey_column[0]] == row[route_survey_column[0]],'Total_DIFFERENCE'] = value - 1
                            route_level_df.loc[route_level_df[route_survey_column[0]] == row[route_survey_column[0]], 'Overall_Goal_DIFFERENCE'] = overall_value - 1
                            break
                else:
                    reverse_df.loc[index,'Type']='Reverse'
                    route_level_df.loc[route_level_df[route_survey_column[0]] == row[route_survey_column[0]],'Total_DIFFERENCE'] = value - 1
                    route_level_df.loc[route_level_df[route_survey_column[0]] == row[route_survey_column[0]], 'Overall_Goal_DIFFERENCE'] = overall_value - 1              
        elif overall_value>0:
            reverse_df.loc[index,'Type']='Reverse'
            route_level_df.loc[route_level_df[route_survey_column[0]] == row[route_survey_column[0]], 'Overall_Goal_DIFFERENCE'] = overall_value - 1
        else:
            reverse_df.loc[index,'Type']=''
    
    return reverse_df

wkday_reverse_df=create_required_type_values(wkday_reverse_df,wkday_route_level,weekday_df)
wkend_reverse_df=create_required_type_values(wkend_reverse_df,wkend_route_level,weekend_df)

wkend_reverse_df['COMPLETED By']=''
wkday_reverse_df['COMPLETED By']=''
wkday_all_type_df['COMPLETED By']=''

# wkday_all_type_df['Type'].fillna('Reverse',inplace=True)



route_survey_name_column_check=['routesurveyed']
route_survey_name_column=check_all_characters_present(df,route_survey_name_column_check)

# For wkday_reverse_df
if not wkday_reverse_df.empty:
    wkday_reverse_df_filtered = wkday_reverse_df[(wkday_reverse_df['Type'].str.strip() != '')]
else:
    wkday_reverse_df['Type']=''
    wkday_reverse_df_filtered=wkday_reverse_df

# For wkend_reverse_df
if not wkend_reverse_df.empty:
    wkend_reverse_df_filtered = wkend_reverse_df[wkend_reverse_df['Type'].str.strip() != '']
else:
    wkend_reverse_df['Type']=''
    wkend_reverse_df_filtered=wkend_reverse_df


generateable_column_check=['generatabletrips']
generateable_column=check_all_characters_present(df,generateable_column_check)

for _,row in wkday_reverse_df_filtered.iterrows():
    value=df[df['id']==row['id']][generateable_column[0]].values
    if  pd.isna(value[0]):
        wkday_reverse_df_filtered.loc[row.name,'Generated Trips']='Not Used'
    else:
        wkday_reverse_df_filtered.loc[row.name,'Generated Trips']='Used'

for _,row in wkend_reverse_df_filtered.iterrows():
    value=df[df['id']==row['id']][generateable_column[0]].values
    if pd.isna(value[0]):
        wkend_reverse_df_filtered.loc[row.name,'Generated Trips']='Not Used'
    else:
        wkend_reverse_df_filtered.loc[row.name,'Generated Trips']='Used'

# wkend_comparison_df.rename(columns={'CR_PRE_Early_AM':'(0) Goal','CR_Early_AM':'(1) Goal','CR_AM_Peak':'(2) Goal','CR_Midday':'(3) Goal','CR_PM_Peak':'(4) Goal','CR_Evening':'(5) Goal',
#          'DB_PRE_Early_AM_Peak':'(0) Collect', 'DB_Early_AM_Peak':'(1) Collect', 'DB_AM_Peak':'(2) Collect',
#        'DB_Midday':'(3) Collect', 'DB_PM_Peak':'(4) Collect', 'DB_Evening':'(5) Collect','PRE_Early_AM_DIFFERENCE':'(0) Remain',
#        'Early_AM_DIFFERENCE':'(1) Remain', 'AM_DIFFERENCE':'(2) Remain', 'Midday_DIFFERENCE':'(3) Remain',
#        'PM_DIFFERENCE':'(4) Remain', 'Evening_DIFFERENCE':'(5) Remain','CR_Overall_Goal':'Route Level Goal','DB_Total':'# of Surveys','Overall_Goal_DIFFERENCE':'Remaining'},inplace=True)

# wkday_comparison_df.rename(columns={'CR_PRE_Early_AM':'(0) Goal','CR_Early_AM':'(1) Goal','CR_AM_Peak':'(2) Goal','CR_Midday':'(3) Goal','CR_PM_Peak':'(4) Goal','CR_Evening':'(5) Goal',
#          'DB_PRE_Early_AM_Peak':'(0) Collect', 'DB_Early_AM_Peak':'(1) Collect', 'DB_AM_Peak':'(2) Collect',
#        'DB_Midday':'(3) Collect', 'DB_PM_Peak':'(4) Collect', 'DB_Evening':'(5) Collect','PRE_Early_AM_DIFFERENCE':'(0) Remain',
#        'Early_AM_DIFFERENCE':'(1) Remain', 'AM_DIFFERENCE':'(2) Remain', 'Midday_DIFFERENCE':'(3) Remain',
#        'PM_DIFFERENCE':'(4) Remain', 'Evening_DIFFERENCE':'(5) Remain','CR_Overall_Goal':'Route Level Goal','DB_Total':'# of Surveys','Overall_Goal_DIFFERENCE':'Remaining'},inplace=True)

# wkday_route_direction_df.rename(columns={'CR_PRE_Early_AM':'(0) Goal','CR_Early_AM':'(1) Goal','CR_AM_Peak':'(2) Goal','CR_Midday':'(3) Goal','CR_PM_Peak':'(4) Goal','CR_Evening':'(5) Goal',
#          'DB_PRE_Early_AM_Peak':'(0) Collect', 'DB_Early_AM_Peak':'(1) Collect', 'DB_AM_Peak':'(2) Collect',
#        'DB_Midday':'(3) Collect', 'DB_PM_Peak':'(4) Collect', 'DB_Evening':'(5) Collect','PRE_Early_AM_DIFFERENCE':'(0) Remain',
#        'Early_AM_DIFFERENCE':'(1) Remain', 'AM_DIFFERENCE':'(2) Remain', 'Midday_DIFFERENCE':'(3) Remain',
#        'PM_DIFFERENCE':'(4) Remain', 'Evening_DIFFERENCE':'(5) Remain'},inplace=True)

# wkend_route_direction_df.rename(columns={'CR_PRE_Early_AM':'(0) Goal','CR_Early_AM':'(1) Goal','CR_AM_Peak':'(2) Goal','CR_Midday':'(3) Goal','CR_PM_Peak':'(4) Goal','CR_Evening':'(5) Goal',
#          'DB_PRE_Early_AM_Peak':'(0) Collect', 'DB_Early_AM_Peak':'(1) Collect', 'DB_AM_Peak':'(2) Collect',
#        'DB_Midday':'(3) Collect', 'DB_PM_Peak':'(4) Collect', 'DB_Evening':'(5) Collect','PRE_Early_AM_DIFFERENCE':'(0) Remain',
#        'Early_AM_DIFFERENCE':'(1) Remain', 'AM_DIFFERENCE':'(2) Remain', 'Midday_DIFFERENCE':'(3) Remain',
#        'PM_DIFFERENCE':'(4) Remain', 'Evening_DIFFERENCE':'(5) Remain'},inplace=True)



wkend_comparison_df.rename(columns={'CR_AM_Peak':'(1) Goal','CR_Midday':'(2) Goal','CR_PM_Peak':'(3) Goal','CR_Evening':'(4) Goal',
         'DB_AM_Peak':'(1) Collect',
       'DB_Midday':'(2) Collect', 'DB_PM_Peak':'(3) Collect', 'DB_Evening':'(4) Collect', 'AM_DIFFERENCE':'(1) Remain', 'Midday_DIFFERENCE':'(2) Remain',
       'PM_DIFFERENCE':'(3) Remain', 'Evening_DIFFERENCE':'(4) Remain','CR_Overall_Goal':'Route Level Goal','DB_Total':'# of Surveys','Overall_Goal_DIFFERENCE':'Remaining'},inplace=True)

wkday_comparison_df.rename(columns={'CR_AM_Peak':'(1) Goal','CR_Midday':'(2) Goal','CR_PM_Peak':'(3) Goal','CR_Evening':'(4) Goal',
     'DB_AM_Peak':'(1) Collect',
       'DB_Midday':'(2) Collect', 'DB_PM_Peak':'(3) Collect', 'DB_Evening':'(4) Collect','AM_DIFFERENCE':'(1) Remain', 'Midday_DIFFERENCE':'(2) Remain',
       'PM_DIFFERENCE':'(3) Remain', 'Evening_DIFFERENCE':'(4) Remain','CR_Overall_Goal':'Route Level Goal','DB_Total':'# of Surveys','Overall_Goal_DIFFERENCE':'Remaining'},inplace=True)

wkday_route_direction_df.rename(columns={'CR_AM_Peak':'(1) Goal','CR_Midday':'(2) Goal','CR_PM_Peak':'(3) Goal','CR_Evening':'(4) Goal',
     'DB_AM_Peak':'(1) Collect',
       'DB_Midday':'(2) Collect', 'DB_PM_Peak':'(3) Collect', 'DB_Evening':'(4) Collect','AM_DIFFERENCE':'(1) Remain', 'Midday_DIFFERENCE':'(2) Remain',
       'PM_DIFFERENCE':'(3) Remain', 'Evening_DIFFERENCE':'(4) Remain','CR_Overall_Goal':'Route Level Goal','DB_Total':'# of Surveys','Overall_Goal_DIFFERENCE':'Remaining'},inplace=True)

wkend_route_direction_df.rename(columns={'CR_AM_Peak':'(1) Goal','CR_Midday':'(2) Goal','CR_PM_Peak':'(3) Goal','CR_Evening':'(4) Goal',
         'DB_AM_Peak':'(1) Collect',
       'DB_Midday':'(2) Collect', 'DB_PM_Peak':'(3) Collect', 'DB_Evening':'(4) Collect','AM_DIFFERENCE':'(1) Remain', 'Midday_DIFFERENCE':'(2) Remain',
       'PM_DIFFERENCE':'(3) Remain', 'Evening_DIFFERENCE':'(4) Remain'},inplace=True)



wkday_comparison_df = wkday_comparison_df.merge(
    detail_df_xfers[['ETC_ROUTE_ID', 'ETC_ROUTE_NAME']],
    left_on='ROUTE_SURVEYEDCode',
    right_on='ETC_ROUTE_ID',
    how='left'
)

# Rename the column as per requirement
wkday_comparison_df.rename(columns={'ETC_ROUTE_NAME': 'ROUTE_SURVEYED'}, inplace=True)
wkday_comparison_df.drop(columns=['ETC_ROUTE_ID'], inplace=True)

wkend_comparison_df = wkend_comparison_df.merge(
    detail_df_xfers[['ETC_ROUTE_ID', 'ETC_ROUTE_NAME']],
    left_on='ROUTE_SURVEYEDCode',
    right_on='ETC_ROUTE_ID',
    how='left'
)

# Rename the column as per requirement
wkend_comparison_df.rename(columns={'ETC_ROUTE_NAME': 'ROUTE_SURVEYED'}, inplace=True)
wkend_comparison_df.drop(columns=['ETC_ROUTE_ID'], inplace=True)

for _,row in wkday_route_direction_df.iterrows():
    route_surveyed=detail_df_stops[detail_df_stops['ETC_ROUTE_ID']==row['ROUTE_SURVEYEDCode']]['ETC_ROUTE_NAME'].iloc[0]
    route_surveyed_ID=detail_df_stops[detail_df_stops['ETC_ROUTE_ID']==row['ROUTE_SURVEYEDCode']]['ETC_ROUTE_ID'].iloc[0]
    wkday_route_direction_df.loc[row.name,'ROUTE_SURVEYED']=route_surveyed  

for _,row in wkend_route_direction_df.iterrows():
    route_surveyed=detail_df_stops[detail_df_stops['ETC_ROUTE_ID']==row['ROUTE_SURVEYEDCode']]['ETC_ROUTE_NAME'].iloc[0]
    route_surveyed_ID=detail_df_stops[detail_df_stops['ETC_ROUTE_ID']==row['ROUTE_SURVEYEDCode']]['ETC_ROUTE_ID'].iloc[0]
    wkend_route_direction_df.loc[row.name,'ROUTE_SURVEYED']=route_surveyed  

# f'{file_first_name} Route Level Comparison(Wkday & WkEnd)(v{version}).xlsx'
with pd.ExcelWriter(f'reviewtool_{today_date}_{project_name}_RouteLevelComparison(Wkday & WkEnd)_Latest_01.xlsx') as writer:
    # wkday_route_direction_df.drop(columns=['CR_Total','Total_DIFFERENCE','DB_Total']).to_excel(writer,sheet_name='WkDAY Route DIR Comparison',index=False)
    # wkend_route_direction_df.drop(columns=['CR_Total','Total_DIFFERENCE','DB_Total']).to_excel(writer,sheet_name='WkEND Route DIR Comparison',index=False)
    
    wkday_route_direction_df.drop(columns=['CR_Total','Total_DIFFERENCE']).to_excel(writer,sheet_name='WkDAY Route DIR Comparison',index=False)
    wkend_route_direction_df.drop(columns=['CR_Total','Total_DIFFERENCE']).to_excel(writer,sheet_name='WkEND Route DIR Comparison',index=False)
    
    weekday_raw_df.to_excel(writer,sheet_name='WkDAY RAW DATA',index=False)
    weekend_raw_df.to_excel(writer,sheet_name='WkEND RAW DATA',index=False)

    wkend_time_value_df.to_excel(writer,sheet_name='WkEND Time Data',index=False)
    wkday_time_value_df.to_excel(writer,sheet_name='WkDAY Time Data',index=False)

    # wkday_comparison_df.drop(columns=['CR_Total','DB_PRE_Early_AM_IDS','DB_Early_AM_IDS','DB_AM_IDS','DB_Midday_IDS','DB_PM_IDS','DB_Evening_IDS','Total_DIFFERENCE']).to_excel(writer,sheet_name='WkDAY Route Comparison',index=False)
    # wkend_comparison_df.drop(columns=['CR_Total','DB_PRE_Early_AM_IDS','DB_Early_AM_IDS','DB_AM_IDS','DB_Midday_IDS','DB_PM_IDS','DB_Evening_IDS','Total_DIFFERENCE']).to_excel(writer,sheet_name='WkEND Route Comparison',index=False)
    
    wkday_comparison_df.drop(columns=['CR_Total','DB_AM_IDS','DB_Midday_IDS','DB_PM_IDS','DB_Evening_IDS','Total_DIFFERENCE']).to_excel(writer,sheet_name='WkDAY Route Comparison',index=False)
    wkend_comparison_df.drop(columns=['CR_Total','DB_AM_IDS','DB_Midday_IDS','DB_PM_IDS','DB_Evening_IDS','Total_DIFFERENCE']).to_excel(writer,sheet_name='WkEND Route Comparison',index=False)

    latest_date_df.to_excel(writer, index=False, sheet_name='LAST SURVEY DATE')

    # wkday_all_type_df[['id',route_survey_column[0],route_survey_name_column[0],'Type','COMPLETED By']].to_excel(writer,sheet_name='Reverse Routes',index=False)
    # wkday_reverse_df_filtered[['id', route_survey_column[0], route_survey_name_column[0], 'Type', 'COMPLETED By']].to_excel(writer, sheet_name='Reverse Routes WkDAY', index=False)
    # wkend_reverse_df_filtered[['id', route_survey_column[0], route_survey_name_column[0], 'Type', 'COMPLETED By']].to_excel(writer, sheet_name='Reverse Routes WkEND', index=False)
    
    # wkday_reverse_df_filtered[['id', route_survey_column[0], route_survey_name_column[0], 'Type', 'COMPLETED By','Generated Trips']].to_excel(writer, sheet_name='Reverse Routes WkDAY', index=False)
    # wkend_reverse_df_filtered[['id', route_survey_column[0], route_survey_name_column[0], 'Type', 'COMPLETED By','Generated Trips']].to_excel(writer, sheet_name='Reverse Routes WkEND', index=False)
print("Files Generated SuccessFully")