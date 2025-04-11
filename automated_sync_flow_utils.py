import pandas as pd
import numpy as np
from datetime import date,datetime
from geopy.distance import geodesic
import warnings
import copy
from database import DatabaseConnector
import os
from dotenv import load_dotenv
import math
import random
import base64
import streamlit as st
import io
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import boto3
from io import BytesIO
from decouple import config
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

warnings.filterwarnings('ignore')

load_dotenv()

def edit_ls_code_column(x):
    value=x.split('_')
    if len(value)>3:
        route_value="_".join(value[:-1])
    else:
        route_value="_".join(value)
    return route_value

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

def clean_string(s):
    return s.replace('_', '').replace('[', '').replace(']', '').replace(' ','').replace('#','').lower()


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

def get_day_name(x):
    if pd.isna(x):  # Check if x is NaT (missing value)
        return None  # Or return 'Unknown' or 'Missing' based on your needs
    formats_to_check = ['%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M']
    
    for format_str in formats_to_check:
        try:
            date_object = datetime.strptime(x, format_str)
            day_name = date_object.strftime('%A')
            return day_name
        except ValueError:
            continue

def create_time_value_df_with_display(overall_df,df,time_column,project):

    """
    Create a time-value DataFrame summarizing counts and time ranges.

    Parameters:
        df (pd.DataFrame): Input DataFrame containing the time values.
        time_column (str): Name of the column in the input DataFrame containing the time values.

    Returns:
        pd.DataFrame: Processed DataFrame with counts, time ranges, and display text.
    """

    if project=='TUCSON':
        am_values = ['AM1','AM2','AM3', 'AM4']
        midday_values = [ 'MID1', 'MID2','MID3', 'MID4', 'MID5', 'MID6' ]
        pm_values = ['PM1','PM2', 'PM3']
        evening_values = ['OFF1', 'OFF2', 'OFF3', 'OFF4','OFF5']
        # for TUCSON have to change time groups too
        time_group_mapping = {
            1: am_values,
            2: midday_values,
            3: pm_values,
            4: evening_values,
        }

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

    elif project=='TUCSON RAIL':
            # Filter df where overall_df['ROUTE_SURVEYEDCode'] == df['ROUTE_SURVEYEDCode_Splited']
        matched_df = df[df['ROUTE_SURVEYEDCode_Splited'].isin(overall_df['ROUTE_SURVEYEDCode'])]

        # Define time value groups
        am_values = ['AM1','AM2','AM3', 'AM4']
        midday_values = [ 'MID1', 'MID2','MID3', 'MID4', 'MID5', 'MID6' ]
        pm_values = ['PM1','PM2', 'PM3']
        evening_values = ['OFF1', 'OFF2', 'OFF3', 'OFF4','OFF5']
        # for TUCSON have to change time groups too
        time_group_mapping = {
            1: am_values,
            2: midday_values,
            3: pm_values,
            4: evening_values,
        }

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
                count = matched_df[matched_df[time_column[0]] == value].shape[0]
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
    elif project=='VTA':
        pre_early_am_values = ['AM1']
        early_am_values = ['AM2']
        am_values = ['AM3', 'AM4', 'MID1', 'MID2', 'MID7']
        midday_values = ['MID3', 'MID4', 'MID5', 'MID6', 'PM1']
        pm_values = ['PM2', 'PM3', 'PM4', 'PM5']
        evening_values = ['PM6', 'PM7', 'PM8', 'PM9']

        # Mapping time groups to corresponding columns
        time_group_mapping = {
            0: pre_early_am_values,
            1: early_am_values,
            2: am_values,
            3: midday_values,
            4: pm_values,
            5: evening_values,
        }

        # Mapping time values to time ranges
        time_mapping = {
            'AM1': 'Before 5:00 am',
            'AM2': '5:00 am - 6:00 am',
            'AM3': '6:00 am - 7:00 am',
            'MID1': '7:00 am - 8:00 am',
            'MID2': '8:00 am - 9:00 am',
            'MID7': '9:00 am - 10:00 am',
            'MID3': '10:00 am - 11:00 am',
            'MID4': '11:00 am - 12:00 pm',
            'MID5': '12:00 pm - 1:00 pm',
            'MID6': '1:00 pm - 2:00 pm',
            'PM1': '2:00 pm - 3:00 pm',
            'PM2': '3:00 pm - 4:00 pm',
            'PM3': '4:00 pm - 5:00 pm',
            'PM4': '5:00 pm - 6:00 pm',
            'PM5': '6:00 pm - 7:00 pm',
            'PM6': '7:00 pm - 8:00 pm',
            'PM7': '8:00 pm - 9:00 pm',
            'PM8': '9:00 pm - 10:00 pm',
            'PM9': 'After 10:00 pm'
        }

        # Initialize the new DataFrame
        new_df = pd.DataFrame(columns=["Original Text", 0, 1, 2, 3, 4, 5])

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
    elif project=='UTA':
            # Filter df where overall_df['ROUTE_SURVEYEDCode'] == df['ROUTE_SURVEYEDCode_Splited']
        matched_df = df[df['ROUTE_SURVEYEDCode_Splited'].isin(overall_df['ROUTE_SURVEYEDCode'])]

        # Define time value groups
        pre_early_am_values = [1]
        early_am_values = [2]
        am_values = [3, 4, 5, 6]
        midday_values = [7, 8, 9, 10, 11]
        pm_values = [12, 13, 14]
        evening_values = [15, 16, 17, 18]

        # Mapping time groups to corresponding columns
        time_group_mapping = {
            0: pre_early_am_values,
            1: early_am_values,
            2: am_values,
            3: midday_values,
            4: pm_values,
            5: evening_values,
        }

        # Mapping time values to time ranges
        time_mapping = {
            1: 'Before 5:00 am',
            2: '5:00 am - 6:00 am',
            3: '6:00 am - 7:00 am',
            4: '7:00 am - 8:00 am',
            5: '8:00 am - 9:00 am',
            6: '9:00 am - 10:00 am',
            7: '10:00 am - 11:00 am',
            8: '11:00 am - 12:00 pm',
            9: '12:00 pm - 1:00 pm',
            10: '1:00 pm - 2:00 pm',
            11: '2:00 pm - 3:00 pm',
            12: '3:00 pm - 4:00 pm',
            13: '4:00 pm - 5:00 pm',
            14: '5:00 pm - 6:00 pm',
            15: '6:00 pm - 7:00 pm',
            16: '7:00 pm - 8:00 pm',
            17: '8:00 pm - 9:00 pm',
            18: 'After 9:00 pm',
        }

        # Ensure the time_column is of integer type
        matched_df[time_column] = matched_df[time_column].fillna(0).astype(int)

        # Initialize the new DataFrame
        new_df = pd.DataFrame(columns=["Original Text", 0, 1, 2, 3, 4, 5])

        # Populate the DataFrame with counts
        for col, values in time_group_mapping.items():
            for value in values:
                count = matched_df[matched_df[time_column[0]] == value].shape[0]
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
    elif project=='STL':
        am_values = ['AM1','AM2','AM3', 'AM4']
        midday_values = ['MID1','MID2', 'MID3', 'MID4', 'MID5']
        pm_values = ['PM1','PM2','PM3','PM4','PM5']
        evening_values = ['EVE1','EVE2','EVE3','EVE4']


        # Mapping time groups to corresponding columns
        time_group_mapping = {
            1: am_values,
            2: midday_values,
            3: pm_values,
            4: evening_values
        }


        time_mapping = {
            'AM1': 'Before 6:00 am',
            'AM2': '6:00 am - 7:00 am',
            'AM3': '7:00 am - 8:00 am',
            'AM4': '8:00 am - 9:00 am',
            'MID1': '9:00 am - 10:00 am',
            'MID2': '10:00 am - 11:00 am',
            'MID3': '11:00 am - 12:00 pm',
            'MID4': '12:00 pm - 1:00 pm',
            'MID5': '1:00 pm - 2:00 pm',
            'PM1': '2:00 pm - 3:00 pm',
            'PM2': '3:00 pm - 4:00 pm',
            'PM3': '4:00 pm - 5:00 pm',
            'PM4': '5:00 pm - 6:00 pm',
            'PM5': '6:00 pm - 7:00 pm',
            'EVE1': '7:00 pm - 8:00 pm',
            'EVE2': '8:00 pm - 9:00 pm',
            'EVE3': '9:00 pm - 10:00 pm',
            'EVE4': 'After 10:00 pm'
        }


        # Initialize the new DataFrame
        new_df = pd.DataFrame(columns=["Original Text", 1, 2, 3, 4])

        # Populate the DataFrame with counts
        for col, values in time_group_mapping.items():
            for value in values:
                count = df[df[time_column[0]] == value].shape[0]
                row = {"Original Text": value}

                # Initialize all columns to 0
                for c in range(1,5):
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


def create_route_direction_level_df(overalldf,df,time_column,project):
    if project=='TUCSON':
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
    elif project=='TUCSON RAIL':
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

        # Creating new dataframe for specifically AM, PM, MIDDAY, Evening data and added values from Completion Report
        new_df = pd.DataFrame()
        new_df['ROUTE_SURVEYEDCode']=overalldf['LS_NAME_CODE']
        new_df['STATION_ID']=overalldf['STATION_ID']
        new_df['STATION_ID_SPLITTED']=overalldf['STATION_ID_SPLITTED']
        new_df['CR_AM_Peak'] = pd.to_numeric(overalldf[am_column[0]], errors='coerce').fillna(0).apply(math.ceil)
        new_df['CR_Midday'] = pd.to_numeric(overalldf[midday_colum[0]], errors='coerce').fillna(0).apply(math.ceil)
        new_df['CR_PM_Peak'] = pd.to_numeric(overalldf[pm_column[0]], errors='coerce').fillna(0).apply(math.ceil)
        new_df['CR_Evening'] = pd.to_numeric(overalldf[evening_column[0]], errors='coerce').fillna(0).apply(math.ceil)
        # print("new_df_columns",new_df.columns)
        new_df[['CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']]=new_df[['CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']].applymap(convert_string_to_integer)
        new_df.fillna(0,inplace=True)

        for index, row in new_df.iterrows():
            route_code = row['ROUTE_SURVEYEDCode']
            station_id=row['STATION_ID_SPLITTED']

            def get_counts_and_ids(time_values):

                subset_df = df[(df['ROUTE_SURVEYEDCode'] == route_code)&(df['STATION_ID_SPLITTED']==station_id)  & (df[time_column[0]].isin(time_values))]
                subset_df=subset_df.drop_duplicates(subset='id')
                count = subset_df.shape[0]
                ids = subset_df['id'].values
                return count, ids
            am_value, am_value_ids = get_counts_and_ids(am_values)
            midday_value, midday_value_ids = get_counts_and_ids(midday_values)
            pm_value, pm_value_ids = get_counts_and_ids(pm_values)
            evening_value, evening_value_ids = get_counts_and_ids(evening_values)
            
            new_df.loc[index, 'CR_Total'] =row['CR_AM_Peak'] + row['CR_Midday'] + row['CR_PM_Peak'] + row['CR_Evening']
            new_df.loc[index, 'CR_AM_Peak'] =row['CR_AM_Peak']

            new_df.loc[index, 'DB_AM_Peak'] = am_value
            new_df.loc[index, 'DB_Midday'] = midday_value
            new_df.loc[index, 'DB_PM_Peak'] = pm_value
            new_df.loc[index, 'DB_Evening'] = evening_value
            new_df.loc[index, 'DB_Total'] = evening_value + am_value + midday_value + pm_value

            route_code_level_df=pd.DataFrame()

            unique_routes=new_df['ROUTE_SURVEYEDCode'].unique()

            route_code_level_df['ROUTE_SURVEYEDCode']=unique_routes



            for index, row in new_df.iterrows():
                am_peak_diff=row['CR_AM_Peak']-row['DB_AM_Peak']
                midday_diff=row['CR_Midday']-row['DB_Midday']    
                pm_peak_diff=row['CR_PM_Peak']-row['DB_PM_Peak']
                evening_diff=row['CR_Evening']-row['DB_Evening']
                total_diff=row['CR_Total']-row['DB_Total']
                new_df.loc[index, 'AM_DIFFERENCE'] = math.ceil(max(0, am_peak_diff))
                new_df.loc[index, 'Midday_DIFFERENCE'] = math.ceil(max(0, midday_diff))
                new_df.loc[index, 'PM_DIFFERENCE'] = math.ceil(max(0, pm_peak_diff))
                new_df.loc[index, 'Evening_DIFFERENCE'] = math.ceil(max(0, evening_diff))

                new_df.loc[index, 'Total_DIFFERENCE'] =math.ceil(max(0, am_peak_diff))+math.ceil(max(0, midday_diff))+math.ceil(max(0, pm_peak_diff))+math.ceil(max(0, evening_diff))
    elif project=='VTA':
        pre_early_am_values=['AM1'] 
        early_am_values=['AM2'] 
        am_values=['AM3','AM4','MID1','MID2','MID7'] 
        midday_values=['MID3','MID4','MID5','MID6','PM1']
        pm_values=['PM2','PM3','PM4','PM5']
        evening_values=['PM6','PM7','PM8','PM9']
        pre_early_am_column=[0]  #0 is for Pre-Early AM header
        early_am_column=[1]  #1 is for Early AM header
        am_column=[2] #This is for AM header
        midday_colum=[3] #this is for MIDDAY header
        pm_column=[4] #this is for PM header
        evening_column=[5] #this is for EVENING header

        def convert_string_to_integer(x):
            try:
                return float(x)
            except (ValueError, TypeError):
                return 0
            
        # Creating new dataframe for specifically AM, PM, MIDDAY, Evenving data and added values from Compeletion Report
        new_df=pd.DataFrame()
        new_df['ROUTE_SURVEYEDCode']=overalldf['LS_NAME_CODE']
        new_df['CR_PRE_Early_AM']=overalldf[pre_early_am_column[0]].apply(math.ceil)
        new_df['CR_Early_AM']=overalldf[early_am_column[0]].apply(math.ceil)
        new_df['CR_AM_Peak']=overalldf[am_column[0]].apply(math.ceil)
        new_df['CR_Midday']=overalldf[midday_colum[0]].apply(math.ceil)
        new_df['CR_PM_Peak']=overalldf[pm_column[0]].apply(math.ceil)
        new_df['CR_Evening']=overalldf[evening_column[0]].apply(math.ceil)
        # print("new_df_columns",new_df.columns)
        new_df[['CR_PRE_Early_AM','CR_Early_AM','CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']]=new_df[['CR_PRE_Early_AM','CR_Early_AM','CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']].applymap(convert_string_to_integer)
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

            pre_early_am_value, pre_early_am_value_ids = get_counts_and_ids(pre_early_am_values)
            early_am_value, early_am_value_ids = get_counts_and_ids(early_am_values)
            am_value, am_value_ids = get_counts_and_ids(am_values)
            midday_value, midday_value_ids = get_counts_and_ids(midday_values)
            pm_value, pm_value_ids = get_counts_and_ids(pm_values)
            evening_value, evening_value_ids = get_counts_and_ids(evening_values)
            
            new_df.loc[index, 'CR_Total'] = row['CR_PRE_Early_AM']+row['CR_Early_AM']+row['CR_AM_Peak'] + row['CR_Midday'] + row['CR_PM_Peak'] + row['CR_Evening']
            new_df.loc[index, 'CR_AM_Peak'] =row['CR_AM_Peak']
            # new_df.loc[index, 'CR_AM_Peak'] =row['CR_PRE_EARLY_AM']+row['CR_EARLY_AM']+ row['CR_AM_Peak']
            new_df.loc[index, 'DB_PRE_Early_AM_Peak'] = pre_early_am_value
            new_df.loc[index, 'DB_Early_AM_Peak'] = early_am_value
            new_df.loc[index, 'DB_AM_Peak'] = am_value
            new_df.loc[index, 'DB_Midday'] = midday_value
            new_df.loc[index, 'DB_PM_Peak'] = pm_value
            new_df.loc[index, 'DB_Evening'] = evening_value
            new_df.loc[index, 'DB_Total'] = evening_value + am_value + midday_value + pm_value+pre_early_am_value+early_am_value
            
        #     new_df['ROUTE_SURVEYEDCode_Splited']=new_df['ROUTE_SURVEYEDCode'].apply(lambda x:('_').join(x.split('_')[:-1]) )
            route_code_level_df=pd.DataFrame()

            unique_routes=new_df['ROUTE_SURVEYEDCode'].unique()

            route_code_level_df['ROUTE_SURVEYEDCode']=unique_routes

            # weekend_df.rename(columns={'ROUTE_TOTAL':'CR_Overall_Goal','SURVEY_ROUTE_CODE':'ROUTE_SURVEYEDCode','LS_NAME_CODE':'ROUTE_SURVEYEDCode'},inplace=True)

            for index, row in new_df.iterrows():
                pre_early_am_peak_diff=row['CR_PRE_Early_AM']-row['DB_PRE_Early_AM_Peak']
                early_am_peak_diff=row['CR_Early_AM']-row['DB_Early_AM_Peak']
                am_peak_diff=row['CR_AM_Peak']-row['DB_AM_Peak']
                midday_diff=row['CR_Midday']-row['DB_Midday']    
                pm_peak_diff=row['CR_PM_Peak']-row['DB_PM_Peak']
                evening_diff=row['CR_Evening']-row['DB_Evening']
                total_diff=row['CR_Total']-row['DB_Total']
        #         overall_difference=row['CR_Overall_Goal']-row['DB_Total']
                new_df.loc[index, 'PRE_Early_AM_DIFFERENCE'] = math.ceil(max(0, pre_early_am_peak_diff))
                new_df.loc[index, 'Early_AM_DIFFERENCE'] = math.ceil(max(0, early_am_peak_diff))
                new_df.loc[index, 'AM_DIFFERENCE'] = math.ceil(max(0, am_peak_diff))
                new_df.loc[index, 'Midday_DIFFERENCE'] = math.ceil(max(0, midday_diff))
                new_df.loc[index, 'PM_DIFFERENCE'] = math.ceil(max(0, pm_peak_diff))
                new_df.loc[index, 'Evening_DIFFERENCE'] = math.ceil(max(0, evening_diff))
                # route_level_df.loc[index, 'Total_DIFFERENCE'] = math.ceil(max(0, total_diff))
                new_df.loc[index, 'Total_DIFFERENCE'] =math.ceil(max(0, pre_early_am_peak_diff))+math.ceil(max(0, early_am_peak_diff))+math.ceil(max(0, am_peak_diff))+math.ceil(max(0, midday_diff))+math.ceil(max(0, pm_peak_diff))+math.ceil(max(0, evening_diff))      
    elif project=='STL':
        am_values = ['AM1','AM2','AM3', 'AM4']
        midday_values = ['MID1','MID2', 'MID3', 'MID4', 'MID5']
        pm_values = ['PM1','PM2','PM3','PM4','PM5']
        evening_values = ['EVE1','EVE2','EVE3','EVE4']

        am_column = [1]
        midday_colum = [2]
        pm_column = [3]
        evening_column = [4]

        def convert_string_to_integer(x):
            try:
                return float(x)
            except (ValueError, TypeError):
                return 0

        new_df = pd.DataFrame()
        new_df['ROUTE_SURVEYEDCode'] = overalldf['LS_NAME_CODE']
        
        # Create columns with consistent naming

        new_df['CR_AM_Peak'] = pd.to_numeric(overalldf[am_column[0]], errors='coerce').fillna(0).apply(math.ceil)
        new_df['CR_Midday'] = pd.to_numeric(overalldf[midday_colum[0]], errors='coerce').fillna(0).apply(math.ceil)
        new_df['CR_PM'] = pd.to_numeric(overalldf[pm_column[0]], errors='coerce').fillna(0).apply(math.ceil)
        new_df['CR_Evening'] = pd.to_numeric(overalldf[evening_column[0]], errors='coerce').fillna(0).apply(math.ceil)

        new_df[['CR_AM_Peak','CR_Midday','CR_PM','CR_Evening']] =new_df[['CR_AM_Peak','CR_Midday','CR_PM','CR_Evening']].applymap(convert_string_to_integer)
        new_df.fillna(0, inplace=True)

        # Define time_column (was missing in original code)
    #     time_column = ['your_time_column_name']  # Replace with actual column name from df

        for index, row in new_df.iterrows():
            route_code = row['ROUTE_SURVEYEDCode']

            def get_counts_and_ids(time_values):
                subset_df = df[(df['ROUTE_SURVEYEDCode'] == route_code) & (df[time_column[0]].isin(time_values))]
                subset_df = subset_df.drop_duplicates(subset='id')
                count = subset_df.shape[0]
                ids = subset_df['id'].values
                return count, ids
        
            am_value, am_value_ids = get_counts_and_ids(am_values)
            midday_value, midday_value_ids = get_counts_and_ids(midday_values)
            pm_value, pm_value_ids = get_counts_and_ids(pm_values)
            evening_value, evening_value_ids = get_counts_and_ids(evening_values)
            
            # Use consistent column names (matching the creation above)
            new_df.loc[index, 'CR_Total'] = (row['CR_AM_Peak'] + row['CR_Midday'] + 
                                            row['CR_PM'] + row['CR_Evening'])
            new_df.loc[index, 'CR_AM_Peak'] = row['CR_AM_Peak']

            new_df.loc[index, 'DB_AM_Peak'] = am_value
            new_df.loc[index, 'DB_Midday'] = midday_value
            new_df.loc[index, 'DB_PM'] = pm_value
            new_df.loc[index, 'DB_Evening'] = evening_value
            new_df.loc[index, 'DB_Total'] = (evening_value + am_value + midday_value + pm_value )

            route_code_level_df=pd.DataFrame()

            unique_routes=new_df['ROUTE_SURVEYEDCode'].unique()

            route_code_level_df['ROUTE_SURVEYEDCode']=unique_routes

            # weekend_df.rename(columns={'ROUTE_TOTAL':'CR_Overall_Goal','SURVEY_ROUTE_CODE':'ROUTE_SURVEYEDCode','LS_NAME_CODE':'ROUTE_SURVEYEDCode'},inplace=True)

            for index, row in new_df.iterrows():
                # pre_early_am_peak_diff=row['CR_PRE_Early_AM']-row['DB_PRE_Early_AM_Peak']

                am_peak_diff=row['CR_AM_Peak']-row['DB_AM_Peak']
                midday_diff=row['CR_Midday']-row['DB_Midday']    
                pm_diff=row['CR_PM']-row['DB_PM']
                evening_diff=row['CR_Evening']-row['DB_Evening']
                total_diff=row['CR_Total']-row['DB_Total']
                new_df.loc[index, 'AM_DIFFERENCE'] = math.ceil(max(0, am_peak_diff))
                new_df.loc[index, 'Midday_DIFFERENCE'] = math.ceil(max(0, midday_diff))
                new_df.loc[index, 'PM_DIFFERENCE'] = math.ceil(max(0, pm_diff))
                new_df.loc[index, 'Evening_DIFFERENCE'] = math.ceil(max(0, evening_diff))
                new_df.loc[index, 'Total_DIFFERENCE'] =math.ceil(max(0, am_peak_diff))+math.ceil(max(0, midday_diff))+math.ceil(max(0, pm_diff))+math.ceil(max(0, evening_diff))
    return new_df

def create_tucson_weekend_route_direction_level_df(overalldf,df,time_column,project):
    # For Tucson PROJECT Have to change values TIME PERIOD VALUES
    if project=='TUCSON':
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

        new_df=pd.DataFrame()
        new_df['ROUTE_SURVEYEDCode']=overalldf['LS_NAME_CODE']
        new_df['Day'] = overalldf['DAY']


        new_df['CR_AM_Peak']=overalldf[am_column[0]].apply(math.ceil)
        new_df['CR_Midday']=overalldf[midday_colum[0]].apply(math.ceil)
        new_df['CR_PM_Peak']=overalldf[pm_column[0]].apply(math.ceil)
        new_df['CR_Evening']=overalldf[evening_column[0]].apply(math.ceil)
    
        new_df[['CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']]=new_df[['CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']].applymap(convert_string_to_integer)
        new_df.fillna(0,inplace=True)

        for index, row in new_df.iterrows():
            route_code = row['ROUTE_SURVEYEDCode']
            day=row['Day']
            def get_counts_and_ids(time_values):

                subset_df = df[(df['ROUTE_SURVEYEDCode'] == route_code) & (df[time_column[0]].isin(time_values))& 
                            (df['Day'].str.lower() == str(day).lower())]
                subset_df=subset_df.drop_duplicates(subset='id')
                count = subset_df.shape[0]
                ids = subset_df['id'].values
                return count, ids

            am_value, am_value_ids = get_counts_and_ids(am_values)
            midday_value, midday_value_ids = get_counts_and_ids(midday_values)
            pm_value, pm_value_ids = get_counts_and_ids(pm_values)
            evening_value, evening_value_ids = get_counts_and_ids(evening_values)
            

            new_df.loc[index, 'CR_Total'] = row['CR_AM_Peak'] + row['CR_Midday'] + row['CR_PM_Peak'] + row['CR_Evening']
            new_df.loc[index, 'CR_AM_Peak'] =row['CR_AM_Peak']

            new_df.loc[index, 'DB_AM_Peak'] = am_value
            new_df.loc[index, 'DB_Midday'] = midday_value
            new_df.loc[index, 'DB_PM_Peak'] = pm_value
            new_df.loc[index, 'DB_Evening'] = evening_value
            new_df.loc[index, 'DB_Total'] = evening_value + am_value + midday_value + pm_value


            for index, row in new_df.iterrows():

                am_peak_diff=row['CR_AM_Peak']-row['DB_AM_Peak']
                midday_diff=row['CR_Midday']-row['DB_Midday']    
                pm_peak_diff=row['CR_PM_Peak']-row['DB_PM_Peak']
                evening_diff=row['CR_Evening']-row['DB_Evening']
                total_diff=row['CR_Total']-row['DB_Total']

                new_df.loc[index, 'AM_DIFFERENCE'] = math.ceil(max(0, am_peak_diff))
                new_df.loc[index, 'Midday_DIFFERENCE'] = math.ceil(max(0, midday_diff))
                new_df.loc[index, 'PM_DIFFERENCE'] = math.ceil(max(0, pm_peak_diff))
                new_df.loc[index, 'Evening_DIFFERENCE'] = math.ceil(max(0, evening_diff))
                new_df.loc[index, 'Total_DIFFERENCE'] =math.ceil(max(0, am_peak_diff))+math.ceil(max(0, midday_diff))+math.ceil(max(0, pm_peak_diff))+math.ceil(max(0, evening_diff))

    elif project=='TUCSON RAIL':
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

        # Creating new dataframe for specifically AM, PM, MIDDAY, Evening data and added values from Completion Report
        new_df = pd.DataFrame()
        new_df['ROUTE_SURVEYEDCode']=overalldf['LS_NAME_CODE']
        new_df['Day'] = overalldf['DAY']
        new_df['STATION_ID']=overalldf['STATION_ID']
        new_df['STATION_ID_SPLITTED']=overalldf['STATION_ID_SPLITTED']
        # new_df['CR_PRE_Early_AM'] = pd.to_numeric(overalldf[pre_early_am_column[0]], errors='coerce').fillna(0).apply(math.ceil)
        # new_df['CR_Early_AM'] = pd.to_numeric(overalldf[early_am_column[0]], errors='coerce').fillna(0).apply(math.ceil)
        new_df['CR_AM_Peak'] = pd.to_numeric(overalldf[am_column[0]], errors='coerce').fillna(0).apply(math.ceil)
        new_df['CR_Midday'] = pd.to_numeric(overalldf[midday_colum[0]], errors='coerce').fillna(0).apply(math.ceil)
        new_df['CR_PM_Peak'] = pd.to_numeric(overalldf[pm_column[0]], errors='coerce').fillna(0).apply(math.ceil)
        new_df['CR_Evening'] = pd.to_numeric(overalldf[evening_column[0]], errors='coerce').fillna(0).apply(math.ceil)
        # print("new_df_columns",new_df.columns)
        new_df[['CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']]=new_df[['CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']].applymap(convert_string_to_integer)
        # new_df[['CR_PRE_Early_AM','CR_Early_AM','CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']]=new_df[['CR_PRE_Early_AM','CR_Early_AM','CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']].applymap(convert_string_to_integer)
        new_df.fillna(0,inplace=True)
    #     new code added for merging the same ROUTE_SURVEYEDCode
        # new_df=new_df.groupby('ROUTE_SURVEYEDCode', as_index=False).sum()
        # new_df.reset_index(drop=True, inplace=True)

        for index, row in new_df.iterrows():
            route_code = row['ROUTE_SURVEYEDCode']
            station_id=row['STATION_ID_SPLITTED']
            day=row['Day']
            def get_counts_and_ids(time_values):
                # Just for SALEM
                # subset_df = df[(df['ROUTE_SURVEYEDCode_Splited'] == route_code) & (df[time_column[0]].isin(time_values))]
                subset_df = df[(df['ROUTE_SURVEYEDCode'] == route_code)&(df['STATION_ID_SPLITTED']==station_id)  & (df[time_column[0]].isin(time_values))&(df['Day'].str.lower() == str(day).lower())]
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
            
            new_df.loc[index, 'CR_Total'] =row['CR_AM_Peak'] + row['CR_Midday'] + row['CR_PM_Peak'] + row['CR_Evening']
            # new_df.loc[index, 'CR_Total'] = row['CR_PRE_Early_AM']+row['CR_Early_AM']+row['CR_AM_Peak'] + row['CR_Midday'] + row['CR_PM_Peak'] + row['CR_Evening']
            new_df.loc[index, 'CR_AM_Peak'] =row['CR_AM_Peak']
            # new_df.loc[index, 'CR_AM_Peak'] =row['CR_PRE_EARLY_AM']+row['CR_EARLY_AM']+ row['CR_AM_Peak']
            # new_df.loc[index, 'DB_PRE_Early_AM_Peak'] = pre_early_am_value
            # new_df.loc[index, 'DB_Early_AM_Peak'] = early_am_value
            new_df.loc[index, 'DB_AM_Peak'] = am_value
            new_df.loc[index, 'DB_Midday'] = midday_value
            new_df.loc[index, 'DB_PM_Peak'] = pm_value
            new_df.loc[index, 'DB_Evening'] = evening_value
            new_df.loc[index, 'DB_Total'] = evening_value + am_value + midday_value + pm_value
            # new_df.loc[index, 'DB_Total'] = evening_value + am_value + midday_value + pm_value+pre_early_am_value+early_am_value
            
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
                new_df.loc[index, 'Total_DIFFERENCE'] =math.ceil(max(0, am_peak_diff))+math.ceil(max(0, midday_diff))+math.ceil(max(0, pm_peak_diff))+math.ceil(max(0, evening_diff))
                # new_df.loc[index, 'Total_DIFFERENCE'] =math.ceil(max(0, pre_early_am_peak_diff))+math.ceil(max(0, early_am_peak_diff))+math.ceil(max(0, am_peak_diff))+math.ceil(max(0, midday_diff))+math.ceil(max(0, pm_peak_diff))+math.ceil(max(0, evening_diff))
    return new_df

def create_uta_station_wise_route_level_df(overall_df,df,time_column,time):
    pre_early_am_values = [1]
    early_am_values = [2]
    am_values = [3, 4, 5, 6]
    midday_values = [7, 8, 9, 10, 11]
    pm_values = [12, 13, 14]
    evening_values = [15, 16, 17, 18]

    pre_early_am_column=[0]  #0 is for Pre-Early AM header
    early_am_column=[1]  #1 is for Early AM header
    am_column=[2] #This is for AM header
    midday_column=[3] #this is for MIDDAY header
    pm_column=[4] #this is for PM header
    evening_column=[5] #this is for EVENING header

    def convert_string_to_integer(x):
        try:
            return float(x)
        except (ValueError, TypeError):
            return 0

    # Creating new dataframe for specifically AM, PM, MIDDAY, Evenving data and added values from Compeletion Report
    new_df=pd.DataFrame()
    new_df['ROUTE_SURVEYEDCode']=overall_df['LS_NAME_CODE']
    if time=='weekend':
        new_df['Day'] = overall_df['DAY']
    new_df['STATION_ID']=overall_df['STATION_ID']
    new_df['STATION_ID_SPLITTED']=overall_df['STATION_ID_SPLITTED']
    new_df['CR_PRE_Early_AM'] = pd.to_numeric(overall_df[pre_early_am_column[0]], errors='coerce').fillna(0).apply(math.ceil)
    new_df['CR_Early_AM'] = pd.to_numeric(overall_df[early_am_column[0]], errors='coerce').fillna(0).apply(math.ceil)
    new_df['CR_AM_Peak'] = pd.to_numeric(overall_df[am_column[0]], errors='coerce').fillna(0).apply(math.ceil)
    new_df['CR_Midday'] = pd.to_numeric(overall_df[midday_column[0]], errors='coerce').fillna(0).apply(math.ceil)
    new_df['CR_PM_Peak'] = pd.to_numeric(overall_df[pm_column[0]], errors='coerce').fillna(0).apply(math.ceil)
    new_df['CR_Evening'] = pd.to_numeric(overall_df[evening_column[0]], errors='coerce').fillna(0).apply(math.ceil)
    new_df[['CR_PRE_Early_AM','CR_Early_AM','CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']]=new_df[['CR_PRE_Early_AM','CR_Early_AM','CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']].applymap(convert_string_to_integer)
    new_df.fillna(0,inplace=True)
    new_df['ROUTE_SURVEYEDCode_Splitted']=new_df['ROUTE_SURVEYEDCode'].apply(edit_ls_code_column)
    
    for index, row in new_df.iterrows():
        route_code = row['ROUTE_SURVEYEDCode_Splitted']
        station_id=row['STATION_ID_SPLITTED']
        if time=='weekend':
            day=row['Day']
        def get_counts_and_ids(time_values):
            # Just for SALEM
            # subset_df = df[(df['ROUTE_SURVEYEDCode_Splited'] == route_code) & (df[time_column[0]].isin(time_values))]
            if time=='weekend':
                subset_df = df[(df['ROUTE_SURVEYEDCode_Splited'] == route_code)& (df['STATION_ID_SPLITTED']==station_id)& (df[time_column[0]].isin(time_values))& 
                        (df['Day'].str.lower() == str(day).lower())]
            else:
                subset_df = df[(df['ROUTE_SURVEYEDCode_Splited'] == route_code)& (df['STATION_ID_SPLITTED']==station_id)& (df[time_column[0]].isin(time_values))]
            subset_df=subset_df.drop_duplicates(subset='id')
            count = subset_df.shape[0]
            ids = subset_df['id'].values
            return count, ids

        pre_early_am_value, pre_early_am_value_ids = get_counts_and_ids(pre_early_am_values)
        early_am_value, early_am_value_ids = get_counts_and_ids(early_am_values)
        am_value, am_value_ids = get_counts_and_ids(am_values)
        midday_value, midday_value_ids = get_counts_and_ids(midday_values)
        pm_value, pm_value_ids = get_counts_and_ids(pm_values)
        evening_value, evening_value_ids = get_counts_and_ids(evening_values)
    #     print(pre_early_am_value,early_am_value,am_value,midday_value,pm_value,evening_value)
        new_df.loc[index, 'CR_Total'] = row['CR_PRE_Early_AM']+row['CR_Early_AM']+row['CR_AM_Peak'] + row['CR_Midday'] + row['CR_PM_Peak'] + row['CR_Evening']
        new_df.loc[index, 'CR_AM_Peak'] =row['CR_AM_Peak']
        # new_df.loc[index, 'CR_AM_Peak'] =row['CR_PRE_EARLY_AM']+row['CR_EARLY_AM']+ row['CR_AM_Peak']
        new_df.loc[index, 'DB_PRE_Early_AM_Peak'] = pre_early_am_value
        new_df.loc[index, 'DB_Early_AM_Peak'] = early_am_value
        new_df.loc[index, 'DB_AM_Peak'] = am_value
        new_df.loc[index, 'DB_Midday'] = midday_value
        new_df.loc[index, 'DB_PM_Peak'] = pm_value
        new_df.loc[index, 'DB_Evening'] = evening_value
        new_df.loc[index, 'DB_Total'] = evening_value + am_value + midday_value + pm_value+pre_early_am_value+early_am_value
        unique_station_ids=new_df['STATION_ID_SPLITTED'].unique()

    results = []

    # Iterate over unique station IDs
    for station_id in unique_station_ids:
        # Filter DataFrame for the current station ID
        station_df = new_df[new_df['STATION_ID_SPLITTED'] == station_id]
        
        # Iterate over unique ROUTE_SURVEYEDCode_Splitted for the current station ID
        for route_code in station_df['ROUTE_SURVEYEDCode_Splitted'].unique():
            # Filter rows for the specific route and station
            filtered_df = station_df[station_df['ROUTE_SURVEYEDCode_Splitted'] == route_code]
            
            # Sum numeric columns and convert to a single row
            summed_row = filtered_df.sum(numeric_only=True).to_frame().T
            
            # Add key identifying columns
            summed_row['ROUTE_SURVEYEDCode'] = station_df.iloc[0]['ROUTE_SURVEYEDCode']
            summed_row['STATION_ID'] = station_df.iloc[0]['STATION_ID']
            summed_row['STATION_ID_SPLITTED'] = station_id
            summed_row['ROUTE_SURVEYEDCode_Splitted'] = route_code
            
            # Append the row to results
            results.append(summed_row)

    # Concatenate all results into a new DataFrame
    route_station_wise = pd.concat(results, ignore_index=True)
    route_station_wise.drop(columns=['ROUTE_SURVEYEDCode_Splitted','STATION_ID_SPLITTED'],inplace=True)

    for index, row in route_station_wise.iterrows():
        pre_early_am_peak_diff=row['CR_PRE_Early_AM']-row['DB_PRE_Early_AM_Peak']
        early_am_peak_diff=row['CR_Early_AM']-row['DB_Early_AM_Peak']
        am_peak_diff=row['CR_AM_Peak']-row['DB_AM_Peak']
        midday_diff=row['CR_Midday']-row['DB_Midday']    
        pm_peak_diff=row['CR_PM_Peak']-row['DB_PM_Peak']
        evening_diff=row['CR_Evening']-row['DB_Evening']
        total_diff=row['CR_Total']-row['DB_Total']
#         overall_difference=row['CR_Overall_Goal']-row['DB_Total']
        route_station_wise.loc[index, 'PRE_Early_AM_DIFFERENCE'] = math.ceil(max(0, pre_early_am_peak_diff))
        route_station_wise.loc[index, 'Early_AM_DIFFERENCE'] = math.ceil(max(0, early_am_peak_diff))
        route_station_wise.loc[index, 'AM_DIFFERENCE'] = math.ceil(max(0, am_peak_diff))
        route_station_wise.loc[index, 'Midday_DIFFERENCE'] = math.ceil(max(0, midday_diff))
        route_station_wise.loc[index, 'PM_DIFFERENCE'] = math.ceil(max(0, pm_peak_diff))
        route_station_wise.loc[index, 'Evening_DIFFERENCE'] = math.ceil(max(0, evening_diff))
        # route_level_df.loc[index, 'Total_DIFFERENCE'] = math.ceil(max(0, total_diff))
        route_station_wise.loc[index, 'Total_DIFFERENCE'] =math.ceil(max(0, pre_early_am_peak_diff))+math.ceil(max(0, early_am_peak_diff))+math.ceil(max(0, am_peak_diff))+math.ceil(max(0, midday_diff))+math.ceil(max(0, pm_peak_diff))+math.ceil(max(0, evening_diff))

    return route_station_wise


def create_station_wise_route_level_df(overall_df,df,time_column):
    am_values = ['AM1','AM2','AM3', 'AM4']
    midday_values = [ 'MID1', 'MID2','MID3', 'MID4', 'MID5', 'MID6' ]
    pm_values = ['PM1','PM2', 'PM3']
    evening_values = ['OFF1', 'OFF2', 'OFF3', 'OFF4','OFF5']
    am_column=[1] #This is for AM header
    midday_column=[2] #this is for MIDDAY header
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
    new_df['STATION_ID']=overall_df['STATION_ID']
    new_df['STATION_ID_SPLITTED']=overall_df['STATION_ID_SPLITTED']
    # new_df['CR_PRE_Early_AM'] = pd.to_numeric(overall_df[pre_early_am_column[0]], errors='coerce').fillna(0).apply(math.ceil)
    # new_df['CR_Early_AM'] = pd.to_numeric(overall_df[early_am_column[0]], errors='coerce').fillna(0).apply(math.ceil)
    new_df['CR_AM_Peak'] = pd.to_numeric(overall_df[am_column[0]], errors='coerce').fillna(0).apply(math.ceil)
    new_df['CR_Midday'] = pd.to_numeric(overall_df[midday_column[0]], errors='coerce').fillna(0).apply(math.ceil)
    new_df['CR_PM_Peak'] = pd.to_numeric(overall_df[pm_column[0]], errors='coerce').fillna(0).apply(math.ceil)
    new_df['CR_Evening'] = pd.to_numeric(overall_df[evening_column[0]], errors='coerce').fillna(0).apply(math.ceil)
    new_df[['CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']]=new_df[['CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']].applymap(convert_string_to_integer)
    # new_df[['CR_PRE_Early_AM','CR_Early_AM','CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']]=new_df[['CR_PRE_Early_AM','CR_Early_AM','CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']].applymap(convert_string_to_integer)
    new_df.fillna(0,inplace=True)
    new_df['ROUTE_SURVEYEDCode_Splitted']=new_df['ROUTE_SURVEYEDCode'].apply(edit_ls_code_column)
    
    for index, row in new_df.iterrows():
        route_code = row['ROUTE_SURVEYEDCode_Splitted']
        station_id=row['STATION_ID_SPLITTED']
        def get_counts_and_ids(time_values):
            # Just for SALEM
            # subset_df = df[(df['ROUTE_SURVEYEDCode_Splited'] == route_code) & (df[time_column[0]].isin(time_values))]
            subset_df = df[(df['ROUTE_SURVEYEDCode_Splited'] == route_code)& (df['STATION_ID_SPLITTED']==station_id)& (df[time_column[0]].isin(time_values))]
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
    #     print(pre_early_am_value,early_am_value,am_value,midday_value,pm_value,evening_value)
        new_df.loc[index, 'CR_Total'] = row['CR_AM_Peak'] + row['CR_Midday'] + row['CR_PM_Peak'] + row['CR_Evening']
        # new_df.loc[index, 'CR_Total'] = row['CR_PRE_Early_AM']+row['CR_Early_AM']+row['CR_AM_Peak'] + row['CR_Midday'] + row['CR_PM_Peak'] + row['CR_Evening']
        new_df.loc[index, 'CR_AM_Peak'] =row['CR_AM_Peak']
        # new_df.loc[index, 'CR_AM_Peak'] =row['CR_PRE_EARLY_AM']+row['CR_EARLY_AM']+ row['CR_AM_Peak']
        # new_df.loc[index, 'DB_PRE_Early_AM_Peak'] = pre_early_am_value
        # new_df.loc[index, 'DB_Early_AM_Peak'] = early_am_value
        new_df.loc[index, 'DB_AM_Peak'] = am_value
        new_df.loc[index, 'DB_Midday'] = midday_value
        new_df.loc[index, 'DB_PM_Peak'] = pm_value
        new_df.loc[index, 'DB_Evening'] = evening_value
        new_df.loc[index, 'DB_Total'] = evening_value + am_value + midday_value + pm_value
        # new_df.loc[index, 'DB_Total'] = evening_value + am_value + midday_value + pm_value+pre_early_am_value+early_am_value
        unique_station_ids=new_df['STATION_ID_SPLITTED'].unique()

    results = []

    # Iterate over unique station IDs
    for station_id in unique_station_ids:
        # Filter DataFrame for the current station ID
        station_df = new_df[new_df['STATION_ID_SPLITTED'] == station_id]
        
        # Iterate over unique ROUTE_SURVEYEDCode_Splitted for the current station ID
        for route_code in station_df['ROUTE_SURVEYEDCode_Splitted'].unique():
            # Filter rows for the specific route and station
            filtered_df = station_df[station_df['ROUTE_SURVEYEDCode_Splitted'] == route_code]
            
            # Sum numeric columns and convert to a single row
            summed_row = filtered_df.sum(numeric_only=True).to_frame().T
            
            # Add key identifying columns
            summed_row['ROUTE_SURVEYEDCode'] = station_df.iloc[0]['ROUTE_SURVEYEDCode']
            summed_row['STATION_ID'] = station_df.iloc[0]['STATION_ID']
            summed_row['STATION_ID_SPLITTED'] = station_id
            summed_row['ROUTE_SURVEYEDCode_Splitted'] = route_code
            
            # Append the row to results
            results.append(summed_row)

    # Concatenate all results into a new DataFrame
    route_station_wise = pd.concat(results, ignore_index=True)
    route_station_wise.drop(columns=['ROUTE_SURVEYEDCode_Splitted','STATION_ID_SPLITTED'],inplace=True)

    for index, row in route_station_wise.iterrows():
        # pre_early_am_peak_diff=row['CR_PRE_Early_AM']-row['DB_PRE_Early_AM_Peak']
        # early_am_peak_diff=row['CR_Early_AM']-row['DB_Early_AM_Peak']
        am_peak_diff=row['CR_AM_Peak']-row['DB_AM_Peak']
        midday_diff=row['CR_Midday']-row['DB_Midday']    
        pm_peak_diff=row['CR_PM_Peak']-row['DB_PM_Peak']
        evening_diff=row['CR_Evening']-row['DB_Evening']
        total_diff=row['CR_Total']-row['DB_Total']
#         overall_difference=row['CR_Overall_Goal']-row['DB_Total']
        # route_station_wise.loc[index, 'PRE_Early_AM_DIFFERENCE'] = math.ceil(max(0, pre_early_am_peak_diff))
        # route_station_wise.loc[index, 'Early_AM_DIFFERENCE'] = math.ceil(max(0, early_am_peak_diff))
        route_station_wise.loc[index, 'AM_DIFFERENCE'] = math.ceil(max(0, am_peak_diff))
        route_station_wise.loc[index, 'Midday_DIFFERENCE'] = math.ceil(max(0, midday_diff))
        route_station_wise.loc[index, 'PM_DIFFERENCE'] = math.ceil(max(0, pm_peak_diff))
        route_station_wise.loc[index, 'Evening_DIFFERENCE'] = math.ceil(max(0, evening_diff))
        # route_level_df.loc[index, 'Total_DIFFERENCE'] = math.ceil(max(0, total_diff))
        route_station_wise.loc[index, 'Total_DIFFERENCE'] =math.ceil(max(0, am_peak_diff))+math.ceil(max(0, midday_diff))+math.ceil(max(0, pm_peak_diff))+math.ceil(max(0, evening_diff))
        # route_station_wise.loc[index, 'Total_DIFFERENCE'] =math.ceil(max(0, pre_early_am_peak_diff))+math.ceil(max(0, early_am_peak_diff))+math.ceil(max(0, am_peak_diff))+math.ceil(max(0, midday_diff))+math.ceil(max(0, pm_peak_diff))+math.ceil(max(0, evening_diff))

    return route_station_wise


def create_route_level_df(overall_df,route_df,df,time_column,project):
    if project=='TUCSON':
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
            print("In loop 899")
            route_code = row['ROUTE_SURVEYEDCode']

            # Define a function to get the counts and IDs
            def get_counts_and_ids(time_values):
                print("In get_counts_and_ids")
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
            print("In loop 965")
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
            print("In loop 1004")
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
    
    elif project=='TUCSON RAIL':
        am_values = ['AM1','AM2','AM3', 'AM4']
        midday_values = [ 'MID1', 'MID2','MID3', 'MID4', 'MID5', 'MID6' ]
        pm_values = ['PM1','PM2', 'PM3']
        evening_values = ['OFF1', 'OFF2', 'OFF3', 'OFF4','OFF5']
        am_column=[1] #This is for AM header
        midday_column=[2] #this is for MIDDAY header
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
        # new_df['CR_PRE_Early_AM'] = pd.to_numeric(overall_df[pre_early_am_column[0]], errors='coerce').fillna(0).apply(math.ceil)
        # new_df['CR_Early_AM'] = pd.to_numeric(overall_df[early_am_column[0]], errors='coerce').fillna(0).apply(math.ceil)
        new_df['CR_AM_Peak'] = pd.to_numeric(overall_df[am_column[0]], errors='coerce').fillna(0).apply(math.ceil)
        new_df['CR_Midday'] = pd.to_numeric(overall_df[midday_column[0]], errors='coerce').fillna(0).apply(math.ceil)
        new_df['CR_PM_Peak'] = pd.to_numeric(overall_df[pm_column[0]], errors='coerce').fillna(0).apply(math.ceil)
        new_df['CR_Evening'] = pd.to_numeric(overall_df[evening_column[0]], errors='coerce').fillna(0).apply(math.ceil)
        new_df[['CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']]=new_df[['CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']].applymap(convert_string_to_integer)
        # new_df[['CR_PRE_Early_AM','CR_Early_AM','CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']]=new_df[['CR_PRE_Early_AM','CR_Early_AM','CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']].applymap(convert_string_to_integer)
        new_df.fillna(0,inplace=True)
        #  new code added for merging the same ROUTE_SURVEYEDCode 
        new_df = new_df.groupby('ROUTE_SURVEYEDCode', as_index=False).sum()
        new_df.reset_index(drop=True, inplace=True)

        # adding values for AM, PM, MIDDAY and Evening from Database file to new Dataframe
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
        #     print(pre_early_am_value,early_am_value,am_value,midday_value,pm_value,evening_value)
            new_df.loc[index, 'CR_Total'] = row['CR_AM_Peak'] + row['CR_Midday'] + row['CR_PM_Peak'] + row['CR_Evening']
            # new_df.loc[index, 'CR_Total'] = row['CR_PRE_Early_AM']+row['CR_Early_AM']+row['CR_AM_Peak'] + row['CR_Midday'] + row['CR_PM_Peak'] + row['CR_Evening']
            new_df.loc[index, 'CR_AM_Peak'] =row['CR_AM_Peak']
            # new_df.loc[index, 'CR_AM_Peak'] =row['CR_PRE_EARLY_AM']+row['CR_EARLY_AM']+ row['CR_AM_Peak']
            # new_df.loc[index, 'DB_PRE_Early_AM_Peak'] = pre_early_am_value
            # new_df.loc[index, 'DB_Early_AM_Peak'] = early_am_value
            new_df.loc[index, 'DB_AM_Peak'] = am_value
            new_df.loc[index, 'DB_Midday'] = midday_value
            new_df.loc[index, 'DB_PM_Peak'] = pm_value
            new_df.loc[index, 'DB_Evening'] = evening_value
            new_df.loc[index, 'DB_Total'] = evening_value + am_value + midday_value + pm_value
            # new_df.loc[index, 'DB_Total'] = evening_value + am_value + midday_value + pm_value+pre_early_am_value+early_am_value

            
            # # Join the IDs as a comma-separated string
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

        # Have to change the name accordingly
        route_df.rename(columns={'ROUTE_TOTAL':'CR_Overall_Goal','ETC_ROUTE_ID':'ROUTE_SURVEYEDCode'},inplace=True)

        route_df.dropna(subset=['ROUTE_SURVEYEDCode'],inplace=True)
        route_level_df=pd.merge(route_level_df,route_df[['ROUTE_SURVEYEDCode','CR_Overall_Goal']],on='ROUTE_SURVEYEDCode')

        route_level_df=route_level_df.groupby('ROUTE_SURVEYEDCode', as_index=False).sum()
        route_level_df.reset_index(drop=True, inplace=True)

        # adding values from database file and compeletion report for Route_Level
        for index , row in route_level_df.iterrows():
            subset_df=new_df[new_df['ROUTE_SURVEYEDCode_Splited']==row['ROUTE_SURVEYEDCode']]
            # sum_per_route_cr = subset_df[['CR_AM_Peak', 'CR_Midday', 'CR_PM_Peak', 'CR_Evening','CR_Total','Overall Goal']].sum()



            sum_per_route_cr = subset_df[['CR_AM_Peak', 'CR_Midday', 'CR_PM_Peak', 'CR_Evening','CR_Total']].sum()
            # sum_per_route_cr = subset_df[['CR_PRE_Early_AM','CR_Early_AM','CR_AM_Peak', 'CR_Midday', 'CR_PM_Peak', 'CR_Evening','CR_Total']].sum()
            # sum_per_route_cr = subset_df[['CR_EARLY_AM','CR_AM_Peak', 'CR_Midday', 'CR_PM_Peak', 'CR_Evening','CR_Total']].sum()
            sum_per_route_db = subset_df[['DB_AM_Peak', 'DB_Midday', 'DB_PM_Peak', 'DB_Evening','DB_Total']].sum()
            # sum_per_route_db = subset_df[['DB_PRE_Early_AM_Peak','DB_Early_AM_Peak','DB_AM_Peak', 'DB_Midday', 'DB_PM_Peak', 'DB_Evening','DB_Total']].sum()

            # route_level_df.loc[index,'CR_PRE_Early_AM']=sum_per_route_cr['CR_PRE_Early_AM']
            # route_level_df.loc[index,'CR_Early_AM']=sum_per_route_cr['CR_Early_AM']
            route_level_df.loc[index,'CR_AM_Peak']=sum_per_route_cr['CR_AM_Peak']
            route_level_df.loc[index,'CR_Midday']=sum_per_route_cr['CR_Midday']
            route_level_df.loc[index,'CR_PM_Peak']=sum_per_route_cr['CR_PM_Peak']
            route_level_df.loc[index,'CR_Evening']=sum_per_route_cr['CR_Evening']
            route_level_df.loc[index,'CR_Total']=sum_per_route_cr['CR_Total']
            
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
            route_level_df.loc[index, 'Total_DIFFERENCE'] =math.ceil(max(0, am_peak_diff))+math.ceil(max(0, midday_diff))+math.ceil(max(0, pm_peak_diff))+math.ceil(max(0, evening_diff))
            # route_level_df.loc[index, 'Total_DIFFERENCE'] =math.ceil(max(0, pre_early_am_peak_diff))+math.ceil(max(0, early_am_peak_diff))+math.ceil(max(0, am_peak_diff))+math.ceil(max(0, midday_diff))+math.ceil(max(0, pm_peak_diff))+math.ceil(max(0, evening_diff))
            route_level_df.loc[index, 'Overall_Goal_DIFFERENCE'] = math.ceil(max(0,overall_difference))
    elif project=='VTA':
        pre_early_am_values=['AM1'] 
        early_am_values=['AM2'] 
        am_values=['AM3','AM4','MID1','MID2','MID7'] 
        midday_values=['MID3','MID4','MID5','MID6','PM1']
        pm_values=['PM2','PM3','PM4','PM5']
        evening_values=['PM6','PM7','PM8','PM9']

        pre_early_am_column=[0]  #0 is for Pre-Early AM header
        early_am_column=[1]  #1 is for Early AM header
        am_column=[2] #This is for AM header
        midday_colum=[3] #this is for MIDDAY header
        pm_column=[4] #this is for PM header
        evening_column=[5] #this is for EVENING header

        def convert_string_to_integer(x):
            try:
                return float(x)
            except (ValueError, TypeError):
                return 0

        # Creating new dataframe for specifically AM, PM, MIDDAY, Evenving data and added values from Compeletion Report
        new_df=pd.DataFrame()
        new_df['ROUTE_SURVEYEDCode']=overall_df['LS_NAME_CODE']
        new_df['CR_PRE_Early_AM']=overall_df[pre_early_am_column[0]].apply(math.ceil)
        new_df['CR_Early_AM']=overall_df[early_am_column[0]].apply(math.ceil)
        new_df['CR_AM_Peak']=overall_df[am_column[0]].apply(math.ceil)
        new_df['CR_Midday']=overall_df[midday_colum[0]].apply(math.ceil)
        new_df['CR_PM_Peak']=overall_df[pm_column[0]].apply(math.ceil)
        new_df['CR_Evening']=overall_df[evening_column[0]].apply(math.ceil)
        print("new_df_columns",new_df.columns)
        new_df[['CR_PRE_Early_AM','CR_Early_AM','CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']]=new_df[['CR_PRE_Early_AM','CR_Early_AM','CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']].applymap(convert_string_to_integer)
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
            pre_early_am_value, pre_early_am_value_ids = get_counts_and_ids(pre_early_am_values)
            early_am_value, early_am_value_ids = get_counts_and_ids(early_am_values)
            am_value, am_value_ids = get_counts_and_ids(am_values)
            midday_value, midday_value_ids = get_counts_and_ids(midday_values)
            pm_value, pm_value_ids = get_counts_and_ids(pm_values)
            evening_value, evening_value_ids = get_counts_and_ids(evening_values)
            
            # Assign values to new_df
            # new_df.loc[index, 'CR_Total'] = row['CR_EARLY_AM'] + row['CR_AM_Peak'] + row['CR_Midday'] + row['CR_PM_Peak'] + row['CR_Evening']
            new_df.loc[index, 'CR_Total'] = row['CR_PRE_Early_AM']+row['CR_Early_AM']+row['CR_AM_Peak'] + row['CR_Midday'] + row['CR_PM_Peak'] + row['CR_Evening']
            new_df.loc[index, 'CR_AM_Peak'] =row['CR_AM_Peak']
            # new_df.loc[index, 'CR_AM_Peak'] =row['CR_PRE_EARLY_AM']+row['CR_EARLY_AM']+ row['CR_AM_Peak']
            new_df.loc[index, 'DB_PRE_Early_AM_Peak'] = pre_early_am_value
            new_df.loc[index, 'DB_Early_AM_Peak'] = early_am_value
            new_df.loc[index, 'DB_AM_Peak'] = am_value
            new_df.loc[index, 'DB_Midday'] = midday_value
            new_df.loc[index, 'DB_PM_Peak'] = pm_value
            new_df.loc[index, 'DB_Evening'] = evening_value
            new_df.loc[index, 'DB_Total'] = evening_value + am_value + midday_value + pm_value+pre_early_am_value+early_am_value
            
            # Join the IDs as a comma-separated string
            new_df.loc[index, 'DB_PRE_Early_AM_IDS'] = ', '.join(map(str, pre_early_am_value_ids))
            new_df.loc[index, 'DB_Early_AM_IDS'] = ', '.join(map(str, early_am_value_ids))
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
            


            sum_per_route_cr = subset_df[['CR_PRE_Early_AM','CR_Early_AM','CR_AM_Peak', 'CR_Midday', 'CR_PM_Peak', 'CR_Evening','CR_Total']].sum()
            # sum_per_route_cr = subset_df[['CR_EARLY_AM','CR_AM_Peak', 'CR_Midday', 'CR_PM_Peak', 'CR_Evening','CR_Total']].sum()
            sum_per_route_db = subset_df[['DB_PRE_Early_AM_Peak','DB_Early_AM_Peak','DB_AM_Peak', 'DB_Midday', 'DB_PM_Peak', 'DB_Evening','DB_Total']].sum()
            
            route_level_df.loc[index,'CR_PRE_Early_AM']=sum_per_route_cr['CR_PRE_Early_AM']
            route_level_df.loc[index,'CR_Early_AM']=sum_per_route_cr['CR_Early_AM']
            route_level_df.loc[index,'CR_AM_Peak']=sum_per_route_cr['CR_AM_Peak']
            route_level_df.loc[index,'CR_Midday']=sum_per_route_cr['CR_Midday']
            route_level_df.loc[index,'CR_PM_Peak']=sum_per_route_cr['CR_PM_Peak']
            route_level_df.loc[index,'CR_Evening']=sum_per_route_cr['CR_Evening']
            route_level_df.loc[index,'CR_Total']=sum_per_route_cr['CR_Total']
            # route_level_df.loc[index,'CR_Overall_Goal']=sum_per_route_cr['Overall Goal']
            
            route_level_df.loc[index,'DB_PRE_Early_AM_Peak']=sum_per_route_db['DB_PRE_Early_AM_Peak']
            route_level_df.loc[index,'DB_Early_AM_Peak']=sum_per_route_db['DB_Early_AM_Peak']
            route_level_df.loc[index,'DB_AM_Peak']=sum_per_route_db['DB_AM_Peak']
            route_level_df.loc[index,'DB_Midday']=sum_per_route_db['DB_Midday']
            route_level_df.loc[index,'DB_PM_Peak']=sum_per_route_db['DB_PM_Peak']
            route_level_df.loc[index,'DB_Evening']=sum_per_route_db['DB_Evening']
            route_level_df.loc[index,'DB_Total']=sum_per_route_db['DB_Total']   
            route_level_df.loc[index,'DB_PRE_Early_AM_IDS']=', '.join(str(value) for value in subset_df['DB_PRE_Early_AM_IDS'].values)    
            route_level_df.loc[index,'DB_Early_AM_IDS']=', '.join(str(value) for value in subset_df['DB_Early_AM_IDS'].values)    
            route_level_df.loc[index,'DB_AM_IDS']=', '.join(str(value) for value in subset_df['DB_AM_IDS'].values)    
            route_level_df.loc[index,'DB_Midday_IDS']=', '.join(str(value) for value in subset_df['DB_Midday_IDS'].values)    
            route_level_df.loc[index,'DB_PM_IDS']=', '.join(str(value) for value in subset_df['DB_PM_IDS'].values)    
            route_level_df.loc[index,'DB_Evening_IDS']=', '.join(str(value) for value in subset_df['DB_Evening_IDS'].values)

        # route_level_df.to_csv('Route Level Comparison(Value_Check).csv',index=False)
            
        # calculating the difference between values of database and compeletion report for Route_Level
        for index, row in route_level_df.iterrows():
            pre_early_am_peak_diff=row['CR_PRE_Early_AM']-row['DB_PRE_Early_AM_Peak']
            early_am_peak_diff=row['CR_Early_AM']-row['DB_Early_AM_Peak']
            am_peak_diff=row['CR_AM_Peak']-row['DB_AM_Peak']
            midday_diff=row['CR_Midday']-row['DB_Midday']    
            pm_peak_diff=row['CR_PM_Peak']-row['DB_PM_Peak']
            evening_diff=row['CR_Evening']-row['DB_Evening']
            total_diff=row['CR_Total']-row['DB_Total']
            overall_difference=row['CR_Overall_Goal']-row['DB_Total']
            route_level_df.loc[index, 'PRE_Early_AM_DIFFERENCE'] = math.ceil(max(0, pre_early_am_peak_diff))
            route_level_df.loc[index, 'Early_AM_DIFFERENCE'] = math.ceil(max(0, early_am_peak_diff))
            route_level_df.loc[index, 'AM_DIFFERENCE'] = math.ceil(max(0, am_peak_diff))
            route_level_df.loc[index, 'Midday_DIFFERENCE'] = math.ceil(max(0, midday_diff))
            route_level_df.loc[index, 'PM_DIFFERENCE'] = math.ceil(max(0, pm_peak_diff))
            route_level_df.loc[index, 'Evening_DIFFERENCE'] = math.ceil(max(0, evening_diff))
            # route_level_df.loc[index, 'Total_DIFFERENCE'] = math.ceil(max(0, total_diff))
            route_level_df.loc[index, 'Total_DIFFERENCE'] =math.ceil(max(0, pre_early_am_peak_diff))+math.ceil(max(0, early_am_peak_diff))+math.ceil(max(0, am_peak_diff))+math.ceil(max(0, midday_diff))+math.ceil(max(0, pm_peak_diff))+math.ceil(max(0, evening_diff))
            route_level_df.loc[index, 'Overall_Goal_DIFFERENCE'] = math.ceil(max(0,overall_difference))
            
    elif project=='UTA':
        pre_early_am_values = [1]
        early_am_values = [2]
        am_values = [3, 4, 5, 6]
        midday_values = [7, 8, 9, 10, 11]
        pm_values = [12, 13, 14]
        evening_values = [15, 16, 17, 18]

        pre_early_am_column=[0]  #0 is for Pre-Early AM header
        early_am_column=[1]  #1 is for Early AM header
        am_column=[2] #This is for AM header
        midday_column=[3] #this is for MIDDAY header
        pm_column=[4] #this is for PM header
        evening_column=[5] #this is for EVENING header

        def convert_string_to_integer(x):
            try:
                return float(x)
            except (ValueError, TypeError):
                return 0

        # Creating new dataframe for specifically AM, PM, MIDDAY, Evenving data and added values from Compeletion Report
        new_df=pd.DataFrame()
        new_df['ROUTE_SURVEYEDCode']=overall_df['LS_NAME_CODE']
        new_df['CR_PRE_Early_AM'] = pd.to_numeric(overall_df[pre_early_am_column[0]], errors='coerce').fillna(0).apply(math.ceil)
        new_df['CR_Early_AM'] = pd.to_numeric(overall_df[early_am_column[0]], errors='coerce').fillna(0).apply(math.ceil)
        new_df['CR_AM_Peak'] = pd.to_numeric(overall_df[am_column[0]], errors='coerce').fillna(0).apply(math.ceil)
        new_df['CR_Midday'] = pd.to_numeric(overall_df[midday_column[0]], errors='coerce').fillna(0).apply(math.ceil)
        new_df['CR_PM_Peak'] = pd.to_numeric(overall_df[pm_column[0]], errors='coerce').fillna(0).apply(math.ceil)
        new_df['CR_Evening'] = pd.to_numeric(overall_df[evening_column[0]], errors='coerce').fillna(0).apply(math.ceil)
        new_df[['CR_PRE_Early_AM','CR_Early_AM','CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']]=new_df[['CR_PRE_Early_AM','CR_Early_AM','CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']].applymap(convert_string_to_integer)
        new_df.fillna(0,inplace=True)
        #  new code added for merging the same ROUTE_SURVEYEDCode 
        new_df = new_df.groupby('ROUTE_SURVEYEDCode', as_index=False).sum()
        new_df.reset_index(drop=True, inplace=True)

        # adding values for AM, PM, MIDDAY and Evening from Database file to new Dataframe
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

            pre_early_am_value, pre_early_am_value_ids = get_counts_and_ids(pre_early_am_values)
            early_am_value, early_am_value_ids = get_counts_and_ids(early_am_values)
            am_value, am_value_ids = get_counts_and_ids(am_values)
            midday_value, midday_value_ids = get_counts_and_ids(midday_values)
            pm_value, pm_value_ids = get_counts_and_ids(pm_values)
            evening_value, evening_value_ids = get_counts_and_ids(evening_values)
        #     print(pre_early_am_value,early_am_value,am_value,midday_value,pm_value,evening_value)
            new_df.loc[index, 'CR_Total'] = row['CR_PRE_Early_AM']+row['CR_Early_AM']+row['CR_AM_Peak'] + row['CR_Midday'] + row['CR_PM_Peak'] + row['CR_Evening']
            new_df.loc[index, 'CR_AM_Peak'] =row['CR_AM_Peak']
            # new_df.loc[index, 'CR_AM_Peak'] =row['CR_PRE_EARLY_AM']+row['CR_EARLY_AM']+ row['CR_AM_Peak']
            new_df.loc[index, 'DB_PRE_Early_AM_Peak'] = pre_early_am_value
            new_df.loc[index, 'DB_Early_AM_Peak'] = early_am_value
            new_df.loc[index, 'DB_AM_Peak'] = am_value
            new_df.loc[index, 'DB_Midday'] = midday_value
            new_df.loc[index, 'DB_PM_Peak'] = pm_value
            new_df.loc[index, 'DB_Evening'] = evening_value
            new_df.loc[index, 'DB_Total'] = evening_value + am_value + midday_value + pm_value+pre_early_am_value+early_am_value

            
            # # Join the IDs as a comma-separated string
            new_df.loc[index, 'DB_PRE_Early_AM_IDS'] = ', '.join(map(str, pre_early_am_value_ids))
            new_df.loc[index, 'DB_Early_AM_IDS'] = ', '.join(map(str, early_am_value_ids))
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

        # Have to change the name accordingly
        route_df.rename(columns={'ROUTE_TOTAL':'CR_Overall_Goal','ETC_ROUTE_ID':'ROUTE_SURVEYEDCode'},inplace=True)

        route_df.dropna(subset=['ROUTE_SURVEYEDCode'],inplace=True)
        route_level_df=pd.merge(route_level_df,route_df[['ROUTE_SURVEYEDCode','CR_Overall_Goal']],on='ROUTE_SURVEYEDCode')

        route_level_df=route_level_df.groupby('ROUTE_SURVEYEDCode', as_index=False).sum()
        route_level_df.reset_index(drop=True, inplace=True)

        # adding values from database file and compeletion report for Route_Level
        for index , row in route_level_df.iterrows():
            subset_df=new_df[new_df['ROUTE_SURVEYEDCode_Splited']==row['ROUTE_SURVEYEDCode']]
            # sum_per_route_cr = subset_df[['CR_AM_Peak', 'CR_Midday', 'CR_PM_Peak', 'CR_Evening','CR_Total','Overall Goal']].sum()



            sum_per_route_cr = subset_df[['CR_PRE_Early_AM','CR_Early_AM','CR_AM_Peak', 'CR_Midday', 'CR_PM_Peak', 'CR_Evening','CR_Total']].sum()
            # sum_per_route_cr = subset_df[['CR_EARLY_AM','CR_AM_Peak', 'CR_Midday', 'CR_PM_Peak', 'CR_Evening','CR_Total']].sum()
            sum_per_route_db = subset_df[['DB_PRE_Early_AM_Peak','DB_Early_AM_Peak','DB_AM_Peak', 'DB_Midday', 'DB_PM_Peak', 'DB_Evening','DB_Total']].sum()

            route_level_df.loc[index,'CR_PRE_Early_AM']=sum_per_route_cr['CR_PRE_Early_AM']
            route_level_df.loc[index,'CR_Early_AM']=sum_per_route_cr['CR_Early_AM']
            route_level_df.loc[index,'CR_AM_Peak']=sum_per_route_cr['CR_AM_Peak']
            route_level_df.loc[index,'CR_Midday']=sum_per_route_cr['CR_Midday']
            route_level_df.loc[index,'CR_PM_Peak']=sum_per_route_cr['CR_PM_Peak']
            route_level_df.loc[index,'CR_Evening']=sum_per_route_cr['CR_Evening']
            route_level_df.loc[index,'CR_Total']=sum_per_route_cr['CR_Total']
            
            route_level_df.loc[index,'DB_PRE_Early_AM_Peak']=sum_per_route_db['DB_PRE_Early_AM_Peak']
            route_level_df.loc[index,'DB_Early_AM_Peak']=sum_per_route_db['DB_Early_AM_Peak']
            route_level_df.loc[index,'DB_AM_Peak']=sum_per_route_db['DB_AM_Peak']
            route_level_df.loc[index,'DB_Midday']=sum_per_route_db['DB_Midday']
            route_level_df.loc[index,'DB_PM_Peak']=sum_per_route_db['DB_PM_Peak']
            route_level_df.loc[index,'DB_Evening']=sum_per_route_db['DB_Evening']
            route_level_df.loc[index,'DB_Total']=sum_per_route_db['DB_Total']   
            route_level_df.loc[index,'DB_PRE_Early_AM_IDS']=', '.join(str(value) for value in subset_df['DB_PRE_Early_AM_IDS'].values)    
            route_level_df.loc[index,'DB_Early_AM_IDS']=', '.join(str(value) for value in subset_df['DB_Early_AM_IDS'].values)    
            route_level_df.loc[index,'DB_AM_IDS']=', '.join(str(value) for value in subset_df['DB_AM_IDS'].values)    
            route_level_df.loc[index,'DB_Midday_IDS']=', '.join(str(value) for value in subset_df['DB_Midday_IDS'].values)    
            route_level_df.loc[index,'DB_PM_IDS']=', '.join(str(value) for value in subset_df['DB_PM_IDS'].values)    
            route_level_df.loc[index,'DB_Evening_IDS']=', '.join(str(value) for value in subset_df['DB_Evening_IDS'].values)

        # route_level_df.to_csv('Route Level Comparison(Value_Check).csv',index=False)
            
        # calculating the difference between values of database and compeletion report for Route_Level
        for index, row in route_level_df.iterrows():
            pre_early_am_peak_diff=row['CR_PRE_Early_AM']-row['DB_PRE_Early_AM_Peak']
            early_am_peak_diff=row['CR_Early_AM']-row['DB_Early_AM_Peak']
            am_peak_diff=row['CR_AM_Peak']-row['DB_AM_Peak']
            midday_diff=row['CR_Midday']-row['DB_Midday']    
            pm_peak_diff=row['CR_PM_Peak']-row['DB_PM_Peak']
            evening_diff=row['CR_Evening']-row['DB_Evening']
            total_diff=row['CR_Total']-row['DB_Total']
            overall_difference=row['CR_Overall_Goal']-row['DB_Total']
            route_level_df.loc[index, 'PRE_Early_AM_DIFFERENCE'] = math.ceil(max(0, pre_early_am_peak_diff))
            route_level_df.loc[index, 'Early_AM_DIFFERENCE'] = math.ceil(max(0, early_am_peak_diff))
            route_level_df.loc[index, 'AM_DIFFERENCE'] = math.ceil(max(0, am_peak_diff))
            route_level_df.loc[index, 'Midday_DIFFERENCE'] = math.ceil(max(0, midday_diff))
            route_level_df.loc[index, 'PM_DIFFERENCE'] = math.ceil(max(0, pm_peak_diff))
            route_level_df.loc[index, 'Evening_DIFFERENCE'] = math.ceil(max(0, evening_diff))
            # route_level_df.loc[index, 'Total_DIFFERENCE'] = math.ceil(max(0, total_diff))
            route_level_df.loc[index, 'Total_DIFFERENCE'] =math.ceil(max(0, pre_early_am_peak_diff))+math.ceil(max(0, early_am_peak_diff))+math.ceil(max(0, am_peak_diff))+math.ceil(max(0, midday_diff))+math.ceil(max(0, pm_peak_diff))+math.ceil(max(0, evening_diff))
            route_level_df.loc[index, 'Overall_Goal_DIFFERENCE'] = math.ceil(max(0,overall_difference))
    elif project=='STL':
        am_values = ['AM1','AM2','AM3', 'AM4']
        midday_values = ['MID1','MID2', 'MID3', 'MID4', 'MID5']
        pm_values = ['PM1','PM2','PM3','PM4','PM5']
        evening_values = ['EVE1','EVE2','EVE3','EVE4']

        am_column = [1]
        midday_colum = [2]
        pm_column = [3]
        evening_column = [4]

        def convert_string_to_integer(x):
            try:
                return float(x)
            except (ValueError, TypeError):
                return 0

        # Creating new dataframe
        new_df = pd.DataFrame()
        new_df['ROUTE_SURVEYEDCode'] = overall_df['LS_NAME_CODE']
        
        # Create columns with consistent naming
        new_df['CR_AM_Peak'] = pd.to_numeric(overall_df[am_column[0]], errors='coerce').fillna(0).apply(math.ceil)
        new_df['CR_Midday'] = pd.to_numeric(overall_df[midday_colum[0]], errors='coerce').fillna(0).apply(math.ceil)
        new_df['CR_PM'] = pd.to_numeric(overall_df[pm_column[0]], errors='coerce').fillna(0).apply(math.ceil)
        new_df['CR_Evening'] = pd.to_numeric(overall_df[evening_column[0]], errors='coerce').fillna(0).apply(math.ceil)

        
        new_df[['CR_AM_Peak','CR_Midday','CR_PM','CR_Evening']] =new_df[['CR_AM_Peak','CR_Midday','CR_PM','CR_Evening']].applymap(convert_string_to_integer)
        new_df.fillna(0, inplace=True)

        # Define time_column - replace with your actual column name
    #     time_column = ['Time_Period']  # Adjust this to match your df column name containing AM1, AM2, etc.

        # Populate DB columns
        for index, row in new_df.iterrows():
            route_code = row['ROUTE_SURVEYEDCode']

            def get_counts_and_ids(time_values):
                subset_df = df[(df['ROUTE_SURVEYEDCode'] == route_code) & (df[time_column[0]].isin(time_values))]
                subset_df = subset_df.drop_duplicates(subset='id')
                count = subset_df.shape[0]
                ids = subset_df['id'].values
                return count, ids
            
            am_value, _ = get_counts_and_ids(am_values)
            midday_value, _ = get_counts_and_ids(midday_values)
            pm_value, _ = get_counts_and_ids(pm_values)
            evening_value, _ = get_counts_and_ids(evening_values)
            
            # Use consistent column names
            new_df.loc[index, 'CR_Total'] = ( row['CR_AM_Peak'] + row['CR_Midday'] +row['CR_PM'] + row['CR_Evening'])
            # new_df.loc[index, 'CR_AM_Peak'] = row['CR_AM_Peak']


            new_df.loc[index, 'DB_AM_Peak'] = am_value
            new_df.loc[index, 'DB_Midday'] = midday_value
            new_df.loc[index, 'DB_PM'] = pm_value
            new_df.loc[index, 'DB_Evening'] = evening_value
            new_df.loc[index, 'DB_Total'] = (evening_value + am_value + midday_value + pm_value)

        # Route Level Comparison
        new_df['ROUTE_SURVEYEDCode_Splited'] = new_df['ROUTE_SURVEYEDCode'].apply(lambda x: '_'.join(x.split('_')[:-1]))
        new_df.to_csv('W')
        # Create route_level_df
        route_level_df = pd.DataFrame()
        unique_routes = new_df['ROUTE_SURVEYEDCode_Splited'].unique()
        route_level_df['ROUTE_SURVEYEDCode'] = unique_routes

        route_df.rename(columns={'ROUTE_TOTAL':'CR_Overall_Goal','SURVEY_ROUTE_CODE':'ROUTE_SURVEYEDCode','LS_NAME_CODE':'ROUTE_SURVEYEDCode'}, inplace=True)
        route_df.dropna(subset=['ROUTE_SURVEYEDCode'], inplace=True)
        route_level_df = pd.merge(route_level_df, route_df[['ROUTE_SURVEYEDCode','CR_Overall_Goal']], on='ROUTE_SURVEYEDCode')

        # Populate route-level sums
        for index, row in route_level_df.iterrows():
            subset_df = new_df[new_df['ROUTE_SURVEYEDCode_Splited'] == row['ROUTE_SURVEYEDCode']]
            sum_per_route_cr = subset_df[['CR_AM_Peak', 'CR_Midday', 'CR_PM', 'CR_Evening', 'CR_Total']].sum()
            sum_per_route_db = subset_df[['DB_AM_Peak', 'DB_Midday', 'DB_PM', 'DB_Evening', 'DB_Total']].sum()
            

            route_level_df.loc[index,'CR_AM_Peak'] = sum_per_route_cr['CR_AM_Peak']
            route_level_df.loc[index,'CR_Midday'] = sum_per_route_cr['CR_Midday']
            route_level_df.loc[index,'CR_PM'] = sum_per_route_cr['CR_PM']
            route_level_df.loc[index,'CR_Evening'] = sum_per_route_cr['CR_Evening']
            route_level_df.loc[index,'CR_Total'] = sum_per_route_cr['CR_Total']
            

            route_level_df.loc[index,'DB_AM_Peak'] = sum_per_route_db['DB_AM_Peak']
            route_level_df.loc[index,'DB_Midday'] = sum_per_route_db['DB_Midday']
            route_level_df.loc[index,'DB_PM'] = sum_per_route_db['DB_PM']
            route_level_df.loc[index,'DB_Evening'] = sum_per_route_db['DB_Evening']
            route_level_df.loc[index,'DB_Total'] = sum_per_route_db['DB_Total']

        # Calculate differences
        for index, row in route_level_df.iterrows():

            am_peak_diff = row['CR_AM_Peak'] - row['DB_AM_Peak']
            midday_diff = row['CR_Midday'] - row['DB_Midday']
            pm_diff = row['CR_PM'] - row['DB_PM']
            evening_diff = row['CR_Evening'] - row['DB_Evening']
            total_diff = row['CR_Total'] - row['DB_Total']
            overall_difference = row['CR_Overall_Goal'] - row['DB_Total']
            
            route_level_df.loc[index, 'AM_DIFFERENCE'] = math.ceil(max(0, am_peak_diff))
            route_level_df.loc[index, 'Midday_DIFFERENCE'] = math.ceil(max(0, midday_diff))
            route_level_df.loc[index, 'PM_DIFFERENCE'] = math.ceil(max(0, pm_diff))
            route_level_df.loc[index, 'Evening_DIFFERENCE'] = math.ceil(max(0, evening_diff))
            route_level_df.loc[index, 'Total_DIFFERENCE'] = (math.ceil(max(0, am_peak_diff)) +math.ceil(max(0, midday_diff)) + math.ceil(max(0, pm_diff)) + math.ceil(max(0, evening_diff)))
            route_level_df.loc[index, 'Overall_Goal_DIFFERENCE'] = math.ceil(max(0, overall_difference))
    return route_level_df



def create_wkend_route_level_df(overall_df, route_df, df,time_column,project):
    am_values = ['AM1','AM2','AM3', 'AM4']
    midday_values = ['MID1', 'MID2','MID3', 'MID4', 'MID5', 'MID6']
    pm_values = ['PM1','PM2', 'PM3']
    evening_values = ['OFF1', 'OFF2', 'OFF3', 'OFF4','OFF5']
    am_column=[1]
    midday_colum=[2]
    pm_column=[3]
    evening_column=[4]

    def convert_string_to_integer(x):
        try:
            return float(x)
        except (ValueError, TypeError):
            return 0

    # Create separate dataframes for each day
    new_df = pd.DataFrame()
    new_df['ROUTE_SURVEYEDCode'] = overall_df['LS_NAME_CODE']
    new_df['Day'] = overall_df['DAY']
    new_df['CR_AM_Peak'] = overall_df[am_column[0]].apply(math.ceil)
    new_df['CR_Midday'] = overall_df[midday_colum[0]].apply(math.ceil)
    new_df['CR_PM_Peak'] = overall_df[pm_column[0]].apply(math.ceil)
    new_df['CR_Evening'] = overall_df[evening_column[0]].apply(math.ceil)

    new_df[['CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']] = new_df[['CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']].applymap(convert_string_to_integer)
    new_df.fillna(0, inplace=True)
#     new_df.to_csv("First Draft WeekEND.csv",index=False)
    for index, row in new_df.iterrows():
        route_code = row['ROUTE_SURVEYEDCode']
        day = row['Day']
        def get_counts_and_ids(time_values):
            # Filter by both route code AND day
            subset_df = df[(df['ROUTE_SURVEYEDCode'] == route_code) & 
                        (df[time_column[0]].isin(time_values)) & 
                        (df['Day'].str.lower() == str(day).lower())]
            print(route_code,day,subset_df.shape)
            subset_df = subset_df.drop_duplicates(subset='id')
            count = subset_df.shape[0]
            ids = subset_df['id'].values
            return count, ids
        am_value, am_value_ids = get_counts_and_ids(am_values)
        midday_value, midday_value_ids = get_counts_and_ids(midday_values)
        pm_value, pm_value_ids = get_counts_and_ids(pm_values)
        evening_value, evening_value_ids = get_counts_and_ids(evening_values)

        new_df.loc[index, 'CR_Total'] = row['CR_AM_Peak'] + row['CR_Midday'] + row['CR_PM_Peak'] + row['CR_Evening']
        new_df.loc[index, 'DB_AM_Peak'] = am_value
        new_df.loc[index, 'DB_Midday'] = midday_value
        new_df.loc[index, 'DB_PM_Peak'] = pm_value
        new_df.loc[index, 'DB_Evening'] = evening_value
        new_df.loc[index, 'DB_Total'] = evening_value + am_value + midday_value + pm_value

    new_df['ROUTE_SURVEYEDCode_Splited'] = new_df['ROUTE_SURVEYEDCode'].apply(lambda x: '_'.join(x.split('_')[:-1]))
    unique_route_days = new_df[['ROUTE_SURVEYEDCode_Splited', 'Day']].drop_duplicates()
    route_level_df = pd.DataFrame()
    # Create unique combinations of route and day
    unique_route_days = new_df[['ROUTE_SURVEYEDCode_Splited', 'Day']].drop_duplicates()
    route_level_df['ROUTE_SURVEYEDCode'] = new_df['ROUTE_SURVEYEDCode_Splited']
    route_level_df['Day'] = new_df['Day']

    route_df.rename(columns={'ROUTE_TOTAL':'CR_Overall_Goal','SURVEY_ROUTE_CODE':'ROUTE_SURVEYEDCode','LS_NAME_CODE':'ROUTE_SURVEYEDCode','DAY':'Day'}, inplace=True)
    route_df.dropna(subset=['ROUTE_SURVEYEDCode'], inplace=True)
    # route_level_df = pd.merge(route_level_df, route_df[['ROUTE_SURVEYEDCode','CR_Overall_Goal']], on='ROUTE_SURVEYEDCode')
    route_level_df.drop_duplicates(subset=['ROUTE_SURVEYEDCode','Day'],inplace=True)
    for _,row in route_level_df.iterrows():
        print(row['ROUTE_SURVEYEDCode'],row['Day'])
        subset_df = new_df[(new_df['ROUTE_SURVEYEDCode_Splited'] == row['ROUTE_SURVEYEDCode']) & 
                        (new_df['Day'] == row['Day'])]

        sum_per_route_cr = subset_df[['CR_AM_Peak', 'CR_Midday', 'CR_PM_Peak', 'CR_Evening','CR_Total']].sum()
        sum_per_route_db = subset_df[['DB_AM_Peak', 'DB_Midday', 'DB_PM_Peak', 'DB_Evening','DB_Total']].sum()
        print(sum_per_route_cr['CR_AM_Peak'])
        route_level_df.loc[row.name,'CR_AM_Peak'] = sum_per_route_cr['CR_AM_Peak']
        route_level_df.loc[row.name,'CR_Midday'] = sum_per_route_cr['CR_Midday']
        route_level_df.loc[row.name,'CR_PM_Peak'] = sum_per_route_cr['CR_PM_Peak']
        route_level_df.loc[row.name,'CR_Evening'] = sum_per_route_cr['CR_Evening']
        route_level_df.loc[row.name,'CR_Total'] = sum_per_route_cr['CR_Total']

        route_level_df.loc[row.name,'DB_AM_Peak'] = sum_per_route_db['DB_AM_Peak']
        route_level_df.loc[row.name,'DB_Midday'] = sum_per_route_db['DB_Midday']
        route_level_df.loc[row.name,'DB_PM_Peak'] = sum_per_route_db['DB_PM_Peak']
        route_level_df.loc[row.name,'DB_Evening'] = sum_per_route_db['DB_Evening']
        route_level_df.loc[row.name,'DB_Total'] = sum_per_route_db['DB_Total']
    route_level_df = pd.merge(route_level_df, route_df[['ROUTE_SURVEYEDCode', 'Day', 'CR_Overall_Goal']], 
                            on=['ROUTE_SURVEYEDCode', 'Day'])

    for _, row in route_level_df.iterrows():
        am_peak_diff = row['CR_AM_Peak'] - row['DB_AM_Peak']
        midday_diff = row['CR_Midday'] - row['DB_Midday']
        pm_peak_diff = row['CR_PM_Peak'] - row['DB_PM_Peak']
        evening_diff = row['CR_Evening'] - row['DB_Evening']
        total_diff = row['CR_Total'] - row['DB_Total']
        overall_difference = row['CR_Overall_Goal'] - row['DB_Total']

        route_level_df.loc[row.name, 'AM_DIFFERENCE'] = math.ceil(max(0, am_peak_diff))
        route_level_df.loc[row.name, 'Midday_DIFFERENCE'] = math.ceil(max(0, midday_diff))
        route_level_df.loc[row.name, 'PM_DIFFERENCE'] = math.ceil(max(0, pm_peak_diff))
        route_level_df.loc[row.name, 'Evening_DIFFERENCE'] = math.ceil(max(0, evening_diff))
        route_level_df.loc[row.name, 'Total_DIFFERENCE'] = math.ceil(max(0, am_peak_diff)) + math.ceil(max(0, midday_diff)) + math.ceil(max(0, pm_peak_diff)) + math.ceil(max(0, evening_diff))
        route_level_df.loc[row.name, 'Overall_Goal_DIFFERENCE'] = math.ceil(max(0, overall_difference))
    return route_level_df


def create_uta_route_direction_level_df(overalldf, df,time_column,time):
    pre_early_am_values = [1]
    early_am_values = [2]
    am_values = [3, 4, 5, 6]
    midday_values = [7, 8, 9, 10, 11]
    pm_values = [12, 13, 14]
    evening_values = [15, 16, 17, 18]

    pre_early_am_column = [0]  # 0 is for Pre-Early AM header
    early_am_column = [1]  # 1 is for Early AM header
    am_column = [2]  # This is for AM header
    midday_column = [3]  # this is for MIDDAY header
    pm_column = [4]  # this is for PM header
    evening_column = [5]  # this is for EVENING header

    def convert_string_to_integer(x):
        try:
            return float(x)
        except (ValueError, TypeError):
            return 0

    # Creating new dataframe for specifically AM, PM, MIDDAY, Evening data and added values from Completion Report
    new_df = pd.DataFrame()
    new_df['ROUTE_SURVEYEDCode']=overalldf['LS_NAME_CODE']
    if time=='weekend':
        new_df['Day'] = overalldf['DAY']
    
    new_df['STATION_ID']=overalldf['STATION_ID']
    new_df['STATION_ID_SPLITTED']=overalldf['STATION_ID_SPLITTED']
    new_df['CR_PRE_Early_AM'] = pd.to_numeric(overalldf[pre_early_am_column[0]], errors='coerce').fillna(0).apply(math.ceil)
    new_df['CR_Early_AM'] = pd.to_numeric(overalldf[early_am_column[0]], errors='coerce').fillna(0).apply(math.ceil)
    new_df['CR_AM_Peak'] = pd.to_numeric(overalldf[am_column[0]], errors='coerce').fillna(0).apply(math.ceil)
    new_df['CR_Midday'] = pd.to_numeric(overalldf[midday_column[0]], errors='coerce').fillna(0).apply(math.ceil)
    new_df['CR_PM_Peak'] = pd.to_numeric(overalldf[pm_column[0]], errors='coerce').fillna(0).apply(math.ceil)
    new_df['CR_Evening'] = pd.to_numeric(overalldf[evening_column[0]], errors='coerce').fillna(0).apply(math.ceil)
    # print("new_df_columns",new_df.columns)
    new_df[['CR_PRE_Early_AM','CR_Early_AM','CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']]=new_df[['CR_PRE_Early_AM','CR_Early_AM','CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']].applymap(convert_string_to_integer)
    new_df.fillna(0,inplace=True)
#     new code added for merging the same ROUTE_SURVEYEDCode
    # new_df=new_df.groupby('ROUTE_SURVEYEDCode', as_index=False).sum()
    # new_df.reset_index(drop=True, inplace=True)

    for index, row in new_df.iterrows():
        route_code = row['ROUTE_SURVEYEDCode']
        station_id=row['STATION_ID_SPLITTED']
        if time=='weekend':
            day=row['Day']
        def get_counts_and_ids(time_values):
            # Just for SALEM
            # subset_df = df[(df['ROUTE_SURVEYEDCode_Splited'] == route_code) & (df[time_column[0]].isin(time_values))]
            if time=='weekend':
                subset_df = df[(df['ROUTE_SURVEYEDCode'] == route_code)&(df['STATION_ID_SPLITTED']==station_id)  & (df[time_column[0]].isin(time_values))& 
                (df['Day'].str.lower() == str(day).lower())]
            else:
                subset_df = df[(df['ROUTE_SURVEYEDCode'] == route_code)&(df['STATION_ID_SPLITTED']==station_id)  & (df[time_column[0]].isin(time_values))]
            subset_df=subset_df.drop_duplicates(subset='id')
            count = subset_df.shape[0]
            ids = subset_df['id'].values
            return count, ids

        pre_early_am_value, pre_early_am_value_ids = get_counts_and_ids(pre_early_am_values)
        early_am_value, early_am_value_ids = get_counts_and_ids(early_am_values)
        am_value, am_value_ids = get_counts_and_ids(am_values)
        midday_value, midday_value_ids = get_counts_and_ids(midday_values)
        pm_value, pm_value_ids = get_counts_and_ids(pm_values)
        evening_value, evening_value_ids = get_counts_and_ids(evening_values)
        
        new_df.loc[index, 'CR_Total'] = row['CR_PRE_Early_AM']+row['CR_Early_AM']+row['CR_AM_Peak'] + row['CR_Midday'] + row['CR_PM_Peak'] + row['CR_Evening']
        new_df.loc[index, 'CR_AM_Peak'] =row['CR_AM_Peak']
        # new_df.loc[index, 'CR_AM_Peak'] =row['CR_PRE_EARLY_AM']+row['CR_EARLY_AM']+ row['CR_AM_Peak']
        new_df.loc[index, 'DB_PRE_Early_AM_Peak'] = pre_early_am_value
        new_df.loc[index, 'DB_Early_AM_Peak'] = early_am_value
        new_df.loc[index, 'DB_AM_Peak'] = am_value
        new_df.loc[index, 'DB_Midday'] = midday_value
        new_df.loc[index, 'DB_PM_Peak'] = pm_value
        new_df.loc[index, 'DB_Evening'] = evening_value
        new_df.loc[index, 'DB_Total'] = evening_value + am_value + midday_value + pm_value+pre_early_am_value+early_am_value
        
    #     new_df['ROUTE_SURVEYEDCode_Splited']=new_df['ROUTE_SURVEYEDCode'].apply(lambda x:('_').join(x.split('_')[:-1]) )
        route_code_level_df=pd.DataFrame()

        unique_routes=new_df['ROUTE_SURVEYEDCode'].unique()

        route_code_level_df['ROUTE_SURVEYEDCode']=unique_routes

        # weekend_df.rename(columns={'ROUTE_TOTAL':'CR_Overall_Goal','SURVEY_ROUTE_CODE':'ROUTE_SURVEYEDCode','LS_NAME_CODE':'ROUTE_SURVEYEDCode'},inplace=True)

        for index, row in new_df.iterrows():
            pre_early_am_peak_diff=row['CR_PRE_Early_AM']-row['DB_PRE_Early_AM_Peak']
            early_am_peak_diff=row['CR_Early_AM']-row['DB_Early_AM_Peak']
            am_peak_diff=row['CR_AM_Peak']-row['DB_AM_Peak']
            midday_diff=row['CR_Midday']-row['DB_Midday']    
            pm_peak_diff=row['CR_PM_Peak']-row['DB_PM_Peak']
            evening_diff=row['CR_Evening']-row['DB_Evening']
            total_diff=row['CR_Total']-row['DB_Total']
    #         overall_difference=row['CR_Overall_Goal']-row['DB_Total']
            new_df.loc[index, 'PRE_Early_AM_DIFFERENCE'] = math.ceil(max(0, pre_early_am_peak_diff))
            new_df.loc[index, 'Early_AM_DIFFERENCE'] = math.ceil(max(0, early_am_peak_diff))
            new_df.loc[index, 'AM_DIFFERENCE'] = math.ceil(max(0, am_peak_diff))
            new_df.loc[index, 'Midday_DIFFERENCE'] = math.ceil(max(0, midday_diff))
            new_df.loc[index, 'PM_DIFFERENCE'] = math.ceil(max(0, pm_peak_diff))
            new_df.loc[index, 'Evening_DIFFERENCE'] = math.ceil(max(0, evening_diff))
            # route_level_df.loc[index, 'Total_DIFFERENCE'] = math.ceil(max(0, total_diff))
            new_df.loc[index, 'Total_DIFFERENCE'] =math.ceil(max(0, pre_early_am_peak_diff))+math.ceil(max(0, early_am_peak_diff))+math.ceil(max(0, am_peak_diff))+math.ceil(max(0, midday_diff))+math.ceil(max(0, pm_peak_diff))+math.ceil(max(0, evening_diff))

    return new_df



