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
    elif project=='KCATA' or project=='ACTRANSIT':
        """
        Create a time-value DataFrame summarizing counts and time ranges.
        """
        early_am_values = ['AM1','AM2']
        am_values = ['AM3', 'MID1','MID2','MID7']
        midday_values = [ 'MID3', 'MID4', 'MID5', 'MID6','PM1']
        pm_peak_values = ['PM2','PM3','PM4','PM5']
        evening_values = ['PM6','PM7','PM8','PM9']

        # Mapping time groups to corresponding columns
        time_group_mapping = {
            1: early_am_values,
            2: am_values,
            3: midday_values,
            4: pm_peak_values,
            5: evening_values,
        }

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
        new_df = pd.DataFrame(columns=["Original Text", 0, 1, 2, 3, 4,5,6])

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
    elif project=='KCATA RAIL':
        # Filter df where overall_df['ROUTE_SURVEYEDCode'] == df['ROUTE_SURVEYEDCode_Splited']
        matched_df = df[df['ROUTE_SURVEYEDCode_Splited'].isin(overall_df['ROUTE_SURVEYEDCode'])]

        # Define time value groups
        early_am_values = ['AM1','AM2']
        am_values = ['AM3', 'MID1','MID2','MID7']
        midday_values = ['MID3', 'MID4', 'MID5', 'MID6','PM1']
        pm_peak_values = ['PM2','PM3','PM4','PM5']
        evening_values = ['PM6','PM7','PM8','PM9']

        # Mapping time groups to corresponding columns
        time_group_mapping = {
            1: early_am_values,
            2: am_values,
            3: midday_values,
            4: pm_peak_values,
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
            'PM8': 'After 9:00 pm',
            'PM9': 'After 9:00 pm'
        }

        # Initialize the new DataFrame
        new_df = pd.DataFrame(columns=["Original Text", 1, 2, 3, 4, 5])

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
    elif project=='KCATA' or project== 'ACTRANSIT':
        # Time period values
        early_am_values = ['AM1','AM2']
        am_values = ['AM3', 'MID1','MID2','MID7']
        midday_values = ['MID3', 'MID4', 'MID5', 'MID6','PM1']
        pm_peak_values = ['PM2','PM3','PM4','PM5']
        evening_values = ['PM6','PM7','PM8','PM9']

        # Column names in the DataFrame (as strings)
        early_am_column = ['1']  
        am_column = ['2']        
        midday_colum = ['3']     
        pm_peak_column = ['4']   
        evening_column = ['5']   

        # Initialize new DataFrame
        new_df = pd.DataFrame()
        new_df['ROUTE_SURVEYEDCode'] = overalldf['LS_NAME_CODE']

        # Convert and clean numeric columns
        def safe_convert(x):
            try:
                return math.ceil(float(x)) if pd.notnull(x) else 0
            except (ValueError, TypeError):
                return 0

        # Process each time period column
        for col, col_name in [(early_am_column, 'CR_Early_AM'),
                            (am_column, 'CR_AM_Peak'),
                            (midday_colum, 'CR_Midday'),
                            (pm_peak_column, 'CR_PM_Peak'),
                            (evening_column, 'CR_Evening')]:
            overalldf[col[0]] = pd.to_numeric(overalldf[col[0]], errors='coerce').fillna(0)
            new_df[col_name] = overalldf[col[0]].apply(safe_convert)

        # Calculate totals
        new_df['CR_Total'] = new_df['CR_Early_AM'] + new_df['CR_AM_Peak'] + new_df['CR_Midday'] + new_df['CR_PM_Peak'] + new_df['CR_Evening']

        # Get counts from the database
        for index, row in new_df.iterrows():
            route_code = row['ROUTE_SURVEYEDCode']

            def get_counts(time_values):
                subset_df = df[(df['ROUTE_SURVEYEDCode'] == route_code) & (df[time_column[0]].isin(time_values))]
                return subset_df.drop_duplicates(subset='id').shape[0]

            # Initialize all DB columns first
            new_df.at[index, 'DB_Early_AM_Peak'] = get_counts(early_am_values)
            new_df.at[index, 'DB_AM_Peak'] = get_counts(am_values)
            new_df.at[index, 'DB_Midday'] = get_counts(midday_values)
            new_df.at[index, 'DB_PM_Peak'] = get_counts(pm_peak_values)
            new_df.at[index, 'DB_Evening'] = get_counts(evening_values)
            new_df.at[index, 'DB_Total'] = (new_df.at[index, 'DB_Early_AM_Peak'] + 
                                            new_df.at[index, 'DB_AM_Peak'] + 
                                            new_df.at[index, 'DB_Midday'] + 
                                            new_df.at[index, 'DB_PM_Peak'] + 
                                            new_df.at[index, 'DB_Evening'])

        # Calculate differences
        new_df['Early_AM_DIFFERENCE'] = (new_df['CR_Early_AM'] - new_df['DB_Early_AM_Peak']).clip(lower=0).apply(math.ceil)
        new_df['AM_DIFFERENCE'] = (new_df['CR_AM_Peak'] - new_df['DB_AM_Peak']).clip(lower=0).apply(math.ceil)
        new_df['Midday_DIFFERENCE'] = (new_df['CR_Midday'] - new_df['DB_Midday']).clip(lower=0).apply(math.ceil)
        new_df['PM_PEAK_DIFFERENCE'] = (new_df['CR_PM_Peak'] - new_df['DB_PM_Peak']).clip(lower=0).apply(math.ceil)
        new_df['Evening_DIFFERENCE'] = (new_df['CR_Evening'] - new_df['DB_Evening']).clip(lower=0).apply(math.ceil)
        new_df['Total_DIFFERENCE'] = (new_df[['Early_AM_DIFFERENCE', 'AM_DIFFERENCE', 'Midday_DIFFERENCE', 
                                            'PM_PEAK_DIFFERENCE', 'Evening_DIFFERENCE']].sum(axis=1))
    elif project=='KCATA RAIL':
        early_am_values = ['AM1','AM2']
        am_values = ['AM3', 'MID1','MID2','MID7']
        midday_values = ['MID3', 'MID4', 'MID5', 'MID6','PM1']
        pm_peak_values = ['PM2','PM3','PM4','PM5']
        evening_values = ['PM6','PM7','PM8','PM9']

        # Updated column names to match your actual DataFrame columns
        time_period_columns = {
            'CR_Early_AM': 1,  
            'CR_AM_Peak': 2,        
            'CR_Midday': 3,     
            'CR_PM_Peak': 4,   
            'CR_Evening': 5   
        }

        def convert_string_to_integer(x):
            try:
                return float(x)
            except (ValueError, TypeError):
                return 0

        # Creating new dataframe with all required columns
        new_df = pd.DataFrame()
        new_df['ROUTE_SURVEYEDCode'] = overalldf['LS_NAME_CODE']
        new_df['STATION_ID'] = overalldf['STATION_ID']
        new_df['STATION_ID_SPLITTED'] = overalldf['STATION_ID_SPLITTED']
        
        # Process each time period column
        for col_name, col in time_period_columns.items():
            overalldf[col] = pd.to_numeric(overalldf[col], errors='coerce').fillna(0)
            new_df[col_name] = overalldf[col].apply(math.ceil)
        
        # Convert all numeric columns and fill NA
        numeric_cols = list(time_period_columns.keys())
        new_df[numeric_cols] = new_df[numeric_cols].applymap(convert_string_to_integer)
        new_df.fillna(0, inplace=True)

        for index, row in new_df.iterrows():
            route_code = row['ROUTE_SURVEYEDCode']
            station_id = row['STATION_ID_SPLITTED']

            def get_counts_and_ids(time_values):
                subset_df = df[(df['ROUTE_SURVEYEDCode'] == route_code) & 
                            (df['STATION_ID_SPLITTED'] == station_id) & 
                            (df[time_column[0]].isin(time_values))]
                subset_df = subset_df.drop_duplicates(subset='id')
                count = subset_df.shape[0]
                ids = subset_df['id'].values
                return count, ids

            # Get counts for each time period
            early_am_count, early_am_ids = get_counts_and_ids(early_am_values)
            am_count, am_ids = get_counts_and_ids(am_values)
            midday_count, midday_ids = get_counts_and_ids(midday_values)
            pm_peak_count, pm_peak_ids = get_counts_and_ids(pm_peak_values)
            evening_count, evening_ids = get_counts_and_ids(evening_values)
            
            # Calculate totals
            new_df.loc[index, 'CR_Total'] = sum(row[col] for col in numeric_cols)
            
            # Database counts
            new_df.loc[index, 'DB_Early_AM_Peak'] = early_am_count
            new_df.loc[index, 'DB_AM_Peak'] = am_count
            new_df.loc[index, 'DB_Midday'] = midday_count
            new_df.loc[index, 'DB_PM_Peak'] = pm_peak_count
            new_df.loc[index, 'DB_Evening'] = evening_count
            new_df.loc[index, 'DB_Total'] = sum([early_am_count, am_count, midday_count, 
                                            pm_peak_count, evening_count])
            
            # Calculate differences
            early_am_diff = row['CR_Early_AM'] - early_am_count
            am_diff = row['CR_AM_Peak'] - am_count
            midday_diff = row['CR_Midday'] - midday_count
            pm_peak_diff = row['CR_PM_Peak'] - pm_peak_count
            evening_diff = row['CR_Evening'] - evening_count
            
            new_df.loc[index, 'Early_AM_DIFFERENCE'] = math.ceil(max(0, early_am_diff))
            new_df.loc[index, 'AM_DIFFERENCE'] = math.ceil(max(0, am_diff))
            new_df.loc[index, 'Midday_DIFFERENCE'] = math.ceil(max(0, midday_diff))
            new_df.loc[index, 'PM_PEAK_DIFFERENCE'] = math.ceil(max(0, pm_peak_diff))
            new_df.loc[index, 'Evening_DIFFERENCE'] = math.ceil(max(0, evening_diff))
            new_df.loc[index, 'Total_DIFFERENCE'] = sum([
                math.ceil(max(0, early_am_diff)),
                math.ceil(max(0, am_diff)),
                math.ceil(max(0, midday_diff)),
                math.ceil(max(0, pm_peak_diff)),
                math.ceil(max(0, evening_diff))
            ])
    
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

def create_station_wise_route_level_df_kcata(overall_df, df, time_column):
    # Time period values (following KCATA structure)
    early_am_values = ['AM1','AM2']
    am_values = ['AM3', 'MID1','MID2','MID7']
    midday_values = ['MID3', 'MID4', 'MID5', 'MID6','PM1']
    pm_peak_values = ['PM2','PM3','PM4','PM5']
    evening_values = ['PM6','PM7','PM8','PM9']

    # Updated column mapping to match actual numeric columns in your DataFrame
    time_period_columns = {
        'CR_Early_AM': 1,  # Using 1 instead of '1'
        'CR_AM_Peak': 2,
        'CR_Midday': 3,
        'CR_PM_Peak': 4,
        'CR_Evening': 5
    }

    def convert_string_to_integer(x):
        try:
            return float(x)
        except (ValueError, TypeError):
            return 0

    # Creating new dataframe with all required columns
    new_df = pd.DataFrame()
    new_df['ROUTE_SURVEYEDCode'] = overall_df['LS_NAME_CODE']
    new_df['STATION_ID'] = overall_df['STATION_ID']
    new_df['STATION_ID_SPLITTED'] = overall_df['STATION_ID_SPLITTED']
    
    # Process each time period column using the numeric column references
    for col_name, col_num in time_period_columns.items():
        new_df[col_name] = pd.to_numeric(overall_df[col_num], errors='coerce').fillna(0).apply(math.ceil)
    
    # Convert all numeric columns and fill NA
    numeric_cols = list(time_period_columns.keys())
    new_df[numeric_cols] = new_df[numeric_cols].applymap(convert_string_to_integer)
    new_df.fillna(0, inplace=True)
    new_df['ROUTE_SURVEYEDCode_Splitted'] = new_df['ROUTE_SURVEYEDCode'].apply(edit_ls_code_column)
    
    for index, row in new_df.iterrows():
        route_code = row['ROUTE_SURVEYEDCode_Splitted']
        station_id = row['STATION_ID_SPLITTED']

        def get_counts_and_ids(time_values):
            subset_df = df[(df['ROUTE_SURVEYEDCode_Splited'] == route_code) & 
                          (df['STATION_ID_SPLITTED'] == station_id) & 
                          (df[time_column[0]].isin(time_values))]
            subset_df = subset_df.drop_duplicates(subset='id')
            count = subset_df.shape[0]
            ids = subset_df['id'].values
            return count, ids

        # Get counts for each time period
        early_am_count, early_am_ids = get_counts_and_ids(early_am_values)
        am_count, am_ids = get_counts_and_ids(am_values)
        midday_count, midday_ids = get_counts_and_ids(midday_values)
        pm_peak_count, pm_peak_ids = get_counts_and_ids(pm_peak_values)
        evening_count, evening_ids = get_counts_and_ids(evening_values)
        
        # Calculate totals
        new_df.loc[index, 'CR_Total'] = sum(row[col] for col in numeric_cols)
        
        # Database counts
        new_df.loc[index, 'DB_Early_AM_Peak'] = early_am_count
        new_df.loc[index, 'DB_AM_Peak'] = am_count
        new_df.loc[index, 'DB_Midday'] = midday_count
        new_df.loc[index, 'DB_PM_Peak'] = pm_peak_count
        new_df.loc[index, 'DB_Evening'] = evening_count
        new_df.loc[index, 'DB_Total'] = sum([early_am_count, am_count, midday_count, 
                                           pm_peak_count, evening_count])
    
    # Group by station and route
    results = []
    unique_station_ids = new_df['STATION_ID_SPLITTED'].unique()

    for station_id in unique_station_ids:
        station_df = new_df[new_df['STATION_ID_SPLITTED'] == station_id]
        
        for route_code in station_df['ROUTE_SURVEYEDCode_Splitted'].unique():
            filtered_df = station_df[station_df['ROUTE_SURVEYEDCode_Splitted'] == route_code]
            summed_row = filtered_df.sum(numeric_only=True).to_frame().T
            
            # Add key identifying columns
            summed_row['ROUTE_SURVEYEDCode'] = station_df.iloc[0]['ROUTE_SURVEYEDCode']
            summed_row['STATION_ID'] = station_df.iloc[0]['STATION_ID']
            summed_row['STATION_ID_SPLITTED'] = station_id
            summed_row['ROUTE_SURVEYEDCode_Splitted'] = route_code
            
            results.append(summed_row)

    # Create final dataframe
    route_station_wise = pd.concat(results, ignore_index=True)
    route_station_wise.drop(columns=['ROUTE_SURVEYEDCode_Splitted','STATION_ID_SPLITTED'], inplace=True)

    # Calculate differences
    for index, row in route_station_wise.iterrows():
        early_am_diff = row['CR_Early_AM'] - row['DB_Early_AM_Peak']
        am_diff = row['CR_AM_Peak'] - row['DB_AM_Peak']
        midday_diff = row['CR_Midday'] - row['DB_Midday']
        pm_peak_diff = row['CR_PM_Peak'] - row['DB_PM_Peak']
        evening_diff = row['CR_Evening'] - row['DB_Evening']
        
        route_station_wise.loc[index, 'Early_AM_DIFFERENCE'] = math.ceil(max(0, early_am_diff))
        route_station_wise.loc[index, 'AM_DIFFERENCE'] = math.ceil(max(0, am_diff))
        route_station_wise.loc[index, 'Midday_DIFFERENCE'] = math.ceil(max(0, midday_diff))
        route_station_wise.loc[index, 'PM_PEAK_DIFFERENCE'] = math.ceil(max(0, pm_peak_diff))
        route_station_wise.loc[index, 'Evening_DIFFERENCE'] = math.ceil(max(0, evening_diff))
        route_station_wise.loc[index, 'Total_DIFFERENCE'] = sum([
            math.ceil(max(0, early_am_diff)),
            math.ceil(max(0, am_diff)),
            math.ceil(max(0, midday_diff)),
            math.ceil(max(0, pm_peak_diff)),
            math.ceil(max(0, evening_diff))
        ])

    return route_station_wise

# import logging
# from datetime import datetime

# def setup_logging():
#     """Configure logging with file and console handlers"""
#     timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
#     log_filename = f"kcata_route_level_debug_{timestamp}.log"
    
#     # Clear any existing handlers
#     logging.getLogger().handlers = []
    
#     # Configure logging
#     logging.basicConfig(
#         level=logging.DEBUG,
#         format='%(asctime)s - %(levelname)s - %(message)s',
#         handlers=[
#             logging.FileHandler(log_filename, mode='w'),
#             logging.StreamHandler()
#         ]
#     )
#     logger = logging.getLogger()
    
#     # Print log file location for debugging
#     logger.info(f"Log file created at: {os.path.abspath(log_filename)}")
#     return logger

def create_route_level_df(overall_df,route_df,df,time_column,project):
    # logger = setup_logging()
    
    try:
        # logger.info("\n=== KCATA ROUTE LEVEL DEBUG STARTED ===")
        # logger.info(f"Initial survey data shape: {df.shape}")
        # logger.info(f"Unique route codes in survey data: {df['ROUTE_SURVEYEDCode'].nunique()}")
        # logger.info(f"Time column being used: {time_column[0]}")

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
        elif project=='KCATA' or project=='ACTRANSIT':
            early_am_values = ['AM1','AM2']
            am_values = ['AM3', 'MID1','MID2','MID7']
            midday_values = ['MID3', 'MID4', 'MID5', 'MID6','PM1']
            pm_peak_values = ['PM2','PM3','PM4','PM5']
            evening_values = ['PM6','PM7','PM8','PM9']

            # Column mappings for completion report
            early_am_column = ['1']
            am_column = ['2']
            midday_colum = ['3']
            pm_peak_column = ['4']
            evening_column = ['5']

            def convert_string_to_integer(x):
                """Safely convert values to integers"""
                try:
                    return float(x)
                except (ValueError, TypeError):
                    # logger.warning(f"Could not convert value to float: {x}")
                    return 0

            # Create new dataframe with completion report data
            new_df = pd.DataFrame()
            new_df['ROUTE_SURVEYEDCode'] = overall_df['LS_NAME_CODE']
            new_df['CR_Early_AM'] = overall_df[early_am_column[0]].apply(math.ceil)
            new_df['CR_AM_Peak'] = overall_df[am_column[0]].apply(math.ceil)
            new_df['CR_Midday'] = overall_df[midday_colum[0]].apply(math.ceil)
            new_df['CR_PM_Peak'] = overall_df[pm_peak_column[0]].apply(math.ceil)
            new_df['CR_Evening'] = overall_df[evening_column[0]].apply(math.ceil)
            
            # Convert all values to numeric
            new_df[['CR_Early_AM','CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']] = \
                new_df[['CR_Early_AM','CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']].applymap(convert_string_to_integer)
            
            new_df.fillna(0, inplace=True)
            # logger.info(f"Created new_df with shape: {new_df.shape}")

            # Populate database counts for each time period
            for index, row in new_df.iterrows():
                route_code = row['ROUTE_SURVEYEDCode']
                # logger.debug(f"\nProcessing route: {route_code}")

                def get_counts_and_ids(time_values, time_name):
                    """Get survey counts and IDs for a time period"""
                    subset_df = df[(df['ROUTE_SURVEYEDCode'] == route_code) & 
                                (df[time_column[0]].isin(time_values))]
                    original_count = subset_df.shape[0]
                    subset_df = subset_df.drop_duplicates(subset='id')
                    count = subset_df.shape[0]
                    ids = subset_df['id'].values
                    
                    # logger.debug(f"  {time_name} period - Found {count} surveys (from {original_count} raw)")
                    return count, ids
                
                # Get counts for each time period
                early_am_value, early_am_value_ids = get_counts_and_ids(early_am_values, "Early AM")
                am_value, am_value_ids = get_counts_and_ids(am_values, "AM Peak")
                midday_value, midday_value_ids = get_counts_and_ids(midday_values, "Midday")
                pm_peak_value, pm_peak_value_ids = get_counts_and_ids(pm_peak_values, "PM Peak")
                evening_value, evening_value_ids = get_counts_and_ids(evening_values, "Evening")
                
                # Calculate and store database totals
                db_total = evening_value + early_am_value + am_value + midday_value + pm_peak_value
                new_df.loc[index, 'CR_Total'] = row['CR_Early_AM'] + row['CR_AM_Peak'] + row['CR_Midday'] + row['CR_PM_Peak'] + row['CR_Evening']
                
                new_df.loc[index, 'DB_Early_AM_Peak'] = early_am_value
                new_df.loc[index, 'DB_AM_Peak'] = am_value
                new_df.loc[index, 'DB_Midday'] = midday_value
                new_df.loc[index, 'DB_PM_Peak'] = pm_peak_value
                new_df.loc[index, 'DB_Evening'] = evening_value
                new_df.loc[index, 'DB_Total'] = db_total

            # Create route-level summary
            new_df['ROUTE_SURVEYEDCode_Splited'] = new_df['ROUTE_SURVEYEDCode'].apply(lambda x:('_').join(x.split('_')[:-1]))
            # logger.info(f"\nAfter splitting route codes, unique routes: {new_df['ROUTE_SURVEYEDCode_Splited'].nunique()}")

            route_level_df = pd.DataFrame()
            unique_routes = new_df['ROUTE_SURVEYEDCode_Splited'].unique()
            route_level_df['ROUTE_SURVEYEDCode'] = unique_routes

            # Merge with route goals
            route_df.rename(columns={
                'ROUTE_TOTAL':'CR_Overall_Goal',
                'SURVEY_ROUTE_CODE':'ROUTE_SURVEYEDCode',
                'LS_NAME_CODE':'ROUTE_SURVEYEDCode'
            }, inplace=True)
            route_df.dropna(subset=['ROUTE_SURVEYEDCode'], inplace=True)
            
            route_level_df = pd.merge(
                route_level_df,
                route_df[['ROUTE_SURVEYEDCode','CR_Overall_Goal']],
                on='ROUTE_SURVEYEDCode'
            )

            # Aggregate counts by route
            for index, row in route_level_df.iterrows():
                route_code = row['ROUTE_SURVEYEDCode']
                subset_df = new_df[new_df['ROUTE_SURVEYEDCode_Splited'] == route_code]
                
                sum_per_route_cr = subset_df[['CR_Early_AM', 'CR_AM_Peak', 'CR_Midday', 'CR_PM_Peak', 'CR_Evening', 'CR_Total']].sum()
                sum_per_route_db = subset_df[['DB_Early_AM_Peak','DB_AM_Peak', 'DB_Midday', 'DB_PM_Peak', 'DB_Evening', 'DB_Total']].sum()

                # Store aggregated values
                route_level_df.loc[index,'CR_Early_AM'] = sum_per_route_cr['CR_Early_AM']
                route_level_df.loc[index,'CR_AM_Peak'] = sum_per_route_cr['CR_AM_Peak']
                route_level_df.loc[index,'CR_Midday'] = sum_per_route_cr['CR_Midday']
                route_level_df.loc[index,'CR_PM_Peak'] = sum_per_route_cr['CR_PM_Peak']
                route_level_df.loc[index,'CR_Evening'] = sum_per_route_cr['CR_Evening']
                route_level_df.loc[index,'CR_Total'] = sum_per_route_cr['CR_Total']
                
                route_level_df.loc[index,'DB_Early_AM_Peak'] = sum_per_route_db['DB_Early_AM_Peak']
                route_level_df.loc[index,'DB_AM_Peak'] = sum_per_route_db['DB_AM_Peak']
                route_level_df.loc[index,'DB_Midday'] = sum_per_route_db['DB_Midday']
                route_level_df.loc[index,'DB_PM_Peak'] = sum_per_route_db['DB_PM_Peak']
                route_level_df.loc[index,'DB_Evening'] = sum_per_route_db['DB_Evening']
                route_level_df.loc[index,'DB_Total'] = sum_per_route_db['DB_Total']

            # Calculate differences
            for index, row in route_level_df.iterrows():
                early_am_peak_diff = row['CR_Early_AM'] - row['DB_Early_AM_Peak']
                am_peak_diff = row['CR_AM_Peak'] - row['DB_AM_Peak']
                midday_diff = row['CR_Midday'] - row['DB_Midday']    
                pm_peak_diff = row['CR_PM_Peak'] - row['DB_PM_Peak']
                evening_diff = row['CR_Evening'] - row['DB_Evening']
                overall_difference = row['CR_Overall_Goal'] - row['DB_Total']
                
                route_level_df.loc[index, 'Early_AM_DIFFERENCE'] = math.ceil(max(0, early_am_peak_diff))
                route_level_df.loc[index, 'AM_DIFFERENCE'] = math.ceil(max(0, am_peak_diff))
                route_level_df.loc[index, 'Midday_DIFFERENCE'] = math.ceil(max(0, midday_diff))
                route_level_df.loc[index, 'PM_PEAK_DIFFERENCE'] = math.ceil(max(0, pm_peak_diff))
                route_level_df.loc[index, 'Evening_DIFFERENCE'] = math.ceil(max(0, evening_diff))
                route_level_df.loc[index, 'Total_DIFFERENCE'] = (
                    math.ceil(max(0, early_am_peak_diff)) + 
                    math.ceil(max(0, am_peak_diff)) + 
                    math.ceil(max(0, midday_diff)) + 
                    math.ceil(max(0, pm_peak_diff)) + 
                    math.ceil(max(0, evening_diff))
                )
                route_level_df.loc[index, 'Overall_Goal_DIFFERENCE'] = math.ceil(max(0, overall_difference))
        elif project=='KCATA RAIL':
            early_am_values = ['AM1','AM2']
            am_values = ['AM3', 'MID1','MID2','MID7']
            midday_values = ['MID3', 'MID4', 'MID5', 'MID6','PM1']
            pm_peak_values = ['PM2','PM3','PM4','PM5']
            evening_values = ['PM6','PM7','PM8','PM9']

            # Updated column mapping to match actual numeric columns in your DataFrame
            time_period_columns = {
                'CR_Early_AM': 1,  # Using 1 instead of '1'
                'CR_AM_Peak': 2,
                'CR_Midday': 3,
                'CR_PM_Peak': 4,
                'CR_Evening': 5
            }

            def convert_string_to_integer(x):
                try:
                    return float(x)
                except (ValueError, TypeError):
                    return 0

            # Creating new dataframe with all required columns
            new_df = pd.DataFrame()
            new_df['ROUTE_SURVEYEDCode'] = overall_df['LS_NAME_CODE']
            
            # Process each time period column using the numeric column references
            for col_name, col_num in time_period_columns.items():
                new_df[col_name] = pd.to_numeric(overall_df[col_num], errors='coerce').fillna(0).apply(math.ceil)
            
            # Convert all numeric columns and fill NA
            numeric_cols = list(time_period_columns.keys())
            new_df[numeric_cols] = new_df[numeric_cols].applymap(convert_string_to_integer)
            new_df.fillna(0, inplace=True)
            
            # Group by route code
            new_df = new_df.groupby('ROUTE_SURVEYEDCode', as_index=False).sum()
            new_df.reset_index(drop=True, inplace=True)

            # Get counts from database
            for index, row in new_df.iterrows():
                route_code = row['ROUTE_SURVEYEDCode']

                def get_counts_and_ids(time_values):
                    subset_df = df[(df['ROUTE_SURVEYEDCode'] == route_code) & 
                                (df[time_column[0]].isin(time_values))]
                    subset_df = subset_df.drop_duplicates(subset='id')
                    count = subset_df.shape[0]
                    ids = subset_df['id'].values
                    return count, ids

                # Get counts for each time period
                early_am_count, early_am_ids = get_counts_and_ids(early_am_values)
                am_count, am_ids = get_counts_and_ids(am_values)
                midday_count, midday_ids = get_counts_and_ids(midday_values)
                pm_peak_count, pm_peak_ids = get_counts_and_ids(pm_peak_values)
                evening_count, evening_ids = get_counts_and_ids(evening_values)
                
                # Calculate totals
                new_df.loc[index, 'CR_Total'] = sum(row[col] for col in numeric_cols)
                
                # Database counts
                new_df.loc[index, 'DB_Early_AM_Peak'] = early_am_count
                new_df.loc[index, 'DB_AM_Peak'] = am_count
                new_df.loc[index, 'DB_Midday'] = midday_count
                new_df.loc[index, 'DB_PM_Peak'] = pm_peak_count
                new_df.loc[index, 'DB_Evening'] = evening_count
                new_df.loc[index, 'DB_Total'] = sum([early_am_count, am_count, midday_count, 
                                                pm_peak_count, evening_count])
                
                # Join the IDs as comma-separated strings
                new_df.loc[index, 'DB_Early_AM_IDS'] = ', '.join(map(str, early_am_ids))
                new_df.loc[index, 'DB_AM_IDS'] = ', '.join(map(str, am_ids))
                new_df.loc[index, 'DB_Midday_IDS'] = ', '.join(map(str, midday_ids))
                new_df.loc[index, 'DB_PM_IDS'] = ', '.join(map(str, pm_peak_ids))
                new_df.loc[index, 'DB_Evening_IDS'] = ', '.join(map(str, evening_ids))

            # Create route level comparison
            new_df['ROUTE_SURVEYEDCode_Splited'] = new_df['ROUTE_SURVEYEDCode'].apply(lambda x: '_'.join(x.split('_')[:-1]))
            
            route_level_df = pd.DataFrame()
            unique_routes = new_df['ROUTE_SURVEYEDCode_Splited'].unique()
            route_level_df['ROUTE_SURVEYEDCode'] = unique_routes

            # Merge with route_df for overall goal
            route_df.rename(columns={'ROUTE_TOTAL':'CR_Overall_Goal','ETC_ROUTE_ID':'ROUTE_SURVEYEDCode'}, inplace=True)
            route_df.dropna(subset=['ROUTE_SURVEYEDCode'], inplace=True)
            route_level_df = pd.merge(route_level_df, route_df[['ROUTE_SURVEYEDCode','CR_Overall_Goal']], 
                                    on='ROUTE_SURVEYEDCode')
            route_level_df = route_level_df.groupby('ROUTE_SURVEYEDCode', as_index=False).sum()
            route_level_df.reset_index(drop=True, inplace=True)

            # Aggregate values for route level
            for index, row in route_level_df.iterrows():
                subset_df = new_df[new_df['ROUTE_SURVEYEDCode_Splited'] == row['ROUTE_SURVEYEDCode']]
                
                sum_per_route_cr = subset_df[['CR_Early_AM', 'CR_AM_Peak', 'CR_Midday', 
                                            'CR_PM_Peak', 'CR_Evening', 'CR_Total']].sum()
                sum_per_route_db = subset_df[['DB_Early_AM_Peak', 'DB_AM_Peak', 'DB_Midday', 
                                            'DB_PM_Peak', 'DB_Evening', 'DB_Total']].sum()

                route_level_df.loc[index, 'CR_Early_AM'] = sum_per_route_cr['CR_Early_AM']
                route_level_df.loc[index, 'CR_AM_Peak'] = sum_per_route_cr['CR_AM_Peak']
                route_level_df.loc[index, 'CR_Midday'] = sum_per_route_cr['CR_Midday']
                route_level_df.loc[index, 'CR_PM_Peak'] = sum_per_route_cr['CR_PM_Peak']
                route_level_df.loc[index, 'CR_Evening'] = sum_per_route_cr['CR_Evening']
                route_level_df.loc[index, 'CR_Total'] = sum_per_route_cr['CR_Total']
                
                route_level_df.loc[index, 'DB_Early_AM_Peak'] = sum_per_route_db['DB_Early_AM_Peak']
                route_level_df.loc[index, 'DB_AM_Peak'] = sum_per_route_db['DB_AM_Peak']
                route_level_df.loc[index, 'DB_Midday'] = sum_per_route_db['DB_Midday']
                route_level_df.loc[index, 'DB_PM_Peak'] = sum_per_route_db['DB_PM_Peak']
                route_level_df.loc[index, 'DB_Evening'] = sum_per_route_db['DB_Evening']
                route_level_df.loc[index, 'DB_Total'] = sum_per_route_db['DB_Total']
                
                # Combine IDs from all sub-routes
                route_level_df.loc[index, 'DB_Early_AM_IDS'] = ', '.join(str(value) for value in subset_df['DB_Early_AM_IDS'].values)
                route_level_df.loc[index, 'DB_AM_IDS'] = ', '.join(str(value) for value in subset_df['DB_AM_IDS'].values)
                route_level_df.loc[index, 'DB_Midday_IDS'] = ', '.join(str(value) for value in subset_df['DB_Midday_IDS'].values)
                route_level_df.loc[index, 'DB_PM_IDS'] = ', '.join(str(value) for value in subset_df['DB_PM_IDS'].values)
                route_level_df.loc[index, 'DB_Evening_IDS'] = ', '.join(str(value) for value in subset_df['DB_Evening_IDS'].values)

            # Calculate differences
            for index, row in route_level_df.iterrows():
                early_am_diff = row['CR_Early_AM'] - row['DB_Early_AM_Peak']
                am_diff = row['CR_AM_Peak'] - row['DB_AM_Peak']
                midday_diff = row['CR_Midday'] - row['DB_Midday']
                pm_peak_diff = row['CR_PM_Peak'] - row['DB_PM_Peak']
                evening_diff = row['CR_Evening'] - row['DB_Evening']
                total_diff = row['CR_Total'] - row['DB_Total']
                overall_diff = row['CR_Overall_Goal'] - row['DB_Total']
                
                route_level_df.loc[index, 'Early_AM_DIFFERENCE'] = math.ceil(max(0, early_am_diff))
                route_level_df.loc[index, 'AM_DIFFERENCE'] = math.ceil(max(0, am_diff))
                route_level_df.loc[index, 'Midday_DIFFERENCE'] = math.ceil(max(0, midday_diff))
                route_level_df.loc[index, 'PM_PEAK_DIFFERENCE'] = math.ceil(max(0, pm_peak_diff))
                route_level_df.loc[index, 'Evening_DIFFERENCE'] = math.ceil(max(0, evening_diff))
                route_level_df.loc[index, 'Total_DIFFERENCE'] = sum([
                    math.ceil(max(0, early_am_diff)),
                    math.ceil(max(0, am_diff)),
                    math.ceil(max(0, midday_diff)),
                    math.ceil(max(0, pm_peak_diff)),
                    math.ceil(max(0, evening_diff))
                ])
                route_level_df.loc[index, 'Overall_Goal_DIFFERENCE'] = math.ceil(max(0, overall_diff))
        # logger.info("\n=== FINAL RESULTS ===")
        # logger.info(f"Final route_level_df shape: {route_level_df.shape}")
        # logger.info("Sample of final results:")
        # logger.info(route_level_df.head(3).to_string())
        
        return route_level_df
    except Exception as e:
        # logger.error(f"Error in create_route_level_df: {str(e)}", exc_info=True)
        raise
    finally:
        pass
        # logger.info("=== KCATA ROUTE LEVEL DEBUG COMPLETED ===")
        # Ensure all logs are flushed
        # for handler in logger.handlers:
        #     handler.flush()





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


def clean_route_name(route_series):
    """Clean route names by removing [INBOUND] and [OUTBOUND] markers"""
    return (
        route_series
        .astype(str)
        .str.replace(r' \[(INBOUND|OUTBOUND)\]', '', regex=True)
        .str.strip()
    )

def format_percentage(value):
    """Format percentage values consistently"""
    return f"{round(float(value), 2)}%"

def calculate_avg_time(time_series):
    """Calculate average time from HH:MM:SS formatted series"""
    total_seconds = time_series.apply(
        lambda x: sum(int(t) * 60 ** i for i, t in enumerate(reversed(x.split(":"))))
    ).sum()
    count = len(time_series)
    if count == 0:
        return "00:00:00"
    avg = int(total_seconds / count)
    mins, sec = divmod(avg, 60)
    hr, mins = divmod(mins, 60)
    return f"{hr:02}:{mins:02}:{sec:02}"

def process_survey_data(df):
    """
    Process survey data by filtering, cleaning route names, and creating summary tables.
    
    Parameters:
    df (pd.DataFrame): Input dataframe containing survey data
    
    Returns:
    tuple: A tuple containing (interviewer_pivot, route_pivot, detail_table)
    """
    # Convert 'Completed' column to datetime
    df['Completed'] = pd.to_datetime(df['Completed'], errors='coerce').dt.date
    df['HAVE_5_MIN_FOR_SURVE_Code_'] = df['HAVE_5_MIN_FOR_SURVE_Code_'].astype(str)
    df['INTERV_INIT'] = df['INTERV_INIT'].astype(str)

    # Apply filters
    filtered_df = df[
        (df['HAVE_5_MIN_FOR_SURVE_Code_'] == '1') &
        (df['INTERV_INIT'] != "999")
    ].copy()

    # Clean route: remove _00, _01 etc. from the end
    filtered_df['ROUTE_MAIN'] = filtered_df['ROUTE_SURVEYED_Code_'].str.extract(r'(^.*)_\d\d$')

    # Fallback for cases without _dd pattern
    filtered_df['ROUTE_MAIN'] = filtered_df['ROUTE_MAIN'].fillna(filtered_df['ROUTE_SURVEYED_Code_'])

    # Create Interviewer-by-Date pivot - RESET INDEX to make INTERV_INIT a proper column
    interviewer_pivot = pd.pivot_table(
        filtered_df,
        values='ROUTE_SURVEYED_Code_',
        index='INTERV_INIT',
        columns='Completed',
        aggfunc='count',
        fill_value=0,
        margins=True,
        margins_name='Total'
    ).reset_index()  # Add this line to convert index to column

    # Create Route-by-Date pivot - RESET INDEX to make ROUTE_MAIN a proper column
    route_pivot = pd.pivot_table(
        filtered_df,
        values='INTERV_INIT',
        index='ROUTE_MAIN',
        columns='Completed',
        aggfunc='count',
        fill_value=0,
        margins=True,
        margins_name='Total'
    ).reset_index()  # Add this line to convert index to column

    # Rename the index column appropriately
    interviewer_pivot = interviewer_pivot.rename(columns={'INTERV_INIT': 'Interviewer'})
    route_pivot = route_pivot.rename(columns={'ROUTE_MAIN': 'Route'})

    # Create detail table
    detail_table = (
        filtered_df
        .groupby(['INTERV_INIT', 'ROUTE_MAIN', 'Completed'])
        .size()
        .reset_index(name='Count')
        .rename(columns={'ROUTE_MAIN': 'ROUTE', 'Completed': 'Date'})
        .sort_values(by=['Date', 'INTERV_INIT', 'ROUTE'])
    )
    detail_table['Date'] = pd.to_datetime(detail_table['Date']).dt.date

    return interviewer_pivot, route_pivot, detail_table


def process_surveyor_data_kcata(df, elvis_df):
    """Process data for surveyor-level report"""
    # Clean and filter data
    df['INTERV_INIT'] = df['INTERV_INIT'].astype(str)
    filtered_df = df[df['INTERV_INIT'] != "999"]
    valid_surveys_df = filtered_df[filtered_df['HAVE_5_MIN_FOR_SURVECode'] == 1]
    
    # Base counts
    record_counts = (
        valid_surveys_df
        .groupby('INTERV_INIT')['id']
        .count()
        .reset_index()
        .rename(columns={'id': '# of Records'})
    )
    
    interv_list = sorted(filtered_df['INTERV_INIT'].unique())
    summary_df = pd.DataFrame({'INTERV_INIT': interv_list}).merge(record_counts, how='left').fillna(0)
    
    # Supervisor Deletes
    delete_counts = (
        filtered_df[
            (filtered_df['ELVIS_STATUS'] == 'Delete') & 
            (filtered_df['HAVE_5_MIN_FOR_SURVECode'] == 1)
        ]
        .groupby('INTERV_INIT')['id']
        .count()
        .reset_index()
        .rename(columns={'id': '# of Supervisor Delete'})
    )
    summary_df = summary_df.merge(delete_counts, how='left').fillna(0)
    
    # Records Remove
    remove_counts = (
        valid_surveys_df[valid_surveys_df['Final_Usage'] == 'Remove']
        .groupby('INTERV_INIT')['id']
        .count()
        .reset_index()
        .rename(columns={'id': '# of Records Remove'})
    )
    summary_df = summary_df.merge(remove_counts, how='left').fillna(0)
    
    # Records Reviewed/Not Reviewed
    reviewed_df = valid_surveys_df[valid_surveys_df['Final_Usage'].notna()]
    reviewed_counts = (
        reviewed_df
        .groupby('INTERV_INIT')['id']
        .count()
        .reset_index()
        .rename(columns={'id': '# of Records Reviewed'})
    )
    summary_df = summary_df.merge(reviewed_counts, how='left').fillna(0)
    
    not_reviewed_counts = (
        valid_surveys_df[valid_surveys_df['Final_Usage'].isna()]
        .groupby('INTERV_INIT')['id']
        .count()
        .reset_index()
        .rename(columns={'id': '# of Records Not Reviewed'})
    )
    summary_df = summary_df.merge(not_reviewed_counts, how='left').fillna(0)
    
    # Process elvis data for additional metrics
    elvis_df['INTERV_INIT'] = elvis_df['INTERV_INIT'].astype(str)
    address_filtered = elvis_df[
        (elvis_df['INTERV_INIT'] != "999") &
        (elvis_df['HAVE_5_MIN_FOR_SURVECode'] == 1)
    ].copy()
    
    # Address completeness
    address_fields = [
        'HOME_ADDRESS_LAT', 'HOME_ADDRESS_LONG', 'HOME_ADDRESS_PLACE',
        'HOME_ADDRESS_ADDR', 'HOME_ADDRESS_CITY', 'HOME_ADDRESS_STATE', 'HOME_ADDRESS_ZIP'
    ]
    address_filtered['Incomplete_Address'] = address_filtered[address_fields].isnull().any(axis=1)
    
    address_group = address_filtered.groupby('INTERV_INIT').agg(
        total_records=('id', 'count'),
        incomplete_count=('Incomplete_Address', 'sum')
    ).reset_index()
    
    address_group['% of Incomplete Home Address'] = (
        (address_group['incomplete_count'] / address_group['total_records']) * 100
    ).round(2).astype(str) + '%'
    
    summary_df = summary_df.merge(
        address_group[['INTERV_INIT', '% of Incomplete Home Address']], 
        how='left'
    ).fillna('0.0%')
    
    # Survey times
    time_cols = ['HOMEADD_TIME', 'NOTE_TIME', 'REVIEWSCR_TIME']
    for col in time_cols:
        address_filtered[col] = pd.to_datetime(address_filtered[col], errors='coerce')
    
    address_filtered['SurveyTime (All)'] = (
        address_filtered['NOTE_TIME'] - address_filtered['HOMEADD_TIME']
    ).dt.total_seconds()
    address_filtered['SurveyTime (TripLogic)'] = (
        address_filtered['REVIEWSCR_TIME'] - address_filtered['HOMEADD_TIME']
    ).dt.total_seconds()
    address_filtered['SurveyTime (DemoLogic)'] = (
        address_filtered['NOTE_TIME'] - address_filtered['REVIEWSCR_TIME']
    ).dt.total_seconds()
    
    # Format times
    survey_time_group = address_filtered.groupby('INTERV_INIT').agg({
        'SurveyTime (All)': 'mean',
        'SurveyTime (TripLogic)': 'mean',
        'SurveyTime (DemoLogic)': 'mean'
    }).reset_index()
    
    for col in ['SurveyTime (All)', 'SurveyTime (TripLogic)', 'SurveyTime (DemoLogic)']:
        survey_time_group[col] = survey_time_group[col].apply(
            lambda x: "00:00:00" if pd.isna(x) or x < 0 else 
            f"{int(x//3600):02}:{int((x%3600)//60):02}:{int(x%60):02}"
        )
    
    summary_df = summary_df.merge(survey_time_group, how='left').fillna("00:00:00")
    
    # Other metrics (0 transfers, access walk, etc.)
    metrics = [
        ('0 Transfers', 
         (address_filtered['PREV_TRANSFERSCode'] == '(0) None') & 
         (address_filtered['NEXT_TRANSFERSCode'] == '(0) None')),
        ('Access Walk', address_filtered['VAL_ACCESS_WALK'] == 1),
        ('Egress Walk', address_filtered['VAL_EGRESS_WALK'] == 1),
        ('LowIncome', address_filtered['INCOMECode'].astype(str).isin(['1', '2', '3', '4'])),
        ('No Income', 
         (address_filtered['INCOMECode'].isna()) | 
         (address_filtered['INCOMECode'].astype(str) == 'REFUSED')),
        ('Hispanic', address_filtered['RACE_6'].astype(str).str.strip().str.upper() == 'YES'),
        ('Black', address_filtered['RACE_5'].astype(str).str.strip().str.upper() == 'YES'),
        ('White', address_filtered['RACE_4'].astype(str).str.strip().str.upper() == 'YES')
    ]
    
    for name, condition in metrics:
        metric_group = (
            address_filtered[condition]
            .groupby('INTERV_INIT')['id']
            .count()
            .reset_index()
            .rename(columns={'id': f'{name.lower()}_count'})
        )
        
        metric_percent = address_group[['INTERV_INIT', 'total_records']].merge(
            metric_group, how='left'
        ).fillna(0)
        
        metric_percent[f'% of {name}'] = (
            (metric_percent[f'{name.lower()}_count'] / metric_percent['total_records']) * 100
        ).apply(format_percentage)
        
        summary_df = summary_df.merge(
            metric_percent[['INTERV_INIT', f'% of {name}']], 
            how='left'
        ).fillna('0.0%')
    
    # Contest metrics (keeping only contest-related metrics)
    contest_filtered = elvis_df[
        (elvis_df['INTERV_INIT'] != "999") & 
        (elvis_df['HAVE_5_MIN_FOR_SURVECode'] == 1)
    ].copy()
    
    contest_filtered['contest_yes'] = (
        contest_filtered['REGISTER_TO_WIN_YNCODE'].astype(str).str.strip().str.upper() == 'YES'
    )
    
    contest_group = contest_filtered.groupby('INTERV_INIT').agg(
        total_records=('id', 'count'),
        contest_yes_count=('contest_yes', 'sum')
    ).reset_index()
    
    contest_group['% of Contest - Yes'] = (
        (contest_group['contest_yes_count'] / contest_group['total_records']) * 100
    ).apply(format_percentage)
    
    summary_df = summary_df.merge(
        contest_group[['INTERV_INIT', '% of Contest - Yes']], 
        how='left'
    ).fillna('0.0%')
    
    contest_filtered['valid_contest'] = (
        contest_filtered['contest_yes'] &
        contest_filtered['REG_2_WIN_CONTACT_NAME'].notna() & 
        (contest_filtered['REG_2_WIN_CONTACT_NAME'].astype(str).str.strip() != '') &
        contest_filtered['REG_2_WIN_CONTACT_PHONE'].notna() & 
        (contest_filtered['REG_2_WIN_CONTACT_PHONE'].astype(str).str.strip() != '')
    )
    
    contest_valid_group = contest_filtered.groupby('INTERV_INIT').agg(
        total_records=('id', 'count'),
        valid_contest_count=('valid_contest', 'sum')
    ).reset_index()
    
    contest_valid_group['% of Contest - (Yes & Good Info)/Overall # of Records'] = (
        (contest_valid_group['valid_contest_count'] / contest_valid_group['total_records']) * 100
    ).apply(format_percentage)
    
    summary_df = summary_df.merge(
        contest_valid_group[['INTERV_INIT', '% of Contest - (Yes & Good Info)/Overall # of Records']],
        how='left'
    ).fillna('0.0%')
    
    # Add total row
    total_row = {
        'INTERV_INIT': 'Total',
        '# of Records': summary_df['# of Records'].sum(),
        '# of Supervisor Delete': summary_df['# of Supervisor Delete'].sum(),
        '# of Records Remove': summary_df['# of Records Remove'].sum(),
        '# of Records Reviewed': summary_df['# of Records Reviewed'].sum(),
        '# of Records Not Reviewed': summary_df['# of Records Not Reviewed'].sum(),
        'SurveyTime (All)': calculate_avg_time(
            summary_df.loc[summary_df['INTERV_INIT'] != 'Total', 'SurveyTime (All)']),
        'SurveyTime (TripLogic)': calculate_avg_time(
            summary_df.loc[summary_df['INTERV_INIT'] != 'Total', 'SurveyTime (TripLogic)']),
        'SurveyTime (DemoLogic)': calculate_avg_time(
            summary_df.loc[summary_df['INTERV_INIT'] != 'Total', 'SurveyTime (DemoLogic)']),
    }
    
    # Add average percentages
    percent_cols = [col for col in summary_df.columns if col.startswith('% of')]
    for col in percent_cols:
        avg = summary_df.loc[summary_df['INTERV_INIT'] != 'Total', col]\
              .str.rstrip('%').astype(float).mean()
        total_row[col] = format_percentage(avg)
    
    summary_df = pd.concat([summary_df, pd.DataFrame([total_row])], ignore_index=True)
    
    column_order = [
        'INTERV_INIT', '# of Records', '# of Supervisor Delete', '# of Records Remove',
        '# of Records Reviewed', '# of Records Not Reviewed', 'SurveyTime (All)',
        'SurveyTime (TripLogic)', 'SurveyTime (DemoLogic)', '% of Incomplete Home Address',
        '% of 0 Transfers', '% of Access Walk', '% of Egress Walk',
        '% of LowIncome', '% of No Income', '% of Hispanic', '% of Black', '% of White',
        '% of Contest - Yes', '% of Contest - (Yes & Good Info)/Overall # of Records'
    ]
    
    return summary_df[column_order]


def process_surveyor_data(df, elvis_df):
    """Process data for surveyor-level report"""
    # Clean and filter data
    df['INTERV_INIT'] = df['INTERV_INIT'].astype(str)
    filtered_df = df[df['INTERV_INIT'] != "999"]
    # filtered_df = df[df['INTERV_INIT'] != 999]
    valid_surveys_df = filtered_df[filtered_df['HAVE_5_MIN_FOR_SURVECode'] == 1]
    
    # Base counts
    record_counts = (
        valid_surveys_df
        .groupby('INTERV_INIT')['id']
        .count()
        .reset_index()
        .rename(columns={'id': '# of Records'})
    )
    
    interv_list = sorted(filtered_df['INTERV_INIT'].unique())
    summary_df = pd.DataFrame({'INTERV_INIT': interv_list}).merge(record_counts, how='left').fillna(0)
    
    # Supervisor Deletes
    delete_counts = (
        filtered_df[
            (filtered_df['ELVIS_STATUS'] == 'Delete') & 
            (filtered_df['HAVE_5_MIN_FOR_SURVECode'] == 1)
        ]
        .groupby('INTERV_INIT')['id']
        .count()
        .reset_index()
        .rename(columns={'id': '# of Supervisor Delete'})
    )
    summary_df = summary_df.merge(delete_counts, how='left').fillna(0)
    
    # Records Remove
    remove_counts = (
        valid_surveys_df[valid_surveys_df['Final_Usage'] == 'Remove']
        .groupby('INTERV_INIT')['id']
        .count()
        .reset_index()
        .rename(columns={'id': '# of Records Remove'})
    )
    summary_df = summary_df.merge(remove_counts, how='left').fillna(0)
    
    # Records Reviewed/Not Reviewed
    reviewed_df = valid_surveys_df[valid_surveys_df['Final_Usage'].notna()]
    reviewed_counts = (
        reviewed_df
        .groupby('INTERV_INIT')['id']
        .count()
        .reset_index()
        .rename(columns={'id': '# of Records Reviewed'})
    )
    summary_df = summary_df.merge(reviewed_counts, how='left').fillna(0)
    
    not_reviewed_counts = (
        valid_surveys_df[valid_surveys_df['Final_Usage'].isna()]
        .groupby('INTERV_INIT')['id']
        .count()
        .reset_index()
        .rename(columns={'id': '# of Records Not Reviewed'})
    )
    summary_df = summary_df.merge(not_reviewed_counts, how='left').fillna(0)
    
    # Process elvis data for additional metrics
    elvis_df['INTERV_INIT'] = elvis_df['INTERV_INIT'].astype(str)
    elvis_df['HAVE_5_MIN_FOR_SURVE_Code_'] = elvis_df['HAVE_5_MIN_FOR_SURVE_Code_'].astype(str)
    address_filtered = elvis_df[
        (elvis_df['INTERV_INIT'] != "999") &
        # (elvis_df['INTERV_INIT'] != 999) &
        (elvis_df['HAVE_5_MIN_FOR_SURVE_Code_'] == '1')
    ].copy()
    
    # Address completeness
    address_fields = [
        'HOME_ADDRESS_LAT_', 'HOME_ADDRESS_LONG_', 'HOME_ADDRESS_PLACE_',
        'HOME_ADDRESS_ADDR_', 'HOME_ADDRESS_CITY_', 'HOME_ADDRESS_STATE_', 'HOME_ADDRESS_ZIP_'
    ]
    address_filtered['Incomplete_Address'] = address_filtered[address_fields].isnull().any(axis=1)
    
    address_group = address_filtered.groupby('INTERV_INIT').agg(
        total_records=('id', 'count'),
        incomplete_count=('Incomplete_Address', 'sum')
    ).reset_index()
    
    address_group['% of Incomplete Home Address'] = (
        (address_group['incomplete_count'] / address_group['total_records']) * 100
    ).round(2).astype(str) + '%'
    
    summary_df = summary_df.merge(
        address_group[['INTERV_INIT', '% of Incomplete Home Address']], 
        how='left'
    ).fillna('0.0%')
    
    # Homeless percentage
    homeless_group = (
        address_filtered[address_filtered['HOME_ADDRESS_HOMELESS_'] == 'YES']
        .groupby('INTERV_INIT')['id']
        .count()
        .reset_index()
        .rename(columns={'id': 'homeless_count'})
    )
    
    homeless_percent = address_group[['INTERV_INIT', 'total_records']].merge(
        homeless_group, how='left'
    ).fillna(0)
    
    homeless_percent['% of Homeless'] = (
        (homeless_percent['homeless_count'] / homeless_percent['total_records']) * 100
    ).apply(format_percentage)
    
    summary_df = summary_df.merge(
        homeless_percent[['INTERV_INIT', '% of Homeless']], 
        how='left'
    ).fillna('0.0%')
    
    # Survey times
    time_cols = ['HOMEADD_TIME', 'NOTE_TIME', 'REVIEWSCR_TIME']
    for col in time_cols:
        address_filtered[col] = pd.to_datetime(address_filtered[col], errors='coerce')
    
    address_filtered['SurveyTime (All)'] = (
        address_filtered['NOTE_TIME'] - address_filtered['HOMEADD_TIME']
    ).dt.total_seconds()
    address_filtered['SurveyTime (TripLogic)'] = (
        address_filtered['REVIEWSCR_TIME'] - address_filtered['HOMEADD_TIME']
    ).dt.total_seconds()
    address_filtered['SurveyTime (DemoLogic)'] = (
        address_filtered['NOTE_TIME'] - address_filtered['REVIEWSCR_TIME']
    ).dt.total_seconds()
    
    # Format times
    survey_time_group = address_filtered.groupby('INTERV_INIT').agg({
        'SurveyTime (All)': 'mean',
        'SurveyTime (TripLogic)': 'mean',
        'SurveyTime (DemoLogic)': 'mean'
    }).reset_index()
    
    for col in ['SurveyTime (All)', 'SurveyTime (TripLogic)', 'SurveyTime (DemoLogic)']:
        survey_time_group[col] = survey_time_group[col].apply(
            lambda x: "00:00:00" if pd.isna(x) or x < 0 else 
            f"{int(x//3600):02}:{int((x%3600)//60):02}:{int(x%60):02}"
        )
    
    summary_df = summary_df.merge(survey_time_group, how='left').fillna("00:00:00")
    
    # Other metrics (0 transfers, access walk, etc.)
    metrics = [
        ('0 Transfers', 
         (address_filtered['PREV_TRANSFERS'] == '(0) None') & 
         (address_filtered['NEXT_TRANSFERS'] == '(0) None')),
        ('Access Walk', address_filtered['VAL_ACCESS_WALK'] == 1),
        ('Egress Walk', address_filtered['VAL_EGRESS_WALK'] == 1),
        ('LowIncome', address_filtered['INCOME_Code_'].astype(str).isin(['1', '2', '3', '4'])),
        ('No Income', 
         (address_filtered['INCOME_Code_'].isna()) | 
         (address_filtered['INCOME_Code_'].astype(str) == 'REFUSED')),
        ('Hispanic', address_filtered['RACE_6_'].astype(str).str.strip().str.upper() == 'YES'),
        ('Black', address_filtered['RACE_5_'].astype(str).str.strip().str.upper() == 'YES'),
        ('White', address_filtered['RACE_4_'].astype(str).str.strip().str.upper() == 'YES')
    ]
    
    for name, condition in metrics:
        metric_group = (
            address_filtered[condition]
            .groupby('INTERV_INIT')['id']
            .count()
            .reset_index()
            .rename(columns={'id': f'{name.lower()}_count'})
        )
        
        metric_percent = address_group[['INTERV_INIT', 'total_records']].merge(
            metric_group, how='left'
        ).fillna(0)
        
        metric_percent[f'% of {name}'] = (
            (metric_percent[f'{name.lower()}_count'] / metric_percent['total_records']) * 100
        ).apply(format_percentage)
        
        summary_df = summary_df.merge(
            metric_percent[['INTERV_INIT', f'% of {name}']], 
            how='left'
        ).fillna('0.0%')
    # elvis_df['INTERV_INIT'] = elvis_df['INTERV_INIT'].astype(str)
    elvis_df['HAVE_5_MIN_FOR_SURVE_Code_'] = elvis_df['HAVE_5_MIN_FOR_SURVE_Code_'].astype(str)
    # Follow-up and contest metrics
    followup_filtered = elvis_df[
        (elvis_df['INTERV_INIT'] != "999") & 
        # (elvis_df['INTERV_INIT'] != 999) & 
        (elvis_df['HAVE_5_MIN_FOR_SURVE_Code_'] == '1')
    ].copy()
    
    followup_filtered['has_followup'] = (
        followup_filtered['FOLLOWUP_SMS_NAME_'].notna() & 
        (followup_filtered['FOLLOWUP_SMS_NAME_'].astype(str).str.strip() != '') &
        followup_filtered['FOLLOWUP_SMS_PHONE_'].notna() & 
        (followup_filtered['FOLLOWUP_SMS_PHONE_'].astype(str).str.strip() != '')
    )
    
    followup_group = followup_filtered.groupby('INTERV_INIT').agg(
        total_records=('id', 'count'),
        followup_count=('has_followup', 'sum')
    ).reset_index()
    
    followup_group['% of Follow-Up Survey'] = (
        (followup_group['followup_count'] / followup_group['total_records']) * 100
    ).apply(format_percentage)
    
    summary_df = summary_df.merge(
        followup_group[['INTERV_INIT', '% of Follow-Up Survey']], 
        how='left'
    ).fillna('0.0%')
    
    # Contest metrics
    contest_filtered = followup_filtered.copy()
    contest_filtered['contest_yes'] = (
        contest_filtered['REGISTER_TO_WIN_Y_N'].astype(str).str.strip().str.upper() == 'YES'
    )
    
    contest_group = contest_filtered.groupby('INTERV_INIT').agg(
        total_records=('id', 'count'),
        contest_yes_count=('contest_yes', 'sum')
    ).reset_index()
    
    contest_group['% of Contest - Yes'] = (
        (contest_group['contest_yes_count'] / contest_group['total_records']) * 100
    ).apply(format_percentage)
    
    summary_df = summary_df.merge(
        contest_group[['INTERV_INIT', '% of Contest - Yes']], 
        how='left'
    ).fillna('0.0%')
    
    contest_filtered['valid_contest'] = (
        contest_filtered['contest_yes'] &
        contest_filtered['REG2WIN_CONTACT_NAME_'].notna() & 
        (contest_filtered['REG2WIN_CONTACT_NAME_'].astype(str).str.strip() != '') &
        contest_filtered['REG2WIN_CONTACT_PHONE_'].notna() & 
        (contest_filtered['REG2WIN_CONTACT_PHONE_'].astype(str).str.strip() != '')
    )
    
    contest_valid_group = contest_filtered.groupby('INTERV_INIT').agg(
        total_records=('id', 'count'),
        valid_contest_count=('valid_contest', 'sum')
    ).reset_index()
    
    contest_valid_group['% of Contest - (Yes & Good Info)/Overall # of Records'] = (
        (contest_valid_group['valid_contest_count'] / contest_valid_group['total_records']) * 100
    ).apply(format_percentage)
    
    summary_df = summary_df.merge(
        contest_valid_group[['INTERV_INIT', '% of Contest - (Yes & Good Info)/Overall # of Records']],
        how='left'
    ).fillna('0.0%')
    
    # Add total row
    total_row = {
        'INTERV_INIT': 'Total',
        '# of Records': summary_df['# of Records'].sum(),
        '# of Supervisor Delete': summary_df['# of Supervisor Delete'].sum(),
        '# of Records Remove': summary_df['# of Records Remove'].sum(),
        '# of Records Reviewed': summary_df['# of Records Reviewed'].sum(),
        '# of Records Not Reviewed': summary_df['# of Records Not Reviewed'].sum(),
        'SurveyTime (All)': calculate_avg_time(
            summary_df.loc[summary_df['INTERV_INIT'] != 'Total', 'SurveyTime (All)']),
        'SurveyTime (TripLogic)': calculate_avg_time(
            summary_df.loc[summary_df['INTERV_INIT'] != 'Total', 'SurveyTime (TripLogic)']),
        'SurveyTime (DemoLogic)': calculate_avg_time(
            summary_df.loc[summary_df['INTERV_INIT'] != 'Total', 'SurveyTime (DemoLogic)']),
    }
    
    # Add average percentages
    percent_cols = [col for col in summary_df.columns if col.startswith('% of')]
    for col in percent_cols:
        avg = summary_df.loc[summary_df['INTERV_INIT'] != 'Total', col]\
              .str.rstrip('%').astype(float).mean()
        total_row[col] = format_percentage(avg)
    
    summary_df = pd.concat([summary_df, pd.DataFrame([total_row])], ignore_index=True)
    column_order = [
        'INTERV_INIT', '# of Records', '# of Supervisor Delete', '# of Records Remove',
        '# of Records Reviewed', '# of Records Not Reviewed', 'SurveyTime (All)',
        'SurveyTime (TripLogic)', 'SurveyTime (DemoLogic)', '% of Incomplete Home Address',
        '% of Homeless', '% of 0 Transfers', '% of Access Walk', '% of Egress Walk',
        '% of LowIncome', '% of No Income', '% of Hispanic', '% of Black', '% of White',
        '% of Follow-Up Survey', '% of Contest - Yes', 
        '% of Contest - (Yes & Good Info)/Overall # of Records'
    ]
    
    return summary_df[column_order]


def process_route_data(df, elvis_df):
    """Process data for route-level report"""
    # Clean route names
    df['ROUTE_ROOT'] = clean_route_name(df['ROUTE_SURVEYED'])
    df['INTERV_INIT'] = df['INTERV_INIT'].astype(str)
    
    # Filter data
    df['INTERV_INIT'] = df['INTERV_INIT'].astype(str)
    df['HAVE_5_MIN_FOR_SURVECode'] = df['HAVE_5_MIN_FOR_SURVECode'].astype(str)
    filtered_df = df[df['INTERV_INIT'] != "999"]
    # filtered_df = df[df['INTERV_INIT'] != 999]
    valid_surveys_df = filtered_df[filtered_df['HAVE_5_MIN_FOR_SURVECode'] == '1']
    
    # Base counts
    record_counts = (
        valid_surveys_df
        .groupby('ROUTE_ROOT')['id']
        .count()
        .reset_index()
        .rename(columns={'id': '# of Records'})
    )
    
    route_list = sorted(filtered_df['ROUTE_ROOT'].unique())
    route_report_df = pd.DataFrame({'ROUTE_ROOT': route_list}).merge(record_counts, how='left').fillna(0)
    
    # Supervisor Deletes
    delete_counts = (
        filtered_df[
            (filtered_df['ELVIS_STATUS'] == 'Delete') & 
            (filtered_df['HAVE_5_MIN_FOR_SURVECode'] == 1)
        ]
        .groupby('ROUTE_ROOT')['id']
        .count()
        .reset_index()
        .rename(columns={'id': '# of Supervisor Delete'})
    )
    route_report_df = route_report_df.merge(delete_counts, how='left').fillna(0)
    
    # Records Remove
    remove_counts = (
        valid_surveys_df[valid_surveys_df['Final_Usage'] == 'Remove']
        .groupby('ROUTE_ROOT')['id']
        .count()
        .reset_index()
        .rename(columns={'id': '# of Records Remove'})
    )
    route_report_df = route_report_df.merge(remove_counts, how='left').fillna(0)
    
    # Records Reviewed/Not Reviewed
    reviewed_df = valid_surveys_df[valid_surveys_df['Final_Usage'].notna()]
    reviewed_counts = (
        reviewed_df
        .groupby('ROUTE_ROOT')['id']
        .count()
        .reset_index()
        .rename(columns={'id': '# of Records Reviewed'})
    )
    route_report_df = route_report_df.merge(reviewed_counts, how='left').fillna(0)
    
    not_reviewed_counts = (
        valid_surveys_df[valid_surveys_df['Final_Usage'].isna()]
        .groupby('ROUTE_ROOT')['id']
        .count()
        .reset_index()
        .rename(columns={'id': '# of Records Not Reviewed'})
    )
    route_report_df = route_report_df.merge(not_reviewed_counts, how='left').fillna(0)
    
    # Process elvis data for additional metrics
    elvis_df['ROUTE_ROOT'] = clean_route_name(elvis_df['ROUTE_SURVEYED'])
    elvis_df['INTERV_INIT'] = elvis_df['INTERV_INIT'].astype(str)
    elvis_df['HAVE_5_MIN_FOR_SURVE_Code_'] = elvis_df['HAVE_5_MIN_FOR_SURVE_Code_'].astype(str)
    address_filtered = elvis_df[
        (elvis_df['INTERV_INIT'] != "999") & 
        # (elvis_df['INTERV_INIT'] != 999) & 
        (elvis_df['HAVE_5_MIN_FOR_SURVE_Code_'] == '1')
    ].copy()
    
    # Address completeness
    address_fields = [
        'HOME_ADDRESS_LAT_', 'HOME_ADDRESS_LONG_', 'HOME_ADDRESS_PLACE_',
        'HOME_ADDRESS_ADDR_', 'HOME_ADDRESS_CITY_', 'HOME_ADDRESS_STATE_', 'HOME_ADDRESS_ZIP_'
    ]
    address_filtered['Incomplete_Address'] = address_filtered[address_fields].isnull().any(axis=1)
    
    address_group = address_filtered.groupby('ROUTE_ROOT').agg(
        total_records=('id', 'count'),
        incomplete_count=('Incomplete_Address', 'sum')
    ).reset_index()
    
    address_group['% of Incomplete Home Address'] = (
        (address_group['incomplete_count'] / address_group['total_records']) * 100
    ).apply(format_percentage)
    
    route_report_df = route_report_df.merge(
        address_group[['ROUTE_ROOT', '% of Incomplete Home Address']], 
        how='left'
    ).fillna('0.0%')
    
    # Homeless percentage
    homeless_group = (
        address_filtered[address_filtered['HOME_ADDRESS_HOMELESS_'] == 'YES']
        .groupby('ROUTE_ROOT')['id']
        .count()
        .reset_index()
        .rename(columns={'id': 'homeless_count'})
    )
    
    homeless_percent = address_group[['ROUTE_ROOT', 'total_records']].merge(
        homeless_group, how='left'
    ).fillna(0)
    
    homeless_percent['% of Homeless'] = (
        (homeless_percent['homeless_count'] / homeless_percent['total_records']) * 100
    ).apply(format_percentage)
    
    route_report_df = route_report_df.merge(
        homeless_percent[['ROUTE_ROOT', '% of Homeless']], 
        how='left'
    ).fillna('0.0%')
    
    # Survey times
    time_cols = ['HOMEADD_TIME', 'NOTE_TIME', 'REVIEWSCR_TIME']
    for col in time_cols:
        address_filtered[col] = pd.to_datetime(address_filtered[col], errors='coerce')
    
    address_filtered['SurveyTime (All)'] = (
        address_filtered['NOTE_TIME'] - address_filtered['HOMEADD_TIME']
    ).dt.total_seconds()
    address_filtered['SurveyTime (TripLogic)'] = (
        address_filtered['REVIEWSCR_TIME'] - address_filtered['HOMEADD_TIME']
    ).dt.total_seconds()
    address_filtered['SurveyTime (DemoLogic)'] = (
        address_filtered['NOTE_TIME'] - address_filtered['REVIEWSCR_TIME']
    ).dt.total_seconds()
    
    # Format times
    survey_time_group = address_filtered.groupby('ROUTE_ROOT').agg({
        'SurveyTime (All)': 'mean',
        'SurveyTime (TripLogic)': 'mean',
        'SurveyTime (DemoLogic)': 'mean'
    }).reset_index()
    
    for col in ['SurveyTime (All)', 'SurveyTime (TripLogic)', 'SurveyTime (DemoLogic)']:
        survey_time_group[col] = survey_time_group[col].apply(
            lambda x: "00:00:00" if pd.isna(x) or x < 0 else 
            f"{int(x//3600):02}:{int((x%3600)//60):02}:{int(x%60):02}"
        )
    
    route_report_df = route_report_df.merge(survey_time_group, how='left').fillna("00:00:00")
    
    # Other metrics (0 transfers, access walk, etc.)
    metrics = [
        ('0 Transfers', 
         (address_filtered['PREV_TRANSFERS'] == '(0) None') & 
         (address_filtered['NEXT_TRANSFERS'] == '(0) None')),
        ('Access Walk', address_filtered['VAL_ACCESS_WALK'] == 1),
        ('Egress Walk', address_filtered['VAL_EGRESS_WALK'] == 1),
        ('LowIncome', address_filtered['INCOME_Code_'].astype(str).isin(['1', '2', '3', '4'])),
        ('No Income', 
         (address_filtered['INCOME_Code_'].isna()) | 
         (address_filtered['INCOME_Code_'].astype(str) == 'REFUSED')),
        ('Hispanic', address_filtered['RACE_6_'].astype(str).str.strip().str.upper() == 'YES'),
        ('Black', address_filtered['RACE_5_'].astype(str).str.strip().str.upper() == 'YES'),
        ('White', address_filtered['RACE_4_'].astype(str).str.strip().str.upper() == 'YES')
    ]
    
    for name, condition in metrics:
        metric_group = (
            address_filtered[condition]
            .groupby('ROUTE_ROOT')['id']
            .count()
            .reset_index()
            .rename(columns={'id': f'{name.lower()}_count'})
        )
        
        metric_percent = address_group[['ROUTE_ROOT', 'total_records']].merge(
            metric_group, how='left'
        ).fillna(0)
        
        metric_percent[f'% of {name}'] = (
            (metric_percent[f'{name.lower()}_count'] / metric_percent['total_records']) * 100
        ).apply(format_percentage)
        
        route_report_df = route_report_df.merge(
            metric_percent[['ROUTE_ROOT', f'% of {name}']], 
            how='left'
        ).fillna('0.0%')
    elvis_df['INTERV_INIT'] = elvis_df['INTERV_INIT'].astype(str)
    elvis_df['HAVE_5_MIN_FOR_SURVE_Code_'] = elvis_df['HAVE_5_MIN_FOR_SURVE_Code_'].astype(str)
    # Follow-up and contest metrics
    followup_filtered = elvis_df[
        (elvis_df['INTERV_INIT'] != "999") & 
        # (elvis_df['INTERV_INIT'] != 999) & 
        (elvis_df['HAVE_5_MIN_FOR_SURVE_Code_'] == '1')
    ].copy()
    followup_filtered['ROUTE_ROOT'] = clean_route_name(followup_filtered['ROUTE_SURVEYED'])
    
    followup_filtered['has_followup'] = (
        followup_filtered['FOLLOWUP_SMS_NAME_'].notna() & 
        (followup_filtered['FOLLOWUP_SMS_NAME_'].astype(str).str.strip() != '') &
        followup_filtered['FOLLOWUP_SMS_PHONE_'].notna() & 
        (followup_filtered['FOLLOWUP_SMS_PHONE_'].astype(str).str.strip() != '')
    )
    
    followup_group = followup_filtered.groupby('ROUTE_ROOT').agg(
        total_records=('id', 'count'),
        followup_count=('has_followup', 'sum')
    ).reset_index()
    
    followup_group['% of Follow-Up Survey'] = (
        (followup_group['followup_count'] / followup_group['total_records']) * 100
    ).apply(format_percentage)
    
    route_report_df = route_report_df.merge(
        followup_group[['ROUTE_ROOT', '% of Follow-Up Survey']], 
        how='left'
    ).fillna('0.0%')
    
    # Contest metrics
    contest_filtered = followup_filtered.copy()
    contest_filtered['contest_yes'] = (
        contest_filtered['REGISTER_TO_WIN_Y_N'].astype(str).str.strip().str.upper() == 'YES'
    )
    
    contest_group = contest_filtered.groupby('ROUTE_ROOT').agg(
        total_records=('id', 'count'),
        contest_yes_count=('contest_yes', 'sum')
    ).reset_index()
    
    contest_group['% of Contest - Yes'] = (
        (contest_group['contest_yes_count'] / contest_group['total_records']) * 100
    ).apply(format_percentage)
    
    route_report_df = route_report_df.merge(
        contest_group[['ROUTE_ROOT', '% of Contest - Yes']], 
        how='left'
    ).fillna('0.0%')
    
    contest_filtered['valid_contest'] = (
        contest_filtered['contest_yes'] &
        contest_filtered['REG2WIN_CONTACT_NAME_'].notna() & 
        (contest_filtered['REG2WIN_CONTACT_NAME_'].astype(str).str.strip() != '') &
        contest_filtered['REG2WIN_CONTACT_PHONE_'].notna() & 
        (contest_filtered['REG2WIN_CONTACT_PHONE_'].astype(str).str.strip() != '')
    )
    
    contest_valid_group = contest_filtered.groupby('ROUTE_ROOT').agg(
        total_records=('id', 'count'),
        valid_contest_count=('valid_contest', 'sum')
    ).reset_index()
    
    contest_valid_group['% of Contest - (Yes & Good Info)/Overall # of Records'] = (
        (contest_valid_group['valid_contest_count'] / contest_valid_group['total_records']) * 100
    ).apply(format_percentage)
    
    route_report_df = route_report_df.merge(
        contest_valid_group[['ROUTE_ROOT', '% of Contest - (Yes & Good Info)/Overall # of Records']],
        how='left'
    ).fillna('0.0%')
    
    # Add total row
    total_row = {
        'ROUTE_ROOT': 'Total',
        '# of Records': route_report_df['# of Records'].sum(),
        '# of Supervisor Delete': route_report_df['# of Supervisor Delete'].sum(),
        '# of Records Remove': route_report_df['# of Records Remove'].sum(),
        '# of Records Reviewed': route_report_df['# of Records Reviewed'].sum(),
        '# of Records Not Reviewed': route_report_df['# of Records Not Reviewed'].sum(),
        'SurveyTime (All)': calculate_avg_time(
            route_report_df.loc[route_report_df['ROUTE_ROOT'] != 'Total', 'SurveyTime (All)']),
        'SurveyTime (TripLogic)': calculate_avg_time(
            route_report_df.loc[route_report_df['ROUTE_ROOT'] != 'Total', 'SurveyTime (TripLogic)']),
        'SurveyTime (DemoLogic)': calculate_avg_time(
            route_report_df.loc[route_report_df['ROUTE_ROOT'] != 'Total', 'SurveyTime (DemoLogic)']),
    }
    
    # Add average percentages
    percent_cols = [col for col in route_report_df.columns if col.startswith('% of')]
    for col in percent_cols:
        avg = route_report_df.loc[route_report_df['ROUTE_ROOT'] != 'Total', col]\
              .str.rstrip('%').astype(float).mean()
        total_row[col] = format_percentage(avg)
    
    route_report_df = pd.concat([route_report_df, pd.DataFrame([total_row])], ignore_index=True)
    
    # Rename and reorder columns
    route_report_df = route_report_df.rename(columns={'ROUTE_ROOT': 'Route'})
    
    column_order = [
        'Route', '# of Records', '# of Supervisor Delete', '# of Records Remove',
        '# of Records Reviewed', '# of Records Not Reviewed', 'SurveyTime (All)',
        'SurveyTime (TripLogic)', 'SurveyTime (DemoLogic)', '% of Incomplete Home Address',
        '% of Homeless', '% of 0 Transfers', '% of Access Walk', '% of Egress Walk',
        '% of LowIncome', '% of No Income', '% of Hispanic', '% of Black', '% of White',
        '% of Follow-Up Survey', '% of Contest - Yes', 
        '% of Contest - (Yes & Good Info)/Overall # of Records'
    ]
    
    return route_report_df[column_order]


def process_route_data_kcata(df, elvis_df):
    """Process data for route-level report"""
    # Clean route names
    df['ROUTE_ROOT'] = clean_route_name(df['ROUTE_SURVEYED'])
    df['INTERV_INIT'] = df['INTERV_INIT'].astype(str)
    
    # Filter data
    filtered_df = df[df['INTERV_INIT'] != "999"]
    valid_surveys_df = filtered_df[filtered_df['HAVE_5_MIN_FOR_SURVECode'] == 1]
    
    # Base counts
    record_counts = (
        valid_surveys_df
        .groupby('ROUTE_ROOT')['id']
        .count()
        .reset_index()
        .rename(columns={'id': '# of Records'})
    )
    
    route_list = sorted(filtered_df['ROUTE_ROOT'].unique())
    route_report_df = pd.DataFrame({'ROUTE_ROOT': route_list}).merge(record_counts, how='left').fillna(0)
    
    # Supervisor Deletes
    delete_counts = (
        filtered_df[
            (filtered_df['ELVIS_STATUS'] == 'Delete') & 
            (filtered_df['HAVE_5_MIN_FOR_SURVECode'] == 1)
        ]
        .groupby('ROUTE_ROOT')['id']
        .count()
        .reset_index()
        .rename(columns={'id': '# of Supervisor Delete'})
    )
    route_report_df = route_report_df.merge(delete_counts, how='left').fillna(0)
    
    # Records Remove
    remove_counts = (
        valid_surveys_df[valid_surveys_df['Final_Usage'] == 'Remove']
        .groupby('ROUTE_ROOT')['id']
        .count()
        .reset_index()
        .rename(columns={'id': '# of Records Remove'})
    )
    route_report_df = route_report_df.merge(remove_counts, how='left').fillna(0)
    
    # Records Reviewed/Not Reviewed
    reviewed_df = valid_surveys_df[valid_surveys_df['Final_Usage'].notna()]
    reviewed_counts = (
        reviewed_df
        .groupby('ROUTE_ROOT')['id']
        .count()
        .reset_index()
        .rename(columns={'id': '# of Records Reviewed'})
    )
    route_report_df = route_report_df.merge(reviewed_counts, how='left').fillna(0)
    
    not_reviewed_counts = (
        valid_surveys_df[valid_surveys_df['Final_Usage'].isna()]
        .groupby('ROUTE_ROOT')['id']
        .count()
        .reset_index()
        .rename(columns={'id': '# of Records Not Reviewed'})
    )
    route_report_df = route_report_df.merge(not_reviewed_counts, how='left').fillna(0)
    
    # Process elvis data for additional metrics
    elvis_df['ROUTE_ROOT'] = clean_route_name(elvis_df['ROUTE_SURVEYED'])
    elvis_df['INTERV_INIT'] = elvis_df['INTERV_INIT'].astype(str)
    
    address_filtered = elvis_df[
        (elvis_df['INTERV_INIT'] != "999") & 
        (elvis_df['HAVE_5_MIN_FOR_SURVECode'] == 1)
    ].copy()
    
    # Address completeness
    address_fields = [
        'HOME_ADDRESS_LAT', 'HOME_ADDRESS_LONG', 'HOME_ADDRESS_PLACE',
        'HOME_ADDRESS_ADDR', 'HOME_ADDRESS_CITY', 'HOME_ADDRESS_STATE', 'HOME_ADDRESS_ZIP'
    ]
    address_filtered['Incomplete_Address'] = address_filtered[address_fields].isnull().any(axis=1)
    
    address_group = address_filtered.groupby('ROUTE_ROOT').agg(
        total_records=('id', 'count'),
        incomplete_count=('Incomplete_Address', 'sum')
    ).reset_index()
    
    address_group['% of Incomplete Home Address'] = (
        (address_group['incomplete_count'] / address_group['total_records']) * 100
    ).apply(format_percentage)
    
    route_report_df = route_report_df.merge(
        address_group[['ROUTE_ROOT', '% of Incomplete Home Address']], 
        how='left'
    ).fillna('0.0%')
    
    # Survey times
    time_cols = ['HOMEADD_TIME', 'NOTE_TIME', 'REVIEWSCR_TIME']
    for col in time_cols:
        address_filtered[col] = pd.to_datetime(address_filtered[col], errors='coerce')
    
    address_filtered['SurveyTime (All)'] = (
        address_filtered['NOTE_TIME'] - address_filtered['HOMEADD_TIME']
    ).dt.total_seconds()
    address_filtered['SurveyTime (TripLogic)'] = (
        address_filtered['REVIEWSCR_TIME'] - address_filtered['HOMEADD_TIME']
    ).dt.total_seconds()
    address_filtered['SurveyTime (DemoLogic)'] = (
        address_filtered['NOTE_TIME'] - address_filtered['REVIEWSCR_TIME']
    ).dt.total_seconds()
    
    # Format times
    survey_time_group = address_filtered.groupby('ROUTE_ROOT').agg({
        'SurveyTime (All)': 'mean',
        'SurveyTime (TripLogic)': 'mean',
        'SurveyTime (DemoLogic)': 'mean'
    }).reset_index()
    
    for col in ['SurveyTime (All)', 'SurveyTime (TripLogic)', 'SurveyTime (DemoLogic)']:
        survey_time_group[col] = survey_time_group[col].apply(
            lambda x: "00:00:00" if pd.isna(x) or x < 0 else 
            f"{int(x//3600):02}:{int((x%3600)//60):02}:{int(x%60):02}"
        )
    
    route_report_df = route_report_df.merge(survey_time_group, how='left').fillna("00:00:00")
    
    # Other metrics (0 transfers, access walk, etc.)
    metrics = [
        ('0 Transfers', 
         (address_filtered['PREV_TRANSFERSCode'] == '(0) None') & 
         (address_filtered['NEXT_TRANSFERSCode'] == '(0) None')),
        ('Access Walk', address_filtered['VAL_ACCESS_WALK'] == 1),
        ('Egress Walk', address_filtered['VAL_EGRESS_WALK'] == 1),
        ('LowIncome', address_filtered['INCOMECode'].astype(str).isin(['1', '2', '3', '4'])),
        ('No Income', 
         (address_filtered['INCOMECode'].isna()) | 
         (address_filtered['INCOMECode'].astype(str) == 'REFUSED')),
        ('Hispanic', address_filtered['RACE_6'].astype(str).str.strip().str.upper() == 'YES'),
        ('Black', address_filtered['RACE_5'].astype(str).str.strip().str.upper() == 'YES'),
        ('White', address_filtered['RACE_4'].astype(str).str.strip().str.upper() == 'YES')
    ]
    
    for name, condition in metrics:
        metric_group = (
            address_filtered[condition]
            .groupby('ROUTE_ROOT')['id']
            .count()
            .reset_index()
            .rename(columns={'id': f'{name.lower()}_count'})
        )
        
        metric_percent = address_group[['ROUTE_ROOT', 'total_records']].merge(
            metric_group, how='left'
        ).fillna(0)
        
        metric_percent[f'% of {name}'] = (
            (metric_percent[f'{name.lower()}_count'] / metric_percent['total_records']) * 100
        ).apply(format_percentage)
        
        route_report_df = route_report_df.merge(
            metric_percent[['ROUTE_ROOT', f'% of {name}']], 
            how='left'
        ).fillna('0.0%')
    
    # Contest metrics only (removed follow-up section)
    contest_filtered = address_filtered.copy()
    contest_filtered['contest_yes'] = (
        contest_filtered['REGISTER_TO_WIN_YNCODE'].astype(str).str.strip().str.upper() == 'YES'
    )
    
    contest_group = contest_filtered.groupby('ROUTE_ROOT').agg(
        total_records=('id', 'count'),
        contest_yes_count=('contest_yes', 'sum')
    ).reset_index()
    
    contest_group['% of Contest - Yes'] = (
        (contest_group['contest_yes_count'] / contest_group['total_records']) * 100
    ).apply(format_percentage)
    
    route_report_df = route_report_df.merge(
        contest_group[['ROUTE_ROOT', '% of Contest - Yes']], 
        how='left'
    ).fillna('0.0%')
    
    contest_filtered['valid_contest'] = (
        contest_filtered['contest_yes'] &
        contest_filtered['REG_2_WIN_CONTACT_NAME'].notna() & 
        (contest_filtered['REG_2_WIN_CONTACT_NAME'].astype(str).str.strip() != '') &
        contest_filtered['REG_2_WIN_CONTACT_PHONE'].notna() & 
        (contest_filtered['REG_2_WIN_CONTACT_PHONE'].astype(str).str.strip() != '')
    )
    
    contest_valid_group = contest_filtered.groupby('ROUTE_ROOT').agg(
        total_records=('id', 'count'),
        valid_contest_count=('valid_contest', 'sum')
    ).reset_index()
    
    contest_valid_group['% of Contest - (Yes & Good Info)/Overall # of Records'] = (
        (contest_valid_group['valid_contest_count'] / contest_valid_group['total_records']) * 100
    ).apply(format_percentage)
    
    route_report_df = route_report_df.merge(
        contest_valid_group[['ROUTE_ROOT', '% of Contest - (Yes & Good Info)/Overall # of Records']],
        how='left'
    ).fillna('0.0%')
    
    # Add total row
    total_row = {
        'ROUTE_ROOT': 'Total',
        '# of Records': route_report_df['# of Records'].sum(),
        '# of Supervisor Delete': route_report_df['# of Supervisor Delete'].sum(),
        '# of Records Remove': route_report_df['# of Records Remove'].sum(),
        '# of Records Reviewed': route_report_df['# of Records Reviewed'].sum(),
        '# of Records Not Reviewed': route_report_df['# of Records Not Reviewed'].sum(),
        'SurveyTime (All)': calculate_avg_time(
            route_report_df.loc[route_report_df['ROUTE_ROOT'] != 'Total', 'SurveyTime (All)']),
        'SurveyTime (TripLogic)': calculate_avg_time(
            route_report_df.loc[route_report_df['ROUTE_ROOT'] != 'Total', 'SurveyTime (TripLogic)']),
        'SurveyTime (DemoLogic)': calculate_avg_time(
            route_report_df.loc[route_report_df['ROUTE_ROOT'] != 'Total', 'SurveyTime (DemoLogic)']),
    }
    
    # Add average percentages
    percent_cols = [col for col in route_report_df.columns if col.startswith('% of')]
    for col in percent_cols:
        avg = route_report_df.loc[route_report_df['ROUTE_ROOT'] != 'Total', col]\
              .str.rstrip('%').astype(float).mean()
        total_row[col] = format_percentage(avg)
    
    route_report_df = pd.concat([route_report_df, pd.DataFrame([total_row])], ignore_index=True)
    
    # Rename and reorder columns
    route_report_df = route_report_df.rename(columns={'ROUTE_ROOT': 'Route'})
    
    column_order = [
        'Route', '# of Records', '# of Supervisor Delete', '# of Records Remove',
        '# of Records Reviewed', '# of Records Not Reviewed', 'SurveyTime (All)',
        'SurveyTime (TripLogic)', 'SurveyTime (DemoLogic)', '% of Incomplete Home Address',
        '% of 0 Transfers', '% of Access Walk', '% of Egress Walk',
        '% of LowIncome', '% of No Income', '% of Hispanic', '% of Black', '% of White',
        '% of Contest - Yes', 
        '% of Contest - (Yes & Good Info)/Overall # of Records'
    ]
    
    return route_report_df[column_order]

def process_surveyor_date_data_kcata(df, elvis_df, survey_date_surveyor):
    """Process data for surveyor-level report with date"""
    df = df.copy()
    elvis_df = elvis_df.copy()
    
    # Clean and filter data
    df['INTERV_INIT'] = df['INTERV_INIT'].astype(str)
    filtered_df = df[df['INTERV_INIT'] != "999"]
    valid_surveys_df = filtered_df[filtered_df['HAVE_5_MIN_FOR_SURVECode'] == 1]
    print("\n[DEBUG] Columns in valid_surveys_df:", valid_surveys_df.columns.tolist())
    print("[DEBUG] First few rows:")
    print(valid_surveys_df.head())
    # Base counts
    record_counts = (
        valid_surveys_df
        .groupby('Date_Surveyor')['id']
        .count()
        .reset_index()
        .rename(columns={'id': '# of Records'})
    )
    
    summary_df = survey_date_surveyor[['Date_Surveyor']].merge(record_counts, on='Date_Surveyor', how='left').fillna(0)
    
    # Supervisor Deletes
    delete_counts = (
        filtered_df[
            (filtered_df['ELVIS_STATUS'] == 'Delete') & 
            (filtered_df['HAVE_5_MIN_FOR_SURVECode'] == 1)
        ]
        .groupby('Date_Surveyor')['id']
        .count()
        .reset_index()
        .rename(columns={'id': '# of Supervisor Delete'})
    )
    summary_df = summary_df.merge(delete_counts, how='left').fillna(0)
    
    # Records Remove
    remove_counts = (
        valid_surveys_df[valid_surveys_df['Final_Usage'] == 'Remove']
        .groupby('Date_Surveyor')['id']
        .count()
        .reset_index()
        .rename(columns={'id': '# of Records Remove'})
    )
    summary_df = summary_df.merge(remove_counts, how='left').fillna(0)
    
    # Records Reviewed/Not Reviewed
    reviewed_df = valid_surveys_df[valid_surveys_df['Final_Usage'].notna()]
    reviewed_counts = (
        reviewed_df
        .groupby('Date_Surveyor')['id']
        .count()
        .reset_index()
        .rename(columns={'id': '# of Records Reviewed'})
    )
    summary_df = summary_df.merge(reviewed_counts, how='left').fillna(0)
    
    not_reviewed_counts = (
        valid_surveys_df[valid_surveys_df['Final_Usage'].isna()]
        .groupby('Date_Surveyor')['id']
        .count()
        .reset_index()
        .rename(columns={'id': '# of Records Not Reviewed'})
    )
    summary_df = summary_df.merge(not_reviewed_counts, how='left').fillna(0)
    
    # Process elvis data for additional metrics
    elvis_df['INTERV_INIT'] = elvis_df['INTERV_INIT'].astype(str)
    address_filtered = elvis_df[
        (elvis_df['INTERV_INIT'] != "999") & 
        (elvis_df['HAVE_5_MIN_FOR_SURVECode'] == 1)
    ].copy()
    
    # Address completeness
    address_fields = [
        'HOME_ADDRESS_LAT', 'HOME_ADDRESS_LONG', 'HOME_ADDRESS_PLACE',
        'HOME_ADDRESS_ADDR', 'HOME_ADDRESS_CITY', 'HOME_ADDRESS_STATE', 'HOME_ADDRESS_ZIP'
    ]
    address_filtered['Incomplete_Address'] = address_filtered[address_fields].isnull().any(axis=1)
    
    address_group = address_filtered.groupby('Date_Surveyor').agg(
        total_records=('id', 'count'),
        incomplete_count=('Incomplete_Address', 'sum')
    ).reset_index()
    
    address_group['% of Incomplete Home Address'] = (
        (address_group['incomplete_count'] / address_group['total_records']) * 100
    ).round(2).astype(str) + '%'
    
    summary_df = summary_df.merge(
        address_group[['Date_Surveyor', '% of Incomplete Home Address']], 
        how='left'
    ).fillna('0.0%')
    
    # Survey times
    time_cols = ['HOMEADD_TIME', 'NOTE_TIME', 'REVIEWSCR_TIME']
    for col in time_cols:
        address_filtered[col] = pd.to_datetime(address_filtered[col], errors='coerce')
    
    address_filtered['SurveyTime (All)'] = (
        address_filtered['NOTE_TIME'] - address_filtered['HOMEADD_TIME']
    ).dt.total_seconds()
    address_filtered['SurveyTime (TripLogic)'] = (
        address_filtered['REVIEWSCR_TIME'] - address_filtered['HOMEADD_TIME']
    ).dt.total_seconds()
    address_filtered['SurveyTime (DemoLogic)'] = (
        address_filtered['NOTE_TIME'] - address_filtered['REVIEWSCR_TIME']
    ).dt.total_seconds()
    
    # Format times
    survey_time_group = address_filtered.groupby('Date_Surveyor').agg({
        'SurveyTime (All)': 'mean',
        'SurveyTime (TripLogic)': 'mean',
        'SurveyTime (DemoLogic)': 'mean'
    }).reset_index()
    
    for col in ['SurveyTime (All)', 'SurveyTime (TripLogic)', 'SurveyTime (DemoLogic)']:
        survey_time_group[col] = survey_time_group[col].apply(
            lambda x: "00:00:00" if pd.isna(x) or x < 0 else 
            f"{int(x//3600):02}:{int((x%3600)//60):02}:{int(x%60):02}"
        )
    
    summary_df = summary_df.merge(survey_time_group, how='left').fillna("00:00:00")
    
    # Other metrics (0 transfers, access walk, etc.)
    metrics = [
        ('0 Transfers', 
         (address_filtered['PREV_TRANSFERSCode'] == '(0) None') & 
         (address_filtered['NEXT_TRANSFERSCode'] == '(0) None')),
        ('Access Walk', address_filtered['VAL_ACCESS_WALK'] == 1),
        ('Egress Walk', address_filtered['VAL_EGRESS_WALK'] == 1),
        ('LowIncome', address_filtered['INCOMECode'].astype(str).isin(['1', '2', '3', '4'])),
        ('No Income', 
         (address_filtered['INCOMECode'].isna()) | 
         (address_filtered['INCOMECode'].astype(str) == 'REFUSED')),
        ('Hispanic', address_filtered['RACE_6'].astype(str).str.strip().str.upper() == 'YES'),
        ('Black', address_filtered['RACE_5'].astype(str).str.strip().str.upper() == 'YES'),
        ('White', address_filtered['RACE_4'].astype(str).str.strip().str.upper() == 'YES')
    ]
    
    for name, condition in metrics:
        metric_group = (
            address_filtered[condition]
            .groupby('Date_Surveyor')['id']
            .count()
            .reset_index()
            .rename(columns={'id': f'{name.lower()}_count'})
        )
        
        metric_percent = address_group[['Date_Surveyor', 'total_records']].merge(
            metric_group, how='left'
        ).fillna(0)
        
        metric_percent[f'% of {name}'] = (
            (metric_percent[f'{name.lower()}_count'] / metric_percent['total_records']) * 100
        ).apply(format_percentage)
        
        summary_df = summary_df.merge(
            metric_percent[['Date_Surveyor', f'% of {name}']], 
            how='left'
        ).fillna('0.0%')
    
    # Contest metrics only (removed follow-up section)
    contest_filtered = address_filtered.copy()
    contest_filtered['contest_yes'] = (
        contest_filtered['REGISTER_TO_WIN_YNCODE'].astype(str).str.strip().str.upper() == 'YES'
    )
    
    contest_group = contest_filtered.groupby('Date_Surveyor').agg(
        total_records=('id', 'count'),
        contest_yes_count=('contest_yes', 'sum')
    ).reset_index()
    
    contest_group['% of Contest - Yes'] = (
        (contest_group['contest_yes_count'] / contest_group['total_records']) * 100
    ).apply(format_percentage)
    
    summary_df = summary_df.merge(
        contest_group[['Date_Surveyor', '% of Contest - Yes']], 
        how='left'
    ).fillna('0.0%')
    
    contest_filtered['valid_contest'] = (
        contest_filtered['contest_yes'] &
        contest_filtered['REG_2_WIN_CONTACT_NAME'].notna() & 
        (contest_filtered['REG_2_WIN_CONTACT_NAME'].astype(str).str.strip() != '') &
        contest_filtered['REG_2_WIN_CONTACT_PHONE'].notna() & 
        (contest_filtered['REG_2_WIN_CONTACT_PHONE'].astype(str).str.strip() != '')
    )
    
    contest_valid_group = contest_filtered.groupby('Date_Surveyor').agg(
        total_records=('id', 'count'),
        valid_contest_count=('valid_contest', 'sum')
    ).reset_index()
    
    contest_valid_group['% of Contest - (Yes & Good Info)/Overall # of Records'] = (
        (contest_valid_group['valid_contest_count'] / contest_valid_group['total_records']) * 100
    ).apply(format_percentage)
    
    summary_df = summary_df.merge(
        contest_valid_group[['Date_Surveyor', '% of Contest - (Yes & Good Info)/Overall # of Records']],
        how='left'
    ).fillna('0.0%')
    
    # Add total row
    total_row = {
        'Date_Surveyor': 'Total',
        '# of Records': summary_df['# of Records'].sum(),
        '# of Supervisor Delete': summary_df['# of Supervisor Delete'].sum(),
        '# of Records Remove': summary_df['# of Records Remove'].sum(),
        '# of Records Reviewed': summary_df['# of Records Reviewed'].sum(),
        '# of Records Not Reviewed': summary_df['# of Records Not Reviewed'].sum(),
        'SurveyTime (All)': calculate_avg_time(
            summary_df.loc[summary_df['Date_Surveyor'] != 'Total', 'SurveyTime (All)']),
        'SurveyTime (TripLogic)': calculate_avg_time(
            summary_df.loc[summary_df['Date_Surveyor'] != 'Total', 'SurveyTime (TripLogic)']),
        'SurveyTime (DemoLogic)': calculate_avg_time(
            summary_df.loc[summary_df['Date_Surveyor'] != 'Total', 'SurveyTime (DemoLogic)']),
    }
    
    # Add average percentages
    percent_cols = [col for col in summary_df.columns if col.startswith('% of')]
    for col in percent_cols:
        avg = summary_df.loc[summary_df['Date_Surveyor'] != 'Total', col]\
              .str.rstrip('%').astype(float).mean()
        total_row[col] = format_percentage(avg)
    
    summary_df = pd.concat([summary_df, pd.DataFrame([total_row])], ignore_index=True)
    
    # Split Date_Surveyor back into Date and Surveyor columns
    summary_df[['Date', 'INTERV_INIT']] = summary_df['Date_Surveyor'].str.split('_', expand=True)
    def safe_date_parse(val):
        try:
            return pd.to_datetime(val).date()
        except:
            return val

    summary_df['Date'] = summary_df['Date'].apply(safe_date_parse)

    # summary_df['Date'] = pd.to_datetime(summary_df['Date']).dt.date
    
    column_order = [
        'Date', 'INTERV_INIT', '# of Records', '# of Supervisor Delete', '# of Records Remove',
        '# of Records Reviewed', '# of Records Not Reviewed', 'SurveyTime (All)',
        'SurveyTime (TripLogic)', 'SurveyTime (DemoLogic)', '% of Incomplete Home Address',
        '% of 0 Transfers', '% of Access Walk', '% of Egress Walk',
        '% of LowIncome', '% of No Income', '% of Hispanic', '% of Black', '% of White',
        '% of Contest - Yes', 
        '% of Contest - (Yes & Good Info)/Overall # of Records'
    ]
    
    return summary_df[column_order]

def process_route_date_data_kcata(df, elvis_df, survey_date_route):
    """Process data for route-level report with date"""
    df = df.copy()
    elvis_df = elvis_df.copy()

    filtered_df = df[df['INTERV_INIT'] != "999"]
    valid_surveys_df = filtered_df[filtered_df['HAVE_5_MIN_FOR_SURVECode'] == 1]
    
    # Base counts
    record_counts = (
        valid_surveys_df
        .groupby('Date_Route')['id']
        .count()
        .reset_index()
        .rename(columns={'id': '# of Records'})
    )
    
    route_report_df = survey_date_route[['Date_Route']].merge(record_counts, on='Date_Route', how='left').fillna(0)
    
    # Supervisor Deletes
    delete_counts = (
        filtered_df[
            (filtered_df['ELVIS_STATUS'] == 'Delete') & 
            (filtered_df['HAVE_5_MIN_FOR_SURVECode'] == 1)
        ]
        .groupby('Date_Route')['id']
        .count()
        .reset_index()
        .rename(columns={'id': '# of Supervisor Delete'})
    )
    route_report_df = route_report_df.merge(delete_counts, how='left').fillna(0)
    
    # Records Remove
    remove_counts = (
        valid_surveys_df[valid_surveys_df['Final_Usage'] == 'Remove']
        .groupby('Date_Route')['id']
        .count()
        .reset_index()
        .rename(columns={'id': '# of Records Remove'})
    )
    route_report_df = route_report_df.merge(remove_counts, how='left').fillna(0)
    
    # Records Reviewed/Not Reviewed
    reviewed_df = valid_surveys_df[valid_surveys_df['Final_Usage'].notna()]
    reviewed_counts = (
        reviewed_df
        .groupby('Date_Route')['id']
        .count()
        .reset_index()
        .rename(columns={'id': '# of Records Reviewed'})
    )
    route_report_df = route_report_df.merge(reviewed_counts, how='left').fillna(0)
    
    not_reviewed_counts = (
        valid_surveys_df[valid_surveys_df['Final_Usage'].isna()]
        .groupby('Date_Route')['id']
        .count()
        .reset_index()
        .rename(columns={'id': '# of Records Not Reviewed'})
    )
    route_report_df = route_report_df.merge(not_reviewed_counts, how='left').fillna(0)
    
    # Process elvis data for additional metrics
    elvis_df['Date_Route'] = clean_route_name(elvis_df['ROUTE_SURVEYED'])
    elvis_df['INTERV_INIT'] = elvis_df['INTERV_INIT'].astype(str)
    
    address_filtered = elvis_df[
        (elvis_df['INTERV_INIT'] != "999") & 
        (elvis_df['HAVE_5_MIN_FOR_SURVECode'] == 1)
    ].copy()
    
    # Address completeness
    address_fields = [
        'HOME_ADDRESS_LAT', 'HOME_ADDRESS_LONG', 'HOME_ADDRESS_PLACE',
        'HOME_ADDRESS_ADDR', 'HOME_ADDRESS_CITY', 'HOME_ADDRESS_STATE', 'HOME_ADDRESS_ZIP'
    ]
    address_filtered['Incomplete_Address'] = address_filtered[address_fields].isnull().any(axis=1)
    
    address_group = address_filtered.groupby('Date_Route').agg(
        total_records=('id', 'count'),
        incomplete_count=('Incomplete_Address', 'sum')
    ).reset_index()
    
    address_group['% of Incomplete Home Address'] = (
        (address_group['incomplete_count'] / address_group['total_records']) * 100
    ).apply(format_percentage)
    
    route_report_df = route_report_df.merge(
        address_group[['Date_Route', '% of Incomplete Home Address']], 
        how='left'
    ).fillna('0.0%')
    
    # Survey times
    time_cols = ['HOMEADD_TIME', 'NOTE_TIME', 'REVIEWSCR_TIME']
    for col in time_cols:
        address_filtered[col] = pd.to_datetime(address_filtered[col], errors='coerce')
    
    address_filtered['SurveyTime (All)'] = (
        address_filtered['NOTE_TIME'] - address_filtered['HOMEADD_TIME']
    ).dt.total_seconds()
    address_filtered['SurveyTime (TripLogic)'] = (
        address_filtered['REVIEWSCR_TIME'] - address_filtered['HOMEADD_TIME']
    ).dt.total_seconds()
    address_filtered['SurveyTime (DemoLogic)'] = (
        address_filtered['NOTE_TIME'] - address_filtered['REVIEWSCR_TIME']
    ).dt.total_seconds()
    
    # Format times
    survey_time_group = address_filtered.groupby('Date_Route').agg({
        'SurveyTime (All)': 'mean',
        'SurveyTime (TripLogic)': 'mean',
        'SurveyTime (DemoLogic)': 'mean'
    }).reset_index()
    
    for col in ['SurveyTime (All)', 'SurveyTime (TripLogic)', 'SurveyTime (DemoLogic)']:
        survey_time_group[col] = survey_time_group[col].apply(
            lambda x: "00:00:00" if pd.isna(x) or x < 0 else 
            f"{int(x//3600):02}:{int((x%3600)//60):02}:{int(x%60):02}"
        )
    
    route_report_df = route_report_df.merge(survey_time_group, how='left').fillna("00:00:00")
    
    # Other metrics (0 transfers, access walk, etc.)
    metrics = [
        ('0 Transfers', 
         (address_filtered['PREV_TRANSFERSCode'] == '(0) None') & 
         (address_filtered['NEXT_TRANSFERSCode'] == '(0) None')),
        ('Access Walk', address_filtered['VAL_ACCESS_WALK'] == 1),
        ('Egress Walk', address_filtered['VAL_EGRESS_WALK'] == 1),
        ('LowIncome', address_filtered['INCOMECode'].astype(str).isin(['1', '2', '3', '4'])),
        ('No Income', 
         (address_filtered['INCOMECode'].isna()) | 
         (address_filtered['INCOMECode'].astype(str) == 'REFUSED')),
        ('Hispanic', address_filtered['RACE_6'].astype(str).str.strip().str.upper() == 'YES'),
        ('Black', address_filtered['RACE_5'].astype(str).str.strip().str.upper() == 'YES'),
        ('White', address_filtered['RACE_4'].astype(str).str.strip().str.upper() == 'YES')
    ]
    
    for name, condition in metrics:
        metric_group = (
            address_filtered[condition]
            .groupby('Date_Route')['id']
            .count()
            .reset_index()
            .rename(columns={'id': f'{name.lower()}_count'})
        )
        
        metric_percent = address_group[['Date_Route', 'total_records']].merge(
            metric_group, how='left'
        ).fillna(0)
        
        metric_percent[f'% of {name}'] = (
            (metric_percent[f'{name.lower()}_count'] / metric_percent['total_records']) * 100
        ).apply(format_percentage)
        
        route_report_df = route_report_df.merge(
            metric_percent[['Date_Route', f'% of {name}']], 
            how='left'
        ).fillna('0.0%')
    
    # Contest metrics (keeping only contest-related metrics)
    contest_filtered = elvis_df[
        (elvis_df['INTERV_INIT'] != "999") & 
        (elvis_df['HAVE_5_MIN_FOR_SURVECode'] == 1)
    ].copy()
    contest_filtered['Date_Route'] = clean_route_name(contest_filtered['ROUTE_SURVEYED'])
    
    contest_filtered['contest_yes'] = (
        contest_filtered['REGISTER_TO_WIN_YNCODE'].astype(str).str.strip().str.upper() == 'YES'
    )
    
    contest_group = contest_filtered.groupby('Date_Route').agg(
        total_records=('id', 'count'),
        contest_yes_count=('contest_yes', 'sum')
    ).reset_index()
    
    contest_group['% of Contest - Yes'] = (
        (contest_group['contest_yes_count'] / contest_group['total_records']) * 100
    ).apply(format_percentage)
    
    route_report_df = route_report_df.merge(
        contest_group[['Date_Route', '% of Contest - Yes']], 
        how='left'
    ).fillna('0.0%')
    
    contest_filtered['valid_contest'] = (
        contest_filtered['contest_yes'] &
        contest_filtered['REG_2_WIN_CONTACT_NAME'].notna() & 
        (contest_filtered['REG_2_WIN_CONTACT_NAME'].astype(str).str.strip() != '') &
        contest_filtered['REG_2_WIN_CONTACT_PHONE'].notna() & 
        (contest_filtered['REG_2_WIN_CONTACT_PHONE'].astype(str).str.strip() != '')
    )
    
    contest_valid_group = contest_filtered.groupby('Date_Route').agg(
        total_records=('id', 'count'),
        valid_contest_count=('valid_contest', 'sum')
    ).reset_index()
    
    contest_valid_group['% of Contest - (Yes & Good Info)/Overall # of Records'] = (
        (contest_valid_group['valid_contest_count'] / contest_valid_group['total_records']) * 100
    ).apply(format_percentage)
    
    route_report_df = route_report_df.merge(
        contest_valid_group[['Date_Route', '% of Contest - (Yes & Good Info)/Overall # of Records']],
        how='left'
    ).fillna('0.0%')
    
    # Add total row
    total_row = {
        'Date_Route': 'Total',
        '# of Records': route_report_df['# of Records'].sum(),
        '# of Supervisor Delete': route_report_df['# of Supervisor Delete'].sum(),
        '# of Records Remove': route_report_df['# of Records Remove'].sum(),
        '# of Records Reviewed': route_report_df['# of Records Reviewed'].sum(),
        '# of Records Not Reviewed': route_report_df['# of Records Not Reviewed'].sum(),
        'SurveyTime (All)': calculate_avg_time(
            route_report_df.loc[route_report_df['Date_Route'] != 'Total', 'SurveyTime (All)']),
        'SurveyTime (TripLogic)': calculate_avg_time(
            route_report_df.loc[route_report_df['Date_Route'] != 'Total', 'SurveyTime (TripLogic)']),
        'SurveyTime (DemoLogic)': calculate_avg_time(
            route_report_df.loc[route_report_df['Date_Route'] != 'Total', 'SurveyTime (DemoLogic)']),
    }
    
    # Add average percentages
    percent_cols = [col for col in route_report_df.columns if col.startswith('% of')]
    for col in percent_cols:
        avg = route_report_df.loc[route_report_df['Date_Route'] != 'Total', col]\
              .str.rstrip('%').astype(float).mean()
        total_row[col] = format_percentage(avg)
    
    route_report_df = pd.concat([route_report_df, pd.DataFrame([total_row])], ignore_index=True)
    
    # Split Date_Route back into Date and Route columns
    route_report_df[['Date', 'ROUTE_ROOT']] = route_report_df['Date_Route'].str.split('_', expand=True)
    def safe_date_parse(val):
        try:
            return pd.to_datetime(val).date()
        except:
            return val

    route_report_df['Date'] = route_report_df['Date'].apply(safe_date_parse)
    
    column_order = [
        'Date', 'ROUTE_ROOT', '# of Records', '# of Supervisor Delete', '# of Records Remove',
        '# of Records Reviewed', '# of Records Not Reviewed', 'SurveyTime (All)',
        'SurveyTime (TripLogic)', 'SurveyTime (DemoLogic)', '% of Incomplete Home Address',
        '% of 0 Transfers', '% of Access Walk', '% of Egress Walk',
        '% of LowIncome', '% of No Income', '% of Hispanic', '% of Black', '% of White',
        '% of Contest - Yes', '% of Contest - (Yes & Good Info)/Overall # of Records'
    ]
    
    return route_report_df[column_order]




def process_surveyor_date_data(df, elvis_df, survey_date_surveyor):
    """Process data for surveyor-level report"""
    # Clean and filter data

    df = df.copy()
    elvis_df = elvis_df.copy()
    # df['INTERV_INIT'] = df['INTERV_INIT'].astype(str)
    filtered_df = df[df['INTERV_INIT'] != "999"]
    # filtered_df = df[df['INTERV_INIT'] != 999]
    valid_surveys_df = filtered_df[filtered_df['HAVE_5_MIN_FOR_SURVECode'] == 1]
    
    # Base counts
    record_counts = (
        valid_surveys_df
        .groupby('Date_Surveyor')['id']
        .count()
        .reset_index()
        .rename(columns={'id': '# of Records'})
    )
    
    summary_df = survey_date_surveyor[['Date_Surveyor']].merge(record_counts, on='Date_Surveyor', how='left').fillna(0)
    
    # Supervisor Deletes
    delete_counts = (
        filtered_df[
            (filtered_df['ELVIS_STATUS'] == 'Delete') & 
            (filtered_df['HAVE_5_MIN_FOR_SURVECode'] == 1)
        ]
        .groupby('Date_Surveyor')['id']
        .count()
        .reset_index()
        .rename(columns={'id': '# of Supervisor Delete'})
    )
    summary_df = summary_df.merge(delete_counts, how='left').fillna(0)
    
    # Records Remove
    remove_counts = (
        valid_surveys_df[valid_surveys_df['Final_Usage'] == 'Remove']
        .groupby('Date_Surveyor')['id']
        .count()
        .reset_index()
        .rename(columns={'id': '# of Records Remove'})
    )
    summary_df = summary_df.merge(remove_counts, how='left').fillna(0)
    
    # Records Reviewed/Not Reviewed
    reviewed_df = valid_surveys_df[valid_surveys_df['Final_Usage'].notna()]
    reviewed_counts = (
        reviewed_df
        .groupby('Date_Surveyor')['id']
        .count()
        .reset_index()
        .rename(columns={'id': '# of Records Reviewed'})
    )
    summary_df = summary_df.merge(reviewed_counts, how='left').fillna(0)
    
    not_reviewed_counts = (
        valid_surveys_df[valid_surveys_df['Final_Usage'].isna()]
        .groupby('Date_Surveyor')['id']
        .count()
        .reset_index()
        .rename(columns={'id': '# of Records Not Reviewed'})
    )
    summary_df = summary_df.merge(not_reviewed_counts, how='left').fillna(0)
    
    # Process elvis data for additional metrics
    elvis_df['INTERV_INIT'] = elvis_df['INTERV_INIT'].astype(str)
    elvis_df['HAVE_5_MIN_FOR_SURVE_Code_'] = elvis_df['HAVE_5_MIN_FOR_SURVE_Code_'].astype(str)
    address_filtered = elvis_df[
        (elvis_df['INTERV_INIT'] != "999") & 
        # (elvis_df['INTERV_INIT'] != 999) & 
        (elvis_df['HAVE_5_MIN_FOR_SURVE_Code_'] == '1')
    ].copy()
    
    # Address completeness
    address_fields = [
        'HOME_ADDRESS_LAT_', 'HOME_ADDRESS_LONG_', 'HOME_ADDRESS_PLACE_',
        'HOME_ADDRESS_ADDR_', 'HOME_ADDRESS_CITY_', 'HOME_ADDRESS_STATE_', 'HOME_ADDRESS_ZIP_'
    ]
    address_filtered['Incomplete_Address'] = address_filtered[address_fields].isnull().any(axis=1)
    
    address_group = address_filtered.groupby('Date_Surveyor').agg(
        total_records=('id', 'count'),
        incomplete_count=('Incomplete_Address', 'sum')
    ).reset_index()
    
    address_group['% of Incomplete Home Address'] = (
        (address_group['incomplete_count'] / address_group['total_records']) * 100
    ).round(2).astype(str) + '%'
    
    summary_df = summary_df.merge(
        address_group[['Date_Surveyor', '% of Incomplete Home Address']], 
        how='left'
    ).fillna('0.0%')
    
    # Homeless percentage
    homeless_group = (
        address_filtered[address_filtered['HOME_ADDRESS_HOMELESS_'] == 'YES']
        .groupby('Date_Surveyor')['id']
        .count()
        .reset_index()
        .rename(columns={'id': 'homeless_count'})
    )
    
    homeless_percent = address_group[['Date_Surveyor', 'total_records']].merge(
        homeless_group, how='left'
    ).fillna(0)
    
    homeless_percent['% of Homeless'] = (
        (homeless_percent['homeless_count'] / homeless_percent['total_records']) * 100
    ).apply(format_percentage)
    
    summary_df = summary_df.merge(
        homeless_percent[['Date_Surveyor', '% of Homeless']], 
        how='left'
    ).fillna('0.0%')
    
    # Survey times
    time_cols = ['HOMEADD_TIME', 'NOTE_TIME', 'REVIEWSCR_TIME']
    for col in time_cols:
        address_filtered[col] = pd.to_datetime(address_filtered[col], errors='coerce')
    
    address_filtered['SurveyTime (All)'] = (
        address_filtered['NOTE_TIME'] - address_filtered['HOMEADD_TIME']
    ).dt.total_seconds()
    address_filtered['SurveyTime (TripLogic)'] = (
        address_filtered['REVIEWSCR_TIME'] - address_filtered['HOMEADD_TIME']
    ).dt.total_seconds()
    address_filtered['SurveyTime (DemoLogic)'] = (
        address_filtered['NOTE_TIME'] - address_filtered['REVIEWSCR_TIME']
    ).dt.total_seconds()
    
    # Format times
    survey_time_group = address_filtered.groupby('Date_Surveyor').agg({
        'SurveyTime (All)': 'mean',
        'SurveyTime (TripLogic)': 'mean',
        'SurveyTime (DemoLogic)': 'mean'
    }).reset_index()
    
    for col in ['SurveyTime (All)', 'SurveyTime (TripLogic)', 'SurveyTime (DemoLogic)']:
        survey_time_group[col] = survey_time_group[col].apply(
            lambda x: "00:00:00" if pd.isna(x) or x < 0 else 
            f"{int(x//3600):02}:{int((x%3600)//60):02}:{int(x%60):02}"
        )
    
    summary_df = summary_df.merge(survey_time_group, how='left').fillna("00:00:00")
    
    # Other metrics (0 transfers, access walk, etc.)
    metrics = [
        ('0 Transfers', 
         (address_filtered['PREV_TRANSFERS'] == '(0) None') & 
         (address_filtered['NEXT_TRANSFERS'] == '(0) None')),
        ('Access Walk', address_filtered['VAL_ACCESS_WALK'] == 1),
        ('Egress Walk', address_filtered['VAL_EGRESS_WALK'] == 1),
        ('LowIncome', address_filtered['INCOME_Code_'].astype(str).isin(['1', '2', '3', '4'])),
        ('No Income', 
         (address_filtered['INCOME_Code_'].isna()) | 
         (address_filtered['INCOME_Code_'].astype(str) == 'REFUSED')),
        ('Hispanic', address_filtered['RACE_6_'].astype(str).str.strip().str.upper() == 'YES'),
        ('Black', address_filtered['RACE_5_'].astype(str).str.strip().str.upper() == 'YES'),
        ('White', address_filtered['RACE_4_'].astype(str).str.strip().str.upper() == 'YES')
    ]
    
    for name, condition in metrics:
        metric_group = (
            address_filtered[condition]
            .groupby('Date_Surveyor')['id']
            .count()
            .reset_index()
            .rename(columns={'id': f'{name.lower()}_count'})
        )
        
        metric_percent = address_group[['Date_Surveyor', 'total_records']].merge(
            metric_group, how='left'
        ).fillna(0)
        
        metric_percent[f'% of {name}'] = (
            (metric_percent[f'{name.lower()}_count'] / metric_percent['total_records']) * 100
        ).apply(format_percentage)
        
        summary_df = summary_df.merge(
            metric_percent[['Date_Surveyor', f'% of {name}']], 
            how='left'
        ).fillna('0.0%')
    elvis_df['INTERV_INIT'] = elvis_df['INTERV_INIT'].astype(str)
    elvis_df['HAVE_5_MIN_FOR_SURVE_Code_'] = elvis_df['HAVE_5_MIN_FOR_SURVE_Code_'].astype(str)
    # Follow-up and contest metrics
    followup_filtered = elvis_df[
        (elvis_df['INTERV_INIT'] != "999") & 
        # (elvis_df['INTERV_INIT'] != 999) & 
        (elvis_df['HAVE_5_MIN_FOR_SURVE_Code_'] == '1')
    ].copy()
    
    followup_filtered['has_followup'] = (
        followup_filtered['FOLLOWUP_SMS_NAME_'].notna() & 
        (followup_filtered['FOLLOWUP_SMS_NAME_'].astype(str).str.strip() != '') &
        followup_filtered['FOLLOWUP_SMS_PHONE_'].notna() & 
        (followup_filtered['FOLLOWUP_SMS_PHONE_'].astype(str).str.strip() != '')
    )
    
    followup_group = followup_filtered.groupby('Date_Surveyor').agg(
        total_records=('id', 'count'),
        followup_count=('has_followup', 'sum')
    ).reset_index()
    
    followup_group['% of Follow-Up Survey'] = (
        (followup_group['followup_count'] / followup_group['total_records']) * 100
    ).apply(format_percentage)
    
    summary_df = summary_df.merge(
        followup_group[['Date_Surveyor', '% of Follow-Up Survey']], 
        how='left'
    ).fillna('0.0%')
    
    # Contest metrics
    contest_filtered = followup_filtered.copy()
    contest_filtered['contest_yes'] = (
        contest_filtered['REGISTER_TO_WIN_Y_N'].astype(str).str.strip().str.upper() == 'YES'
    )
    
    contest_group = contest_filtered.groupby('Date_Surveyor').agg(
        total_records=('id', 'count'),
        contest_yes_count=('contest_yes', 'sum')
    ).reset_index()
    
    contest_group['% of Contest - Yes'] = (
        (contest_group['contest_yes_count'] / contest_group['total_records']) * 100
    ).apply(format_percentage)
    
    summary_df = summary_df.merge(
        contest_group[['Date_Surveyor', '% of Contest - Yes']], 
        how='left'
    ).fillna('0.0%')
    
    contest_filtered['valid_contest'] = (
        contest_filtered['contest_yes'] &
        contest_filtered['REG2WIN_CONTACT_NAME_'].notna() & 
        (contest_filtered['REG2WIN_CONTACT_NAME_'].astype(str).str.strip() != '') &
        contest_filtered['REG2WIN_CONTACT_PHONE_'].notna() & 
        (contest_filtered['REG2WIN_CONTACT_PHONE_'].astype(str).str.strip() != '')
    )
    
    contest_valid_group = contest_filtered.groupby('Date_Surveyor').agg(
        total_records=('id', 'count'),
        valid_contest_count=('valid_contest', 'sum')
    ).reset_index()
    
    contest_valid_group['% of Contest - (Yes & Good Info)/Overall # of Records'] = (
        (contest_valid_group['valid_contest_count'] / contest_valid_group['total_records']) * 100
    ).apply(format_percentage)
    
    summary_df = summary_df.merge(
        contest_valid_group[['Date_Surveyor', '% of Contest - (Yes & Good Info)/Overall # of Records']],
        how='left'
    ).fillna('0.0%')
    
    # Add total row
    total_row = {
        'Date_Surveyor': 'Total',
        '# of Records': summary_df['# of Records'].sum(),
        '# of Supervisor Delete': summary_df['# of Supervisor Delete'].sum(),
        '# of Records Remove': summary_df['# of Records Remove'].sum(),
        '# of Records Reviewed': summary_df['# of Records Reviewed'].sum(),
        '# of Records Not Reviewed': summary_df['# of Records Not Reviewed'].sum(),
        'SurveyTime (All)': calculate_avg_time(
            summary_df.loc[summary_df['Date_Surveyor'] != 'Total', 'SurveyTime (All)']),
        'SurveyTime (TripLogic)': calculate_avg_time(
            summary_df.loc[summary_df['Date_Surveyor'] != 'Total', 'SurveyTime (TripLogic)']),
        'SurveyTime (DemoLogic)': calculate_avg_time(
            summary_df.loc[summary_df['Date_Surveyor'] != 'Total', 'SurveyTime (DemoLogic)']),
    }
    
    # Add average percentages
    percent_cols = [col for col in summary_df.columns if col.startswith('% of')]
    for col in percent_cols:
        avg = summary_df.loc[summary_df['Date_Surveyor'] != 'Total', col]\
              .str.rstrip('%').astype(float).mean()
        total_row[col] = format_percentage(avg)
    
    summary_df = pd.concat([summary_df, pd.DataFrame([total_row])], ignore_index=True)
    column_order = [
        'Date_Surveyor', '# of Records', '# of Supervisor Delete', '# of Records Remove',
        '# of Records Reviewed', '# of Records Not Reviewed', 'SurveyTime (All)',
        'SurveyTime (TripLogic)', 'SurveyTime (DemoLogic)', '% of Incomplete Home Address',
        '% of Homeless', '% of 0 Transfers', '% of Access Walk', '% of Egress Walk',
        '% of LowIncome', '% of No Income', '% of Hispanic', '% of Black', '% of White',
        '% of Follow-Up Survey', '% of Contest - Yes', 
        '% of Contest - (Yes & Good Info)/Overall # of Records'
    ]
    
    return summary_df[column_order]

def process_route_date_data(df, elvis_df, survey_date_route):
    """Process data for route-level report"""
    df = df.copy()
    elvis_df = elvis_df.copy()

    filtered_df = df[df['INTERV_INIT'] != "999"]
    # filtered_df = df[df['INTERV_INIT'] != 999]
    valid_surveys_df = filtered_df[filtered_df['HAVE_5_MIN_FOR_SURVECode'] == 1]
    
    # Base counts
    record_counts = (
        valid_surveys_df
        .groupby('Date_Route')['id']
        .count()
        .reset_index()
        .rename(columns={'id': '# of Records'})
    )
    
    route_report_df = survey_date_route[['Date_Route']].merge(record_counts, on='Date_Route', how='left').fillna(0)
    
    # Supervisor Deletes
    delete_counts = (
        filtered_df[
            (filtered_df['ELVIS_STATUS'] == 'Delete') & 
            (filtered_df['HAVE_5_MIN_FOR_SURVECode'] == 1)
        ]
        .groupby('Date_Route')['id']
        .count()
        .reset_index()
        .rename(columns={'id': '# of Supervisor Delete'})
    )
    route_report_df = route_report_df.merge(delete_counts, how='left').fillna(0)
    
    # Records Remove
    remove_counts = (
        valid_surveys_df[valid_surveys_df['Final_Usage'] == 'Remove']
        .groupby('Date_Route')['id']
        .count()
        .reset_index()
        .rename(columns={'id': '# of Records Remove'})
    )
    route_report_df = route_report_df.merge(remove_counts, how='left').fillna(0)
    
    # Records Reviewed/Not Reviewed
    reviewed_df = valid_surveys_df[valid_surveys_df['Final_Usage'].notna()]
    reviewed_counts = (
        reviewed_df
        .groupby('Date_Route')['id']
        .count()
        .reset_index()
        .rename(columns={'id': '# of Records Reviewed'})
    )
    route_report_df = route_report_df.merge(reviewed_counts, how='left').fillna(0)
    
    not_reviewed_counts = (
        valid_surveys_df[valid_surveys_df['Final_Usage'].isna()]
        .groupby('Date_Route')['id']
        .count()
        .reset_index()
        .rename(columns={'id': '# of Records Not Reviewed'})
    )
    route_report_df = route_report_df.merge(not_reviewed_counts, how='left').fillna(0)
    
    # Process elvis data for additional metrics
    elvis_df['Date_Route'] = clean_route_name(elvis_df['ROUTE_SURVEYED'])
    elvis_df['INTERV_INIT'] = elvis_df['INTERV_INIT'].astype(str)
    elvis_df['HAVE_5_MIN_FOR_SURVE_Code_'] = elvis_df['HAVE_5_MIN_FOR_SURVE_Code_'].astype(str)
    address_filtered = elvis_df[
        (elvis_df['INTERV_INIT'] != "999") & 
        # (elvis_df['INTERV_INIT'] != 999) & 
        (elvis_df['HAVE_5_MIN_FOR_SURVE_Code_'] == '1')
    ].copy()
    
    # Address completeness
    address_fields = [
        'HOME_ADDRESS_LAT_', 'HOME_ADDRESS_LONG_', 'HOME_ADDRESS_PLACE_',
        'HOME_ADDRESS_ADDR_', 'HOME_ADDRESS_CITY_', 'HOME_ADDRESS_STATE_', 'HOME_ADDRESS_ZIP_'
    ]
    address_filtered['Incomplete_Address'] = address_filtered[address_fields].isnull().any(axis=1)
    
    address_group = address_filtered.groupby('Date_Route').agg(
        total_records=('id', 'count'),
        incomplete_count=('Incomplete_Address', 'sum')
    ).reset_index()
    
    address_group['% of Incomplete Home Address'] = (
        (address_group['incomplete_count'] / address_group['total_records']) * 100
    ).apply(format_percentage)
    
    route_report_df = route_report_df.merge(
        address_group[['Date_Route', '% of Incomplete Home Address']], 
        how='left'
    ).fillna('0.0%')
    
    # Homeless percentage
    homeless_group = (
        address_filtered[address_filtered['HOME_ADDRESS_HOMELESS_'] == 'YES']
        .groupby('Date_Route')['id']
        .count()
        .reset_index()
        .rename(columns={'id': 'homeless_count'})
    )
    
    homeless_percent = address_group[['Date_Route', 'total_records']].merge(
        homeless_group, how='left'
    ).fillna(0)
    
    homeless_percent['% of Homeless'] = (
        (homeless_percent['homeless_count'] / homeless_percent['total_records']) * 100
    ).apply(format_percentage)
    
    route_report_df = route_report_df.merge(
        homeless_percent[['Date_Route', '% of Homeless']], 
        how='left'
    ).fillna('0.0%')
    
    # Survey times
    time_cols = ['HOMEADD_TIME', 'NOTE_TIME', 'REVIEWSCR_TIME']
    for col in time_cols:
        address_filtered[col] = pd.to_datetime(address_filtered[col], errors='coerce')
    
    address_filtered['SurveyTime (All)'] = (
        address_filtered['NOTE_TIME'] - address_filtered['HOMEADD_TIME']
    ).dt.total_seconds()
    address_filtered['SurveyTime (TripLogic)'] = (
        address_filtered['REVIEWSCR_TIME'] - address_filtered['HOMEADD_TIME']
    ).dt.total_seconds()
    address_filtered['SurveyTime (DemoLogic)'] = (
        address_filtered['NOTE_TIME'] - address_filtered['REVIEWSCR_TIME']
    ).dt.total_seconds()
    
    # Format times
    survey_time_group = address_filtered.groupby('Date_Route').agg({
        'SurveyTime (All)': 'mean',
        'SurveyTime (TripLogic)': 'mean',
        'SurveyTime (DemoLogic)': 'mean'
    }).reset_index()
    
    for col in ['SurveyTime (All)', 'SurveyTime (TripLogic)', 'SurveyTime (DemoLogic)']:
        survey_time_group[col] = survey_time_group[col].apply(
            lambda x: "00:00:00" if pd.isna(x) or x < 0 else 
            f"{int(x//3600):02}:{int((x%3600)//60):02}:{int(x%60):02}"
        )
    
    route_report_df = route_report_df.merge(survey_time_group, how='left').fillna("00:00:00")
    
    # Other metrics (0 transfers, access walk, etc.)
    metrics = [
        ('0 Transfers', 
         (address_filtered['PREV_TRANSFERS'] == '(0) None') & 
         (address_filtered['NEXT_TRANSFERS'] == '(0) None')),
        ('Access Walk', address_filtered['VAL_ACCESS_WALK'] == 1),
        ('Egress Walk', address_filtered['VAL_EGRESS_WALK'] == 1),
        ('LowIncome', address_filtered['INCOME_Code_'].astype(str).isin(['1', '2', '3', '4'])),
        ('No Income', 
         (address_filtered['INCOME_Code_'].isna()) | 
         (address_filtered['INCOME_Code_'].astype(str) == 'REFUSED')),
        ('Hispanic', address_filtered['RACE_6_'].astype(str).str.strip().str.upper() == 'YES'),
        ('Black', address_filtered['RACE_5_'].astype(str).str.strip().str.upper() == 'YES'),
        ('White', address_filtered['RACE_4_'].astype(str).str.strip().str.upper() == 'YES')
    ]
    
    for name, condition in metrics:
        metric_group = (
            address_filtered[condition]
            .groupby('Date_Route')['id']
            .count()
            .reset_index()
            .rename(columns={'id': f'{name.lower()}_count'})
        )
        
        metric_percent = address_group[['Date_Route', 'total_records']].merge(
            metric_group, how='left'
        ).fillna(0)
        
        metric_percent[f'% of {name}'] = (
            (metric_percent[f'{name.lower()}_count'] / metric_percent['total_records']) * 100
        ).apply(format_percentage)
        
        route_report_df = route_report_df.merge(
            metric_percent[['Date_Route', f'% of {name}']], 
            how='left'
        ).fillna('0.0%')
    elvis_df['HAVE_5_MIN_FOR_SURVE_Code_'] = elvis_df['HAVE_5_MIN_FOR_SURVE_Code_'].astype(str)
    elvis_df['INTERV_INIT'] = elvis_df['INTERV_INIT'].astype(str)
    # Follow-up and contest metrics
    followup_filtered = elvis_df[
        (elvis_df['INTERV_INIT'] != "999") & 
        (elvis_df['HAVE_5_MIN_FOR_SURVE_Code_'] == '1')
    ].copy()
    followup_filtered['Date_Route'] = clean_route_name(followup_filtered['ROUTE_SURVEYED'])
    
    followup_filtered['has_followup'] = (
        followup_filtered['FOLLOWUP_SMS_NAME_'].notna() & 
        (followup_filtered['FOLLOWUP_SMS_NAME_'].astype(str).str.strip() != '') &
        followup_filtered['FOLLOWUP_SMS_PHONE_'].notna() & 
        (followup_filtered['FOLLOWUP_SMS_PHONE_'].astype(str).str.strip() != '')
    )
    
    followup_group = followup_filtered.groupby('Date_Route').agg(
        total_records=('id', 'count'),
        followup_count=('has_followup', 'sum')
    ).reset_index()
    
    followup_group['% of Follow-Up Survey'] = (
        (followup_group['followup_count'] / followup_group['total_records']) * 100
    ).apply(format_percentage)
    
    route_report_df = route_report_df.merge(
        followup_group[['Date_Route', '% of Follow-Up Survey']], 
        how='left'
    ).fillna('0.0%')
    
    # Contest metrics
    contest_filtered = followup_filtered.copy()
    contest_filtered['contest_yes'] = (
        contest_filtered['REGISTER_TO_WIN_Y_N'].astype(str).str.strip().str.upper() == 'YES'
    )
    
    contest_group = contest_filtered.groupby('Date_Route').agg(
        total_records=('id', 'count'),
        contest_yes_count=('contest_yes', 'sum')
    ).reset_index()
    
    contest_group['% of Contest - Yes'] = (
        (contest_group['contest_yes_count'] / contest_group['total_records']) * 100
    ).apply(format_percentage)
    
    route_report_df = route_report_df.merge(
        contest_group[['Date_Route', '% of Contest - Yes']], 
        how='left'
    ).fillna('0.0%')
    
    contest_filtered['valid_contest'] = (
        contest_filtered['contest_yes'] &
        contest_filtered['REG2WIN_CONTACT_NAME_'].notna() & 
        (contest_filtered['REG2WIN_CONTACT_NAME_'].astype(str).str.strip() != '') &
        contest_filtered['REG2WIN_CONTACT_PHONE_'].notna() & 
        (contest_filtered['REG2WIN_CONTACT_PHONE_'].astype(str).str.strip() != '')
    )
    
    contest_valid_group = contest_filtered.groupby('Date_Route').agg(
        total_records=('id', 'count'),
        valid_contest_count=('valid_contest', 'sum')
    ).reset_index()
    
    contest_valid_group['% of Contest - (Yes & Good Info)/Overall # of Records'] = (
        (contest_valid_group['valid_contest_count'] / contest_valid_group['total_records']) * 100
    ).apply(format_percentage)
    
    route_report_df = route_report_df.merge(
        contest_valid_group[['Date_Route', '% of Contest - (Yes & Good Info)/Overall # of Records']],
        how='left'
    ).fillna('0.0%')
    
    # Add total row
    total_row = {
        'Date_Route': 'Total',
        '# of Records': route_report_df['# of Records'].sum(),
        '# of Supervisor Delete': route_report_df['# of Supervisor Delete'].sum(),
        '# of Records Remove': route_report_df['# of Records Remove'].sum(),
        '# of Records Reviewed': route_report_df['# of Records Reviewed'].sum(),
        '# of Records Not Reviewed': route_report_df['# of Records Not Reviewed'].sum(),
        'SurveyTime (All)': calculate_avg_time(
            route_report_df.loc[route_report_df['Date_Route'] != 'Total', 'SurveyTime (All)']),
        'SurveyTime (TripLogic)': calculate_avg_time(
            route_report_df.loc[route_report_df['Date_Route'] != 'Total', 'SurveyTime (TripLogic)']),
        'SurveyTime (DemoLogic)': calculate_avg_time(
            route_report_df.loc[route_report_df['Date_Route'] != 'Total', 'SurveyTime (DemoLogic)']),
    }
    
    # Add average percentages
    percent_cols = [col for col in route_report_df.columns if col.startswith('% of')]
    for col in percent_cols:
        avg = route_report_df.loc[route_report_df['Date_Route'] != 'Total', col]\
              .str.rstrip('%').astype(float).mean()
        total_row[col] = format_percentage(avg)
    
    route_report_df = pd.concat([route_report_df, pd.DataFrame([total_row])], ignore_index=True)
    
    # Rename and reorder columns
    # route_report_df = route_report_df.rename(columns={'Date_Route': 'Route'})
    
    column_order = [
        'Date_Route', '# of Records', '# of Supervisor Delete', '# of Records Remove',
        '# of Records Reviewed', '# of Records Not Reviewed', 'SurveyTime (All)',
        'SurveyTime (TripLogic)', 'SurveyTime (DemoLogic)', '% of Incomplete Home Address',
        '% of Homeless', '% of 0 Transfers', '% of Access Walk', '% of Egress Walk',
        '% of LowIncome', '% of No Income', '% of Hispanic', '% of Black', '% of White',
        '% of Follow-Up Survey', '% of Contest - Yes', 
        '% of Contest - (Yes & Good Info)/Overall # of Records'
    ]
    
    return route_report_df[column_order]




# For KCATA three functions Route Comparison, Route Level Comparison, and Route Date Comparison
def process_route_comparison_data(cr_df, df, ke_df):
    """
    Process route comparison data and handle reverse direction logic
    """
    # Filter KINGElvis data
    ke_df = ke_df[ke_df['INTERV_INIT'].astype(str) != '999']
    ke_df = ke_df[ke_df['1st Cleaner'].astype(str) != 'Test/No 5 MIN']
    ke_df = ke_df[ke_df['Final_Usage'].astype(str).str.lower() == 'use']

    # Merge with filtered KINGElvis data
    df = pd.merge(df, ke_df['id'], on='id', how='inner')
    df.drop_duplicates(subset='id', inplace=True)

    def get_int_from_series(series, default=0):
        """
        Safely get a single int from a Series.
        - If empty -> default
        - If multiple -> take first non-null
        - Coerces to int
        """
        vals = pd.to_numeric(pd.Series(series).dropna(), errors='coerce').dropna().values
        if len(vals) == 0:
            return int(default)
        return int(vals[0])

    # Get column names
    time_column = check_all_characters_present(df, ['timeoncode'])
    route_survey_column = check_all_characters_present(df, ['routesurveyedcode'])
    
    # Time values
    early_am_values = ['AM1','AM2']
    am_values = ['AM3', 'MID1','MID2','MID7']
    midday_values = ['MID3', 'MID4', 'MID5', 'MID6','PM1']
    pm_peak_values = ['PM2','PM3','PM4','PM5']
    evening_values = ['PM6','PM7','PM8','PM9']

    # CR columns
    early_am_column = ['1']
    am_column = ['2']
    midday_colum = ['3']
    pm_column = ['4']
    evening_column = ['5']

    # cr_df.dropna(subset=['LS_NAME_CODE'], inplace=True)

    # Create new dataframe with CR data
    new_df = pd.DataFrame()
    new_df['ROUTE_SURVEYEDCode'] = cr_df['LS_NAME_CODE']
    new_df['CR_Early_AM'] = cr_df[early_am_column[0]]
    new_df['CR_AM_Peak'] = cr_df[am_column[0]]
    new_df['CR_Midday'] = cr_df[midday_colum[0]]
    new_df['CR_PM_Peak'] = cr_df[pm_column[0]]
    new_df['CR_Evening'] = cr_df[evening_column[0]]
    new_df.fillna(0, inplace=True)

    # Add values from database
    for index, row in new_df.iterrows():
        route_code = row['ROUTE_SURVEYEDCode']
        
        def get_counts_and_ids(time_values):
            subset_df = df[(df['ROUTE_SURVEYEDCode'] == route_code) & (df[time_column[0]].isin(time_values))]
            count = subset_df.shape[0]
            ids = subset_df['id'].values
            return count, ids
        
        early_am_value, early_am_ids = get_counts_and_ids(early_am_values)
        am_value, am_value_ids = get_counts_and_ids(am_values)
        midday_value, midday_value_ids = get_counts_and_ids(midday_values)
        pm_value, pm_value_ids = get_counts_and_ids(pm_peak_values)
        evening_value, evening_value_ids = get_counts_and_ids(evening_values)
        
        new_df.loc[index, 'CR_Total'] = row['CR_Early_AM'] + row['CR_AM_Peak'] + row['CR_Midday'] + row['CR_PM_Peak'] + row['CR_Evening']
        new_df.loc[index, 'DB_Early_AM'] = early_am_value
        new_df.loc[index, 'DB_AM_Peak'] = am_value
        new_df.loc[index, 'DB_Midday'] = midday_value
        new_df.loc[index, 'DB_PM_Peak'] = pm_value
        new_df.loc[index, 'DB_Evening'] = evening_value
        new_df.loc[index, 'DB_Total'] = evening_value + am_value + midday_value + pm_value + early_am_value
        
        new_df.loc[index, 'DB_Early_AM_IDS'] = ', '.join(map(str, early_am_ids))
        new_df.loc[index, 'DB_AM_IDS'] = ', '.join(map(str, am_value_ids))
        new_df.loc[index, 'DB_Midday_IDS'] = ', '.join(map(str, midday_value_ids))
        new_df.loc[index, 'DB_PM_IDS'] = ', '.join(map(str, pm_value_ids))
        new_df.loc[index, 'DB_Evening_IDS'] = ', '.join(map(str, evening_value_ids))

    return new_df

def create_route_level_comparison(new_df):
    """
    Create route level comparison from the processed data
    """
    new_df['ROUTE_SURVEYEDCode_Splited'] = new_df['ROUTE_SURVEYEDCode'].apply(lambda x:('_').join(x.split('_')[:-1]))
    
    route_level_df = pd.DataFrame()
    unique_routes = new_df['ROUTE_SURVEYEDCode_Splited'].unique()
    route_level_df['ROUTE_SURVEYEDCode'] = unique_routes
    
    for index, row in route_level_df.iterrows():
        subset_df = new_df[new_df['ROUTE_SURVEYEDCode_Splited'] == row['ROUTE_SURVEYEDCode']]
        sum_per_route_cr = subset_df[['CR_Early_AM', 'CR_AM_Peak', 'CR_Midday', 'CR_PM_Peak', 'CR_Evening', 'CR_Total']].sum()
        sum_per_route_db = subset_df[['DB_Early_AM', 'DB_AM_Peak', 'DB_Midday', 'DB_PM_Peak', 'DB_Evening', 'DB_Total']].sum()
        
        route_level_df.loc[index, 'CR_Early_AM'] = sum_per_route_cr['CR_Early_AM']
        route_level_df.loc[index, 'CR_AM_Peak'] = sum_per_route_cr['CR_AM_Peak']
        route_level_df.loc[index, 'CR_Midday'] = sum_per_route_cr['CR_Midday']
        route_level_df.loc[index, 'CR_PM_Peak'] = sum_per_route_cr['CR_PM_Peak']
        route_level_df.loc[index, 'CR_Evening'] = sum_per_route_cr['CR_Evening']
        route_level_df.loc[index, 'CR_Total'] = sum_per_route_cr['CR_Total']
        
        route_level_df.loc[index, 'DB_Early_AM'] = sum_per_route_db['DB_Early_AM']
        route_level_df.loc[index, 'DB_AM_Peak'] = sum_per_route_db['DB_AM_Peak']
        route_level_df.loc[index, 'DB_Midday'] = sum_per_route_db['DB_Midday']
        route_level_df.loc[index, 'DB_PM_Peak'] = sum_per_route_db['DB_PM_Peak']
        route_level_df.loc[index, 'DB_Evening'] = sum_per_route_db['DB_Evening']
        route_level_df.loc[index, 'DB_Total'] = sum_per_route_db['DB_Total']   
        
        route_level_df.loc[index, 'DB_Early_AM_IDS'] = ', '.join(str(value) for value in subset_df['DB_Early_AM_IDS'].values)
        route_level_df.loc[index, 'DB_AM_IDS'] = ', '.join(str(value) for value in subset_df['DB_AM_IDS'].values)
        route_level_df.loc[index, 'DB_Midday_IDS'] = ', '.join(str(value) for value in subset_df['DB_Midday_IDS'].values)    
        route_level_df.loc[index, 'DB_PM_IDS'] = ', '.join(str(value) for value in subset_df['DB_PM_IDS'].values)    
        route_level_df.loc[index, 'DB_Evening_IDS'] = ', '.join(str(value) for value in subset_df['DB_Evening_IDS'].values)
    
    # Calculate differences
    for index, row in route_level_df.iterrows():
        early_am_peak_diff = row['CR_Early_AM'] - row['DB_Early_AM']
        am_peak_diff = row['CR_AM_Peak'] - row['DB_AM_Peak']
        midday_diff = row['CR_Midday'] - row['DB_Midday']    
        pm_peak_diff = row['CR_PM_Peak'] - row['DB_PM_Peak']
        evening_diff = row['CR_Evening'] - row['DB_Evening']
        total_diff = row['CR_Total'] - row['DB_Total']
        
        route_level_df.loc[index, 'EARLY_AM_DIFFERENCE'] = math.ceil(max(0, early_am_peak_diff))
        route_level_df.loc[index, 'AM_DIFFERENCE'] = math.ceil(max(0, am_peak_diff))
        route_level_df.loc[index, 'Midday_DIFFERENCE'] = math.ceil(max(0, midday_diff))
        route_level_df.loc[index, 'PM_DIFFERENCE'] = math.ceil(max(0, pm_peak_diff))
        route_level_df.loc[index, 'Evening_DIFFERENCE'] = math.ceil(max(0, evening_diff))
        route_level_df.loc[index, 'Total_DIFFERENCE'] = math.ceil(max(0, early_am_peak_diff)) + math.ceil(max(0, am_peak_diff)) + math.ceil(max(0, midday_diff)) + math.ceil(max(0, pm_peak_diff)) + math.ceil(max(0, evening_diff))
    
    return route_level_df

def process_reverse_direction_logic(wkday_overall_df, df, route_level_df, project_name):
    """
    Process reverse direction logic for the routes
    """
    # Get column names
    trip_oppo_dir_column = check_all_characters_present(df, ['tripinoppodir'])
    route_survey_column = check_all_characters_present(df, ['routesurveyedcode'])
    route_survey_name_column = check_all_characters_present(df, ['routesurveyed'])
    trip_code_column = check_all_characters_present(df, ['prevtransferscode', 'nexttransferscode'])
    trip_code_column.sort()
    
    prev_trip_route_code_column = check_all_characters_present(df, ['tripfirstroutecode', 'tripsecondroutecode', 'tripthirdroutecode', 'tripfourthroutecode'])
    next_trip_route_code_column = check_all_characters_present(df, ['tripnextroutecode', 'tripafterroutecode', 'trip3rdroutecode', 'triplast4thrtecode'])
    
    values_to_replace = ['-oth-']
    df[[*prev_trip_route_code_column, *next_trip_route_code_column]] = df[
        [*prev_trip_route_code_column, *next_trip_route_code_column]
    ].replace(values_to_replace, np.nan)
    
    # CR columns
    early_am_column = ['1']
    am_column = ['2']
    midday_colum = ['3']
    pm_column = ['4']
    evening_column = ['5']
    # Create mapping from route code to its details from cr_df
    cr_route_info = {}
    for _, row in wkday_overall_df.iterrows():
        route_code = row['LS_NAME_CODE']
        
        # Extract time period (1, 2, 3, 4, 5) based on which column has values
        valid_time_periods = []
        if pd.notna(row.get(early_am_column[0], 0)) and row[early_am_column[0]] > 0:
            valid_time_periods.append('1')  # Early AM
        if pd.notna(row.get(am_column[0], 0)) and row[am_column[0]] > 0:
            valid_time_periods.append('2')  # AM Peak
        if pd.notna(row.get(midday_colum[0], 0)) and row[midday_colum[0]] > 0:
            valid_time_periods.append('3')  # Midday
        if pd.notna(row.get(pm_column[0], 0)) and row[pm_column[0]] > 0:
            valid_time_periods.append('4')  # PM Peak
        if pd.notna(row.get(evening_column[0], 0)) and row[evening_column[0]] > 0:
            valid_time_periods.append('5')  # Evening
        
        # Randomly select one time period from the valid ones
        time_period = random.choice(valid_time_periods) if valid_time_periods else ''

        # Determine day type (Weekday/Weekend) from route code
        day_type = 'Weekday'  # Default to weekday
        if isinstance(route_code, str):
            if 'WKEND' in route_code.upper() or 'SAT' in route_code.upper() or 'SUN' in route_code.upper():
                day_type = 'Weekend'

        # Extract direction code from route code
        direction_code = ''
        if isinstance(route_code, str):
            if route_code.endswith('00'):
                direction_code = '00'
            elif route_code.endswith('01'):
                direction_code = '01'
            elif route_code.endswith('02'):
                direction_code = '02'
            elif route_code.endswith('03'):
                direction_code = '03'
            else:
                # If no direction code found, check if it's in the route name pattern
                if '_' in route_code:
                    base_route = '_'.join(route_code.split('_')[:-1])
                    direction_code = route_code.split('_')[-1]

        cr_route_info[route_code] = {
            'TIME_PERIOD': time_period,
            'DAY_TYPE': day_type,
            'FINAL_DIRECTION_CODE': direction_code,
            'BASE_ROUTE_CODE': '_'.join(route_code.split('_')[:-1]) if isinstance(route_code, str) and '_' in route_code else route_code
        }

    # Function to get route info from cr_route_info mapping
    def get_route_info(route_code, info_type):
        if route_code in cr_route_info:
            return cr_route_info[route_code].get(info_type, '')
        return ''

    # Function to create the final direction code with concatenation
    def get_final_direction_code(route_code):
        if route_code in cr_route_info:
            base_route = cr_route_info[route_code].get('BASE_ROUTE_CODE', '')
            direction_code = cr_route_info[route_code].get('FINAL_DIRECTION_CODE', '')
            if base_route and direction_code:
                return f"{base_route}_{direction_code}"
        return ''

    def create_url(record_id):
        if project_name.lower() == 'actransit':
            return f"https://elvis-wappler.etc-research.com/elvis/elvisheremap/{record_id}/ac-transit25/lime"
        else:
            return f"https://elvis-wappler.etc-research.com/elvis/elvisheremap/{record_id}/kc-streetcar25/lime"

    # Create reverse dataframe
    reverse_df = df[df[trip_oppo_dir_column[0]].str.lower() == 'yes'][['id', *route_survey_column, *route_survey_name_column, *trip_code_column, *prev_trip_route_code_column, *next_trip_route_code_column]]

    # Add the new columns using the cr_df mapping
    reverse_df['TIME_PERIOD'] = reverse_df[route_survey_column[0]].apply(lambda x: get_route_info(x, 'TIME_PERIOD'))
    reverse_df['DAY_TYPE'] = reverse_df[route_survey_column[0]].apply(lambda x: get_route_info(x, 'DAY_TYPE'))
    reverse_df['FINAL_DIRECTION_CODE'] = reverse_df[route_survey_column[0]].apply(get_final_direction_code)
    reverse_df['URL'] = reverse_df['id'].apply(create_url)

    reverse_df[route_survey_column[0]] = reverse_df[route_survey_column[0]].apply(lambda x: '_'.join(x.split("_")[:-1]))
    reverse_df.reset_index(inplace=True, drop=True)
    reverse_df[[*prev_trip_route_code_column, *next_trip_route_code_column]].fillna('', inplace=True)
    
    def get_int_from_series(series, default=0):
        vals = pd.to_numeric(pd.Series(series).dropna(), errors='coerce').dropna().values
        if len(vals) == 0:
            return int(default)
        return int(vals[0])
    
    def get_valid_routes(row, route_code_column):
        result_array = reverse_df[reverse_df['id'] == row['id']][route_code_column].values
        values_in_list = result_array[0, :]
        return [value for value in values_in_list if not (pd.isna(value) or value == '')]
    
    def process_route(route, counter_list, counter_prefix, target_index):
        counter_list[0] += 1
        rev_prefix = f'Rev-{counter_prefix}'
        random_choice = random.choice([counter_prefix, rev_prefix])
        
        value = get_int_from_series(
            route_level_df.loc[route_level_df[route_survey_column[0]] == route, 'Total_DIFFERENCE']
        )
        
        if value > 0:
            reverse_df.loc[target_index, 'Type'] = f'{random_choice}{counter_list[0]}'
            route_level_df.loc[
                route_level_df[route_survey_column[0]] == route, 'Total_DIFFERENCE'
            ] = value - 1
            return True
        return False
    
    # Implement logic for handling Type values
    for index, row in reverse_df.iterrows():
        random_value = random.choice([0, 1])
        value = get_int_from_series(
            route_level_df.loc[
                route_level_df[route_survey_column[0]] == row[route_survey_column[0]],
                'Total_DIFFERENCE'
            ]
        )
        prev_trans_value = get_int_from_series(
            df.loc[df['id'] == row['id'], trip_code_column[1]],
            default=0
        )
        next_trans_value = get_int_from_series(
            df.loc[df['id'] == row['id'], trip_code_column[0]],
            default=0
        )
        counter = [0]
        
        if random_value:
            if value > 0:
                reverse_df.loc[index, 'Type'] = 'Reverse'
                route_level_df.loc[route_level_df[route_survey_column[0]] == row[route_survey_column[0]], 'Total_DIFFERENCE'] = value - 1
            elif prev_trans_value:
                for route in get_valid_routes(row, prev_trip_route_code_column):
                    result_value = process_route(route, counter, 'p', index)
                    if result_value:
                        break
                    else:
                        reverse_df.loc[index, 'Type'] = f'{random.choice(["p1","Rev-p1"])}'
                        route_level_df.loc[route_level_df[route_survey_column[0]] == row[route_survey_column[0]], 'Total_DIFFERENCE'] = value - 1
                        break
            elif next_trans_value:
                for route in get_valid_routes(row, next_trip_route_code_column):
                    result_value = process_route(route, counter, 'n', index)
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
                    result_value = process_route(route, counter, 'p', index)
                    if result_value:
                        break
                    else:
                        reverse_df.loc[index, 'Type'] = f'{random.choice(["p1","Rev-p1"])}'
                        route_level_df.loc[route_level_df[route_survey_column[0]] == row[route_survey_column[0]], 'Total_DIFFERENCE'] = value - 1
                        break
            elif next_trans_value:
                for route in get_valid_routes(row, next_trip_route_code_column):
                    result_value = process_route(route, counter, 'n', index)
                    if result_value:
                        break
                    else:
                        reverse_df.loc[index, 'Type'] = f'{random.choice(["n1","Rev-n1"])}'
                        route_level_df.loc[route_level_df[route_survey_column[0]] == row[route_survey_column[0]], 'Total_DIFFERENCE'] = value - 1
                        break
            else:
                reverse_df.loc[index, 'Type'] = 'Reverse'
    
    all_type_df = copy.deepcopy(reverse_df)
    
    # Second pass for reverse direction logic
    new_route_level_df = copy.deepcopy(route_level_df)
    
    for index, row in reverse_df.iterrows():
        random_value = random.choice([0, 1])
        value = get_int_from_series(
            new_route_level_df.loc[
                new_route_level_df[route_survey_column[0]] == row[route_survey_column[0]],
                'Total_DIFFERENCE'
            ]
        )
        prev_trans_value = get_int_from_series(
            df.loc[df['id'] == row['id'], trip_code_column[1]],
            default=0
        )
        next_trans_value = get_int_from_series(
            df.loc[df['id'] == row['id'], trip_code_column[0]],
            default=0
        )
        
        counter = [0]
        
        if value:
            if not random_value:
                reverse_df.loc[index, 'Type'] = 'Reverse'
                new_route_level_df.loc[new_route_level_df[route_survey_column[0]] == row[route_survey_column[0]], 'Total_DIFFERENCE'] = value - 1
            else:
                if prev_trans_value:
                    for route in get_valid_routes(row, prev_trip_route_code_column):
                        result_value = process_route(route, counter, 'p', index)
                        if result_value:
                            break
                        else:
                            reverse_df.loc[index, 'Type'] = f'{random.choice(["p1","Rev-p1"])}'
                            new_route_level_df.loc[new_route_level_df[route_survey_column[0]] == row[route_survey_column[0]], 'Total_DIFFERENCE'] = value - 1
                            break
                elif next_trans_value:
                    for route in get_valid_routes(row, next_trip_route_code_column):
                        result_value = process_route(route, counter, 'n', index)
                        if result_value:
                            break
                        else:
                            reverse_df.loc[index, 'Type'] = f'{random.choice(["n1","Rev-n1"])}'
                            new_route_level_df.loc[new_route_level_df[route_survey_column[0]] == row[route_survey_column[0]], 'Total_DIFFERENCE'] = value - 1
                            break
                else:
                    reverse_df.loc[index, 'Type'] = 'Reverse'
                    new_route_level_df.loc[new_route_level_df[route_survey_column[0]] == row[route_survey_column[0]], 'Total_DIFFERENCE'] = value - 1
        else:
            reverse_df.loc[index, 'Type'] = ''
    
    reverse_df['COMPLETED By'] = ''
    all_type_df['COMPLETED By'] = ''
    all_type_df['Type'].fillna('Reverse', inplace=True)
    
    return route_level_df, all_type_df, reverse_df


# For ACTRANSIT 
def create_low_response_report(elvis_df):
    """
    Generate Low Response Questions DataFrame
    Call this function with the elvis_df to create the report DataFrame
    
    Parameters:
    elvis_df (DataFrame): Input data as pandas DataFrame
    
    Returns:
    DataFrame: Low response questions report as DataFrame
    """
    
    def apply_relevance_conditions(df):
        """
        Apply relevance conditions to filter or create boolean columns
        """
        conditions = {}
        
        # COMPANION_ID - always relevant (1 means always True)
        conditions['COMPANION_ID'] = pd.Series([True] * len(df))
        
        # EXTRATRIP_ID - always relevant
        conditions['EXTRATRIP_ID'] = pd.Series([True] * len(df))
        
        # FIRSTWAITTIME - complex condition
        try:
            prev_naok_0 = (~df['PrevTransfers'].isna()) & (df['PrevTransfersCode'] == 0)
            prev_naok_1_to_4 = (~df['PrevTransfers'].isna()) & (df['PrevTransfersCode'].isin([1, 2, 3, 4]))
            trip_first_route_not_empty = ~df['TripFirstRoute'].isna()
            conditions['FIRSTWAITTIME'] = prev_naok_0 | (prev_naok_1_to_4 & trip_first_route_not_empty)
        except:
            conditions['FIRSTWAITTIME'] = pd.Series([False] * len(df))
        
        # REVERSE_TRIPS - always relevant
        conditions['REVERSE_TRIPS'] = pd.Series([True] * len(df))
        
        # SELECT_LANGUAGE - condition on Have5MinForSurve.NAOK
        try:
            conditions['SELECT_LANGUAGE'] = (~df['Have5MinForSurve'].isna()) & (df['Have5MinForSurveCode'] == 5)
        except:
            conditions['SELECT_LANGUAGE'] = pd.Series([False] * len(df))
        
        # SUPERVISOR_REMARK - always relevant
        conditions['SUPERVISOR_REMARK'] = pd.Series([True] * len(df))
        
        # SUPERVISOR_STATUS - always relevant
        conditions['SUPERVISOR_STATUS'] = pd.Series([True] * len(df))
        
        # USERTYPE - condition on Have5MinForSurve.NAOK
        try:
            conditions['USERTYPE'] = (~df['Have5MinForSurve'].isna()) & (df['Have5MinForSurveCode'].isin([1, 2]))
        except:
            conditions['USERTYPE'] = pd.Series([False] * len(df))
        
        # X_MOBILE_APP - condition on PayToRide.NAOK
        try:
            conditions['X_MOBILE_APP'] = (~df['PayToRide'].isna()) & (df['PayToRideCode'] == 3)
        except:
            conditions['X_MOBILE_APP'] = pd.Series([False] * len(df))
        
        # X_OTHER_FARE - condition on PayToRide.NAOK
        try:
            conditions['X_OTHER_FARE'] = (~df['PayToRide'].isna()) & (df['PayToRideCode'] == 4)
        except:
            conditions['X_OTHER_FARE'] = pd.Series([False] * len(df))
        
        # ACCESS_BIKE - condition on OriginTransport.NAOK
        try:
            conditions['ACCESS_BIKE'] = (~df['OriginTransport'].isna()) & (df['OriginTransportCode'].isin([3, 4]))
        except:
            conditions['ACCESS_BIKE'] = pd.Series([False] * len(df))
        
        # EGRESS_BIKE - condition on DestinTransport.NAOK
        try:
            conditions['EGRESS_BIKE'] = (~df['DestinTransport'].isna()) & (df['DestinTransportCode'].isin([3, 4]))
        except:
            conditions['EGRESS_BIKE'] = pd.Series([False] * len(df))
        
        # DID_MAKE_SCHOOL_TRIP - complex condition
        try:
            origin_valid_school = df['OriginPlaceTypeCode'].isin(["-oth-", 1, 2, 3, 4, 7, 8, 9, 10, 11, 12, 13])
            destin_valid_school = df['DestinPlaceTypeCode'].isin(["-oth-", 1, 2, 3, 4, 7, 8, 9, 10, 11, 12, 13])
            student_status_valid = (~df['StudentStatus'].isna()) & (df['StudentStatusCode'].isin([2, 3, 4]))
            conditions['DID_MAKE_SCHOOL_TRIP'] = origin_valid_school & destin_valid_school & student_status_valid
        except:
            conditions['DID_MAKE_SCHOOL_TRIP'] = pd.Series([False] * len(df))
        
        # WILL_MAKE_SCHOOL_TRIP - same condition as DID_MAKE_SCHOOL_TRIP
        try:
            conditions['WILL_MAKE_SCHOOL_TRIP'] = conditions['DID_MAKE_SCHOOL_TRIP']
        except:
            conditions['WILL_MAKE_SCHOOL_TRIP'] = pd.Series([False] * len(df))
        
        # DAYS_PER_WEEK - condition on Have5MinForSurve.NAOK
        try:
            conditions['DAYS_PER_WEEK'] = (~df['Have5MinForSurve'].isna()) & (df['Have5MinForSurveCode'].isin([1, 2]))
        except:
            conditions['DAYS_PER_WEEK'] = pd.Series([False] * len(df))
        
        # DID_MAKE_WORK_TRIP - complex condition
        try:
            origin_valid_work = df['OriginPlaceTypeCode'].isin(["-oth-", 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13])
            destin_valid_work = df['DestinPlaceTypeCode'].isin(["-oth-", 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13])
            employment_valid = (~df['EmploymentStatus'].isna()) & (df['EmploymentStatusCode'].isin([1, 2]))
            conditions['DID_MAKE_WORK_TRIP'] = origin_valid_work & destin_valid_work & employment_valid
        except:
            conditions['DID_MAKE_WORK_TRIP'] = pd.Series([False] * len(df))
        
        # WILL_MAKE_WORK_TRIP - same condition as DID_MAKE_WORK_TRIP
        try:
            conditions['WILL_MAKE_WORK_TRIP'] = conditions['DID_MAKE_WORK_TRIP']
        except:
            conditions['WILL_MAKE_WORK_TRIP'] = pd.Series([False] * len(df))
        
        # ALT_INCOME - condition on Income.NAOK
        try:
            conditions['ALT_INCOME'] = (~df['Income'].isna()) & (df['IncomeCode'] == 10)
        except:
            conditions['ALT_INCOME'] = pd.Series([False] * len(df))
        
        # TIME_ADJUST - always relevant
        conditions['TIME_ADJUST'] = pd.Series([True] * len(df))
        
        # WORK_DAYS - condition on EmploymentStatus.NAOK
        try:
            conditions['WORK_DAYS'] = (~df['EmploymentStatus'].isna()) & (df['EmploymentStatusCode'].isin([1, 2]))
        except:
            conditions['WORK_DAYS'] = pd.Series([False] * len(df))
        
        # CLIPPER_DISCOUNT - condition on PayToRide.NAOK
        try:
            conditions['CLIPPER_DISCOUNT'] = (~df['PayToRide'].isna()) & (df['PayToRideCode'] == 1)
        except:
            conditions['CLIPPER_DISCOUNT'] = pd.Series([False] * len(df))
        
        # ACCESS_WALK - condition on OriginTransport.NAOK
        try:
            conditions['ACCESS_WALK'] = (~df['OriginTransport'].isna()) & (df['OriginTransportCode'] == 1)
        except:
            conditions['ACCESS_WALK'] = pd.Series([False] * len(df))
        
        # EGRESS_WALK - condition on DestinTransport.NAOK
        try:
            conditions['EGRESS_WALK'] = (~df['DestinTransport'].isna()) & (df['DestinTransportCode'] == 1)
        except:
            conditions['EGRESS_WALK'] = pd.Series([False] * len(df))
        
        return conditions

    def get_original_relevance_logic(column_name):
        """
        Return the original LimeSurvey format relevance conditions as provided
        """
        relevance_logic = {
            'COMPANION_ID': '1',
            'EXTRATRIP_ID': '1',
            'FIRSTWAITTIME': '(((!is_empty(PrevTransfers.NAOK) && (PrevTransfers.NAOK == 0)))) or (((!is_empty(PrevTransfers.NAOK) && (PrevTransfers.NAOK == 1)) or (!is_empty(PrevTransfers.NAOK) && (PrevTransfers.NAOK == 2)) or (!is_empty(PrevTransfers.NAOK) && (PrevTransfers.NAOK == 3)) or (!is_empty(PrevTransfers.NAOK) && (PrevTransfers.NAOK == 4))) and (!is_empty(TripFirstRoute.NAOK)))',
            'REVERSE_TRIPS': '1',
            'SELECT_LANGUAGE': '(((!is_empty(Have5MinForSurve.NAOK) && (Have5MinForSurve.NAOK == 5))))',
            'SUPERVISOR_REMARK': '1',
            'SUPERVISOR_STATUS': '1',
            'USERTYPE': '(((!is_empty(Have5MinForSurve.NAOK) && (Have5MinForSurve.NAOK == 1)) or (!is_empty(Have5MinForSurve.NAOK) && (Have5MinForSurve.NAOK == 2))))',
            'X_MOBILE_APP': '(((!is_empty(PayToRide.NAOK) && (PayToRide.NAOK == 3))))',
            'X_OTHER_FARE': '(((!is_empty(PayToRide.NAOK) && (PayToRide.NAOK == 4))))',
            'ACCESS_BIKE': '(((!is_empty(OriginTransport.NAOK) && (OriginTransport.NAOK == 3)) or (!is_empty(OriginTransport.NAOK) && (OriginTransport.NAOK == 4))))',
            'EGRESS_BIKE': '(((!is_empty(DestinTransport.NAOK) && (DestinTransport.NAOK == 3)) or (!is_empty(DestinTransport.NAOK) && (DestinTransport.NAOK == 4))))',
            'DID_MAKE_SCHOOL_TRIP': '((OriginPlaceType.NAOK == "-oth-" or (!is_empty(OriginPlaceType.NAOK) && (OriginPlaceType.NAOK == 1)) or (!is_empty(OriginPlaceType.NAOK) && (OriginPlaceType.NAOK == 10)) or (!is_empty(OriginPlaceType.NAOK) && (OriginPlaceType.NAOK == 11)) or (!is_empty(OriginPlaceType.NAOK) && (OriginPlaceType.NAOK == 12)) or (!is_empty(OriginPlaceType.NAOK) && (OriginPlaceType.NAOK == 13)) or (!is_empty(OriginPlaceType.NAOK) && (OriginPlaceType.NAOK == 2)) or (!is_empty(OriginPlaceType.NAOK) && (OriginPlaceType.NAOK == 3)) or (!is_empty(OriginPlaceType.NAOK) && (OriginPlaceType.NAOK == 4)) or (!is_empty(OriginPlaceType.NAOK) && (OriginPlaceType.NAOK == 7)) or (!is_empty(OriginPlaceType.NAOK) && (OriginPlaceType.NAOK == 8)) or (!is_empty(OriginPlaceType.NAOK) && (OriginPlaceType.NAOK == 9))) and (DestinPlaceType.NAOK == "-oth-" or (!is_empty(DestinPlaceType.NAOK) && (DestinPlaceType.NAOK == 1)) or (!is_empty(DestinPlaceType.NAOK) && (DestinPlaceType.NAOK == 10)) or (!is_empty(DestinPlaceType.NAOK) && (DestinPlaceType.NAOK == 11)) or (!is_empty(DestinPlaceType.NAOK) && (DestinPlaceType.NAOK == 12)) or (!is_empty(DestinPlaceType.NAOK) && (DestinPlaceType.NAOK == 13)) or (!is_empty(DestinPlaceType.NAOK) && (DestinPlaceType.NAOK == 2)) or (!is_empty(DestinPlaceType.NAOK) && (DestinPlaceType.NAOK == 3)) or (!is_empty(DestinPlaceType.NAOK) && (DestinPlaceType.NAOK == 4)) or (!is_empty(DestinPlaceType.NAOK) && (DestinPlaceType.NAOK == 7)) or (!is_empty(DestinPlaceType.NAOK) && (DestinPlaceType.NAOK == 8)) or (!is_empty(DestinPlaceType.NAOK) && (DestinPlaceType.NAOK == 9))) and ((!is_empty(StudentStatus.NAOK) && (StudentStatus.NAOK == 2)) or (!is_empty(StudentStatus.NAOK) && (StudentStatus.NAOK == 3)) or (!is_empty(StudentStatus.NAOK) && (StudentStatus.NAOK == 4))))',
            'WILL_MAKE_SCHOOL_TRIP': '((OriginPlaceType.NAOK == "-oth-" or (!is_empty(OriginPlaceType.NAOK) && (OriginPlaceType.NAOK == 1)) or (!is_empty(OriginPlaceType.NAOK) && (OriginPlaceType.NAOK == 10)) or (!is_empty(OriginPlaceType.NAOK) && (OriginPlaceType.NAOK == 11)) or (!is_empty(OriginPlaceType.NAOK) && (OriginPlaceType.NAOK == 12)) or (!is_empty(OriginPlaceType.NAOK) && (OriginPlaceType.NAOK == 13)) or (!is_empty(OriginPlaceType.NAOK) && (OriginPlaceType.NAOK == 2)) or (!is_empty(OriginPlaceType.NAOK) && (OriginPlaceType.NAOK == 3)) or (!is_empty(OriginPlaceType.NAOK) && (OriginPlaceType.NAOK == 4)) or (!is_empty(OriginPlaceType.NAOK) && (OriginPlaceType.NAOK == 7)) or (!is_empty(OriginPlaceType.NAOK) && (OriginPlaceType.NAOK == 8)) or (!is_empty(OriginPlaceType.NAOK) && (OriginPlaceType.NAOK == 9))) and (DestinPlaceType.NAOK == "-oth-" or (!is_empty(DestinPlaceType.NAOK) && (DestinPlaceType.NAOK == 1)) or (!is_empty(DestinPlaceType.NAOK) && (DestinPlaceType.NAOK == 10)) or (!is_empty(DestinPlaceType.NAOK) && (DestinPlaceType.NAOK == 11)) or (!is_empty(DestinPlaceType.NAOK) && (DestinPlaceType.NAOK == 12)) or (!is_empty(DestinPlaceType.NAOK) && (DestinPlaceType.NAOK == 13)) or (!is_empty(DestinPlaceType.NAOK) && (DestinPlaceType.NAOK == 2)) or (!is_empty(DestinPlaceType.NAOK) && (DestinPlaceType.NAOK == 3)) or (!is_empty(DestinPlaceType.NAOK) && (DestinPlaceType.NAOK == 4)) or (!is_empty(DestinPlaceType.NAOK) && (DestinPlaceType.NAOK == 7)) or (!is_empty(DestinPlaceType.NAOK) && (DestinPlaceType.NAOK == 8)) or (!is_empty(DestinPlaceType.NAOK) && (DestinPlaceType.NAOK == 9))) and ((!is_empty(StudentStatus.NAOK) && (StudentStatus.NAOK == 2)) or (!is_empty(StudentStatus.NAOK) && (StudentStatus.NAOK == 3)) or (!is_empty(StudentStatus.NAOK) && (StudentStatus.NAOK == 4))))',
            'DAYS_PER_WEEK': '(((!is_empty(Have5MinForSurve.NAOK) && (Have5MinForSurve.NAOK == 1)) or (!is_empty(Have5MinForSurve.NAOK) && (Have5MinForSurve.NAOK == 2))))',
            'DID_MAKE_WORK_TRIP': '((OriginPlaceType.NAOK == "-oth-" or (!is_empty(OriginPlaceType.NAOK) && (OriginPlaceType.NAOK == 10)) or (!is_empty(OriginPlaceType.NAOK) && (OriginPlaceType.NAOK == 11)) or (!is_empty(OriginPlaceType.NAOK) && (OriginPlaceType.NAOK == 12)) or (!is_empty(OriginPlaceType.NAOK) && (OriginPlaceType.NAOK == 13)) or (!is_empty(OriginPlaceType.NAOK) && (OriginPlaceType.NAOK == 2)) or (!is_empty(OriginPlaceType.NAOK) && (OriginPlaceType.NAOK == 3)) or (!is_empty(OriginPlaceType.NAOK) && (OriginPlaceType.NAOK == 4)) or (!is_empty(OriginPlaceType.NAOK) && (OriginPlaceType.NAOK == 5)) or (!is_empty(OriginPlaceType.NAOK) && (OriginPlaceType.NAOK == 6)) or (!is_empty(OriginPlaceType.NAOK) && (OriginPlaceType.NAOK == 7)) or (!is_empty(OriginPlaceType.NAOK) && (OriginPlaceType.NAOK == 8)) or (!is_empty(OriginPlaceType.NAOK) && (OriginPlaceType.NAOK == 9))) and (DestinPlaceType.NAOK == "-oth-" or (!is_empty(DestinPlaceType.NAOK) && (DestinPlaceType.NAOK == 10)) or (!is_empty(DestinPlaceType.NAOK) && (DestinPlaceType.NAOK == 11)) or (!is_empty(DestinPlaceType.NAOK) && (DestinPlaceType.NAOK == 12)) or (!is_empty(DestinPlaceType.NAOK) && (DestinPlaceType.NAOK == 13)) or (!is_empty(DestinPlaceType.NAOK) && (DestinPlaceType.NAOK == 2)) or (!is_empty(DestinPlaceType.NAOK) && (DestinPlaceType.NAOK == 3)) or (!is_empty(DestinPlaceType.NAOK) && (DestinPlaceType.NAOK == 4)) or (!is_empty(DestinPlaceType.NAOK) && (DestinPlaceType.NAOK == 5)) or (!is_empty(DestinPlaceType.NAOK) && (DestinPlaceType.NAOK == 6)) or (!is_empty(DestinPlaceType.NAOK) && (DestinPlaceType.NAOK == 7)) or (!is_empty(DestinPlaceType.NAOK) && (DestinPlaceType.NAOK == 8)) or (!is_empty(DestinPlaceType.NAOK) && (DestinPlaceType.NAOK == 9))) and ((!is_empty(EmploymentStatus.NAOK) && (EmploymentStatus.NAOK == 1)) or (!is_empty(EmploymentStatus.NAOK) && (EmploymentStatus.NAOK == 2))))',
            'WILL_MAKE_WORK_TRIP': '((OriginPlaceType.NAOK == "-oth-" or (!is_empty(OriginPlaceType.NAOK) && (OriginPlaceType.NAOK == 10)) or (!is_empty(OriginPlaceType.NAOK) && (OriginPlaceType.NAOK == 11)) or (!is_empty(OriginPlaceType.NAOK) && (OriginPlaceType.NAOK == 12)) or (!is_empty(OriginPlaceType.NAOK) && (OriginPlaceType.NAOK == 13)) or (!is_empty(OriginPlaceType.NAOK) && (OriginPlaceType.NAOK == 2)) or (!is_empty(OriginPlaceType.NAOK) && (OriginPlaceType.NAOK == 3)) or (!is_empty(OriginPlaceType.NAOK) && (OriginPlaceType.NAOK == 4)) or (!is_empty(OriginPlaceType.NAOK) && (OriginPlaceType.NAOK == 5)) or (!is_empty(OriginPlaceType.NAOK) && (OriginPlaceType.NAOK == 6)) or (!is_empty(OriginPlaceType.NAOK) && (OriginPlaceType.NAOK == 7)) or (!is_empty(OriginPlaceType.NAOK) && (OriginPlaceType.NAOK == 8)) or (!is_empty(OriginPlaceType.NAOK) && (OriginPlaceType.NAOK == 9))) and (DestinPlaceType.NAOK == "-oth-" or (!is_empty(DestinPlaceType.NAOK) && (DestinPlaceType.NAOK == 10)) or (!is_empty(DestinPlaceType.NAOK) && (DestinPlaceType.NAOK == 11)) or (!is_empty(DestinPlaceType.NAOK) && (DestinPlaceType.NAOK == 12)) or (!is_empty(DestinPlaceType.NAOK) && (DestinPlaceType.NAOK == 13)) or (!is_empty(DestinPlaceType.NAOK) && (DestinPlaceType.NAOK == 2)) or (!is_empty(DestinPlaceType.NAOK) && (DestinPlaceType.NAOK == 3)) or (!is_empty(DestinPlaceType.NAOK) && (DestinPlaceType.NAOK == 4)) or (!is_empty(DestinPlaceType.NAOK) && (DestinPlaceType.NAOK == 5)) or (!is_empty(DestinPlaceType.NAOK) && (DestinPlaceType.NAOK == 6)) or (!is_empty(DestinPlaceType.NAOK) && (DestinPlaceType.NAOK == 7)) or (!is_empty(DestinPlaceType.NAOK) && (DestinPlaceType.NAOK == 8)) or (!is_empty(DestinPlaceType.NAOK) && (DestinPlaceType.NAOK == 9))) and ((!is_empty(EmploymentStatus.NAOK) && (EmploymentStatus.NAOK == 1)) or (!is_empty(EmploymentStatus.NAOK) && (EmploymentStatus.NAOK == 2))))',
            'ALT_INCOME': '(((!is_empty(Income.NAOK) && (Income.NAOK == 10))))',
            'TIME_ADJUST': '1',
            'WORK_DAYS': '(((!is_empty(EmploymentStatus.NAOK) && (EmploymentStatus.NAOK == 1)) or (!is_empty(EmploymentStatus.NAOK) && (EmploymentStatus.NAOK == 2))))',
            'CLIPPER_DISCOUNT': '(((!is_empty(PayToRide.NAOK) && (PayToRide.NAOK == 1))))',
            'ACCESS_WALK': '(((!is_empty(OriginTransport.NAOK) && (OriginTransport.NAOK == 1))))',
            'EGRESS_WALK': '(((!is_empty(DestinTransport.NAOK) && (DestinTransport.NAOK == 1))))'
        }
        
        return relevance_logic.get(column_name, 'Condition not defined')

    def calculate_column_stats(df, column_name):
        """
        Calculate statistics for a given column - simple completion rate without relevance filtering
        """
        if column_name not in df.columns:
            return "", "", 0.0
        
        # Use all data (don't filter by relevance condition)
        column_data = df[column_name]
        
        # Remove NaN values for calculation
        non_null_data = column_data.dropna()
        
        if len(non_null_data) == 0:
            return "", "", 0.0
        
        # Calculate completion rate (percentage of all respondents who answered)
        total_respondents = len(column_data)
        non_null_count = len(non_null_data)
        percent_filled = (non_null_count / total_respondents) * 100
        
        # Find most common and least common responses
        value_counts = non_null_data.value_counts()
        
        if len(value_counts) == 0:
            return "", "", percent_filled
        
        first_max = value_counts.index[0]  # Most common response
        first_min = value_counts.index[-1]  # Least common response
        
        return first_max, first_min, percent_filled

    def map_column_names(standard_headers, actual_headers):
        """
        Map standard column names to actual column names in the data file
        """
        mapping = {}
        
        # Common naming pattern variations
        patterns = {
            'COMPANION_ID': ['CompanionId', 'COMPANIONID', 'companion_id', 'companionid'],
            'EXTRATRIP_ID': ['ExtraTripId', 'EXTRATRIPID', 'extra_trip_id', 'extratripid'],
            'FIRSTWAITTIME': ['FirstWaitTime', 'firstwaittime', 'FIRST_WAIT_TIME'],
            'REVERSE_TRIPS': ['ReverseTrips', 'reverse_trips', 'REVERSE_TRIPS'],
            'SELECT_LANGUAGE': ['SelectLanguage', 'select_language', 'SELECTLANGUAGE'],
            'SUPERVISOR_REMARK': ['SupervisorRemark', 'supervisor_remark', 'SUPERVISORREMARK'],
            'SUPERVISOR_STATUS': ['SupervisorStatus', 'supervisor_status', 'SUPERVISORSTATUS'],
            'USERTYPE': ['UserType', 'user_type', 'USERTYPE'],
            'X_MOBILE_APP': ['XMobileApp', 'x_mobile_app', 'X_MOBILEAPP'],
            'X_OTHER_FARE': ['XOtherFare', 'x_other_fare', 'X_OTHERFARE'],
            'ACCESS_BIKE': ['AccessBike', 'access_bike', 'ACCESSBIKE'],
            'EGRESS_BIKE': ['EgressBike', 'egress_bike', 'EGRESSBIKE'],
            'DID_MAKE_SCHOOL_TRIP': ['DidMakeSchoolTrip', 'did_make_school_trip', 'DIDMAKESCHOOLTRIP'],
            'WILL_MAKE_SCHOOL_TRIP': ['WillMakeSchoolTrip', 'will_make_school_trip', 'WILLMAKESCHOOLTRIP'],
            'DAYS_PER_WEEK': ['DaysPerWeek', 'days_per_week', 'DAYSPERWEEK'],
            'DID_MAKE_WORK_TRIP': ['DidMakeWorkTrip', 'did_make_work_trip', 'DIDMAKEWORKTRIP'],
            'WILL_MAKE_WORK_TRIP': ['WillMakeWorkTrip', 'will_make_work_trip', 'WILLMAKEWORKTRIP'],
            'ALT_INCOME': ['AltIncome', 'alt_income', 'ALTINCOME'],
            'TIME_ADJUST': ['TimeAdjust', 'time_adjust', 'TIMEADJUST'],
            'WORK_DAYS': ['WorkDays', 'work_days', 'WORKDAYS'],
            'CLIPPER_DISCOUNT': ['ClipperDiscount', 'clipper_discount', 'CLIPPERDISCOUNT'],
            'ACCESS_WALK': ['AccessWalk', 'access_walk', 'ACCESSWALK'],
            'EGRESS_WALK': ['EgressWalk', 'egress_walk', 'EGRESSWALK']
        }
        
        for standard_name in standard_headers:
            # Try exact match first
            if standard_name in actual_headers:
                mapping[standard_name] = standard_name
                continue
                
            # Try pattern matching
            found = False
            if standard_name in patterns:
                for pattern in patterns[standard_name]:
                    if pattern in actual_headers:
                        mapping[standard_name] = pattern
                        found = True
                        break
            
            # If not found, try case-insensitive matching
            if not found:
                actual_lower = [header.lower() for header in actual_headers]
                standard_lower = standard_name.lower()
                if standard_lower in actual_lower:
                    idx = actual_lower.index(standard_lower)
                    mapping[standard_name] = actual_headers[idx]
                else:
                    # If still not found, mark as not available
                    mapping[standard_name] = None
        
        return mapping

    # Main execution starts here
    df = elvis_df
    
    # Define all column headers we need to analyze
    standard_headers = [
        'COMPANION_ID', 'EXTRATRIP_ID', 'FIRSTWAITTIME', 'REVERSE_TRIPS', 
        'SELECT_LANGUAGE', 'SUPERVISOR_REMARK', 'SUPERVISOR_STATUS', 'USERTYPE',
        'X_MOBILE_APP', 'X_OTHER_FARE', 'ACCESS_BIKE', 'EGRESS_BIKE',
        'DID_MAKE_SCHOOL_TRIP', 'WILL_MAKE_SCHOOL_TRIP', 'DAYS_PER_WEEK',
        'DID_MAKE_WORK_TRIP', 'WILL_MAKE_WORK_TRIP', 'ALT_INCOME', 'TIME_ADJUST',
        'WORK_DAYS', 'CLIPPER_DISCOUNT', 'ACCESS_WALK', 'EGRESS_WALK'
    ]
    
    # Map standard column names to actual column names in the data file
    actual_headers = df.columns.tolist()
    column_mapping = map_column_names(standard_headers, actual_headers)
    
    # Prepare data for the report
    report_data = []
    
    for standard_name in standard_headers:
        actual_name = column_mapping.get(standard_name)
        
        if actual_name is None or actual_name not in df.columns:
            # Column not found in data file
            report_data.append({
                'Column Header': standard_name,
                'First Max': "",
                'First Min': "", 
                'percent_filled': 0.0,
                'First relevance': get_original_relevance_logic(standard_name)
            })
        else:
            # Calculate statistics
            first_max, first_min, percent_filled = calculate_column_stats(df, actual_name)
            
            report_data.append({
                'Column Header': standard_name,
                'First Max': first_max,
                'First Min': first_min,
                'percent_filled': round(percent_filled, 2),
                'First relevance': get_original_relevance_logic(standard_name)
            })
    
    # Create DataFrame for the report
    report_df = pd.DataFrame(report_data)
    
    # Sort by percent_filled (lowest first) to highlight low response questions
    report_df = report_df.sort_values('percent_filled', ascending=True)
    
    return report_df

