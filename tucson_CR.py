
import os
import datetime
import numpy as np
import pandas as pd
import streamlit as st
import snowflake.connector
from automated_refresh_flow_new import fetch_and_process_data
from utils import render_aggrid,create_csv,download_csv,update_query_params, fetch_data
from authentication.auth import schema_value,register_page,login,logout,is_authenticated,forgot_password,reset_password,activate_account,change_password,send_change_password_email,change_password_form,create_new_user_page
from dotenv import load_dotenv
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import plotly.express as px


load_dotenv()
st.set_page_config(page_title="Completion REPORT DashBoard", layout='wide')

with open("path/to/key.p8", "rb") as key:
    private_key = serialization.load_pem_private_key(
        key.read(),
        password=os.environ["SNOWFLAKE_PASSPHRASE"].encode(),
        backend=default_backend(),
    )

# Serialize the private key to DER format
private_key_bytes = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption(),
)

# Ensure session state exists
if "logged_in" not in st.session_state:
    st.session_state["logged_in"] = False

# Get existing query parameters
query_params = st.query_params
# current_page = query_params.get("page", [""])[0]  # Get 'page' value if it exists
current_page = st.query_params.get("page", "login")  # Default to "login" if not set

# button_style = """
# <style>
# div.stButton > button, div.stDownloadButton > button{
#     width: 200px;  /* Makes buttons full width of their container */
#     padding: 0.5rem 1rem;  /* Consistent padding */
#     font-size: 16px;  /* Consistent font size */
# }
# </style>
# """
# st.markdown(button_style, unsafe_allow_html=True)

# Initialize project switching states
if "show_switch_success" not in st.session_state:
    st.session_state.show_switch_success = False
if "success_project_name" not in st.session_state:
    st.session_state.success_project_name = None

if not st.session_state["logged_in"]:
    if current_page == "signup":
        register_page()  # Show the register page if the query parameter is set to "register"
    elif current_page == "login":
        login()  # Show the login page by default
    elif current_page == "forgot_password":
        forgot_password()
    elif current_page == "reset_password":
        reset_password()
    elif current_page=='activate':
        activate_account()
    elif current_page=='change_password':
        change_password_form()
    elif current_page=='create_user':
        create_new_user_page()
    else:
        st.write('Token Expired. LogIn Again')
        if st.button("Login"):
            # st.experimental_set_query_params(page="login")
            # st.rerun()
            st.markdown(f'<meta http-equiv="refresh" content="0;url=/?page=login">', unsafe_allow_html=True)
    st.stop()
else:
    if not is_authenticated():
        st.error("Your Token Expired.You need to log in first.")
        # Optionally, redirect the user to the login page
        st.markdown(f'<meta http-equiv="refresh" content="0;url=/?page=login">', unsafe_allow_html=True)
    else:
        selected_schema = st.session_state.get("schema", None)
        selected_project = str(st.session_state.get("selected_project", "")).lower()
        def create_snowflake_connection():
            conn = snowflake.connector.connect(
                user=os.getenv('SNOWFLAKE_USER'),
                private_key= private_key_bytes,
                account=os.getenv('SNOWFLAKE_ACCOUNT'),
                warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
                database=os.getenv('SNOWFLAKE_DATABASE'),
                authenticator="SNOWFLAKE_JWT",
                schema=selected_schema,
                role=os.getenv('SNOWFLAKE_ROLE'),
                    )
            return conn

        def export_elvis_data():
            """
            Connect to the database and download Elvis table as CSV directly
            """
            try:
                from automated_refresh_flow_new import PROJECTS, fetch_data
                
                project_config = PROJECTS[st.session_state["selected_project"]]
                elvis_config = project_config['databases']["elvis"]
                table_name = elvis_config['table']
                database_name = elvis_config["database"]
                
                with st.spinner("🔄 Downloading Elvis data..."):
                    csv_buffer = fetch_data(database_name, table_name)
                    
                    if csv_buffer:
                        # Create filename
                        project_name = st.session_state["selected_project"].lower().replace(" ", "_")
                        file_name = f"{project_name}_elvis_db.csv"
                        
                        # Use the existing CSV buffer directly - no conversion needed
                        st.download_button(
                            label="✅ Download Complete - Click to Save",
                            data=csv_buffer.getvalue(),
                            file_name=file_name,
                            mime="text/csv",
                            key="elvis_download"
                        )
                        
                    else:
                        st.error("❌ Failed to fetch data from Elvis table")
                        
            except Exception as e:
                st.error(f"❌ Error exporting Elvis data: {str(e)}")



        pinned_column='ROUTE_SURVEYEDCode'


         # @st.cache(allow_output_mutation=True)  # Use st.cache in Streamlit 1.6.0
        def fetch_dataframes_from_snowflake(cache_key):
            """
            Fetches data from Snowflake tables and returns them as a dictionary of DataFrames.
            If a table is missing or an error occurs, it returns an empty DataFrame.

            Returns:
                dict: A dictionary where keys are DataFrame names and values are DataFrames.
            """
            # Snowflake connection details
            conn = create_snowflake_connection()
            cur = conn.cursor()

            # Table-to-DataFrame mapping
            table_to_df_mapping = {
                'wkday_stationwise_comparison': 'wkday_stationwise_df',
                'wkend_stationwise_comparison': 'wkend_stationwise_df',
                'wkday_comparison': 'wkday_df',
                'wkday_dir_comparison': 'wkday_dir_df',
                'wkend_comparison': 'wkend_df',
                'wkend_dir_comparison': 'wkend_dir_df',
                'wkend_time_data': 'wkend_time_df',
                'wkday_time_data': 'wkday_time_df',
                'wkend_raw': 'wkend_raw_df',
                'wkday_raw': 'wkday_raw_df',
                'TOD': 'detail_df',
                'by_interv_totals': 'by_interv_totals_df',
                'by_route_totals': 'by_route_totals_df',
                'survey_detail_totals': 'survey_detail_totals_df',
                'surveyor_report_trends': 'surveyor_report_trends_df',
                'route_report_trends': 'route_report_trends_df',
                'surveyor_report_date_trends': 'surveyor_report_date_trends_df',
                'route_report_date_trends': 'route_report_date_trends_df',
                'route_comparison': 'route_comparison_df',
                'reverse_routes': 'reverse_routes_df',
                'reverse_routes_difference': 'reverse_routes_difference_df',
                'low_response_questions': 'low_response_questions_df',
                'refusal_analysis_report': 'refusal_analysis_df',
                'refusal_race_report': 'refusal_race_df',
            }

            # Initialize an empty dictionary to hold DataFrames
            dataframes = {}

            try:
                # Loop through each table, fetch its data, and store it in the corresponding DataFrame
                for table_name, df_name in table_to_df_mapping.items():
                    try:
                        # Query to fetch data
                        query = f"SELECT * FROM {table_name}"
                        
                        # Execute query and fetch data
                        cur.execute(query)
                        data = cur.fetchall()

                        # Get column names from the cursor description
                        columns = [desc[0] for desc in cur.description] if cur.description else []

                        # Convert data to DataFrame
                        df = pd.DataFrame(data, columns=columns) if data else pd.DataFrame(columns=columns)
                        dataframes[df_name] = df

                        print(f"Data fetched and stored in DataFrame: {df_name}")

                    except Exception as e:
                        # Handle cases where the table doesn't exist or another error occurs
                        print(f"Error fetching data from {table_name}: {e}")
                        dataframes[df_name] = pd.DataFrame()  # Return an empty DataFrame instead of None

            finally:
                # Close cursor and connection
                cur.close()
                conn.close()

            return dataframes

        # Fetch dataframes from Snowflake
        if "cache_key" not in st.session_state:
            st.session_state["cache_key"] = 0
        dataframes = fetch_dataframes_from_snowflake(st.session_state["cache_key"] )

        # Access DataFrames from the fetched dataframes dictionary
        wkday_df = dataframes.get('wkday_df', pd.DataFrame())
        wkday_dir_df = dataframes.get('wkday_dir_df', pd.DataFrame())
        wkend_df = dataframes.get('wkend_df', pd.DataFrame())
        wkend_dir_df = dataframes.get('wkend_dir_df', pd.DataFrame())
        wkend_time_df = dataframes.get('wkend_time_df', pd.DataFrame())
        wkday_time_df = dataframes.get('wkday_time_df', pd.DataFrame())
        wkend_raw_df = dataframes.get('wkend_raw_df', pd.DataFrame())
        wkday_raw_df = dataframes.get('wkday_raw_df', pd.DataFrame())
        detail_df = dataframes.get('detail_df', pd.DataFrame())
        surveyor_report_trends_df = dataframes.get('surveyor_report_trends_df', pd.DataFrame())
        route_report_trends_df = dataframes.get('route_report_trends_df', pd.DataFrame())
        surveyor_report_date_trends_df = dataframes.get('surveyor_report_date_trends_df', pd.DataFrame())
        route_report_date_trends_df = dataframes.get('route_report_date_trends_df', pd.DataFrame())

        wkday_stationwise_df = dataframes.get('wkday_stationwise_df', pd.DataFrame())
        wkend_stationwise_df = dataframes.get('wkend_stationwise_df', pd.DataFrame())

        by_interv_totals_df = dataframes.get('by_interv_totals_df', pd.DataFrame())
        by_route_totals_df = dataframes.get('by_route_totals_df', pd.DataFrame())
        survey_detail_totals_df = dataframes.get('survey_detail_totals_df', pd.DataFrame())

        route_comparison_df = dataframes.get('route_comparison_df', pd.DataFrame())
        reverse_routes_df = dataframes.get('reverse_routes_df', pd.DataFrame())
        reverse_routes_difference_df = dataframes.get('reverse_routes_difference_df', pd.DataFrame())
        low_response_questions_df = dataframes.get('low_response_questions_df', pd.DataFrame())

        refusal_analysis_df = dataframes.get('refusal_analysis_df', pd.DataFrame())
        refusal_race_df = dataframes.get('refusal_race_df', pd.DataFrame())

        # st.sidebar.markdown("**User Profile**")
        # st.sidebar.caption(f"**Role:** {st.session_state['user']['role']}")
        # st.sidebar.caption(f"**Username:** {st.session_state['user']['username']}")
        # st.sidebar.caption(f"**Email:** {st.session_state['user']['email']}")

        # st.sidebar.header("Filters")
        # search_query=st.sidebar.text_input(label='Search', placeholder='Search')

        # st.sidebar.markdown("<div style='flex-grow:1;'></div>", unsafe_allow_html=True)
        
        # if st.sidebar.button('Change Password', key='Change Password Button'):
        #     send_change_password_email(st.session_state['user']['email'])

        # if st.sidebar.button("Logout",key='Logout Button'):
        #     logout()

        ####################################################################################################

        def get_page_key(selected_page):
            mapping = {
                "🏠︎   Home": "main",
                "🗓︎   WEEKDAY-OVERALL": "weekday",
                "☀︎   WEEKEND-OVERALL": "weekend",
                "🕒  Time Of Day Details": "timedetails",
                "🗺️  Location Maps": "location_maps",
                "⤓    LOW RESPONSE QUESTIONS": "low_response_questions_tab",
                "↺   Clone Records": "reverse_routes",
                "⌗  DAILY TOTALS": "dailytotals",
                "∆   Surveyor/Route/Trend Reports": "surveyreport",
                "◉  WEEKDAY StationWise Comparison": "weekday_station",
                "⦾  WEEKEND StationWise Comparison": "weekend_station",
                "🚫  Refusal Analysis": "refusal"
            }
            return mapping.get(selected_page, "main")
        

        user = st.session_state["user"]
        username = user["username"]
        email = user["email"]
        role = user["role"]

        

        # Sidebar Styling
        # --- THEME-SYNCED SIDEBAR STYLING ---
                # --- THEME-ENHANCED SIDEBAR DESIGN ---
        st.markdown("""
        <style>

        /* --- Sidebar container with gradient --- */
        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, color-mix(in srgb, var(--secondary-background-color) 95%, var(--background-color)) 0%, var(--background-color) 100%) !important;
            color: var(--text-color);
            box-shadow: 2px 0 10px rgba(0,0,0,0.08);
            border-right: 1px solid rgba(128,128,128,0.1);
            padding-top: 1rem !important;
            animation: slideIn 0.5s ease-out;
                    
        }
                    
        .stColumn {
            flex:1 !important;
        }
        .stDownloadButton > button , .stButton > button{
            width: 100% !important;
        }

        /* --- Animation --- */
        @keyframes slideIn {
            from { transform: translateX(-25px); opacity: 0; }
            to { transform: translateX(0); opacity: 1; }
        }

        /* --- Profile Card - FIXED FOR COLLAPSED SIDEBAR --- */
        .profile-card {
            background: rgba(255,255,255,0.05);
            margin: -12px 16px 16px;
            border-radius: 14px;
            padding: 14px;
            text-align: center;
            transition: all 0.3s ease;
            border: 1px solid rgba(255,255,255,0.08);
            box-shadow: 0 2px 10px rgba(0,0,0,0.05);
            /* Ensure content fits in collapsed sidebar */
            max-width: calc(100% - 32px);
            box-sizing: border-box;
            word-wrap: break-word;
            overflow: hidden;
        }

        /* Specific styles for when sidebar is collapsed */
        [data-testid="stSidebar"][aria-expanded="false"] .profile-card {
            padding: 10px 8px !important;
            margin: 12px 8px !important;
            max-width: calc(100% - 16px);
        }

        [data-testid="stSidebar"][aria-expanded="false"] .profile-initial {
            width: 40px !important;
            height: 40px !important;
            font-size: 1rem !important;
            margin: 0 auto 6px !important;
        }

        [data-testid="stSidebar"][aria-expanded="false"] .profile-card div {
            font-size: 0.8rem !important;
            line-height: 1.2 !important;
        }

        [data-testid="stSidebar"][aria-expanded="false"] .profile-card div:not(:first-child) {
            font-size: 0.7rem !important;
        }

        .profile-card:hover {
            transform: scale(1.02);
            box-shadow: 0 4px 18px rgba(66,133,244,0.25);
            border-color: var(--primary-color);
        }

        /* --- FIXED: Profile Initial for Light/Dark Mode --- */
        .profile-initial {
            width: 52px;
            height: 52px;
            border-radius: 50%;
            background: #4285f4 !important; /* Explicit blue color */
            color: white !important; /* Force white text */
            display: flex;
            align-items: center;
            justify-content: center;
            font-weight: 600;
            font-size: 1.3rem;
            margin: 0 auto 10px;
            transition: all 0.3s ease;
            border: 2px solid rgba(255,255,255,0.3);
            box-shadow: 0 2px 8px rgba(66, 133, 244, 0.3);
        }

        /* Ensure visibility in all themes */
        [data-theme="light"] .profile-initial,
        [data-theme="dark"] .profile-initial,
        body[data-theme="light"] .profile-initial,
        body[data-theme="dark"] .profile-initial {
            background: #4285f4 !important;
            color: white !important;
        }

        [data-testid="stSidebar"][aria-expanded="false"] .profile-initial {
            width: 40px !important;
            height: 40px !important;
            font-size: 1rem !important;
            margin: 0 auto 6px !important;
        }

        /* --- Section label - Adjust for collapsed sidebar --- */
        .section-label {
            text-transform: uppercase;
            font-size: 11px;
            opacity: 0.6;
            margin: 12px 16px 4px;
            letter-spacing: 0.05em;
            transition: all 0.3s ease;
        }

        [data-testid="stSidebar"][aria-expanded="false"] .section-label {
            font-size: 9px !important;
            margin: 10px 8px 2px !important;
            text-align: center;
        }

        /* --- Search Box --- */
        input[type="text"] {
            background-color: color-mix(in srgb, var(--secondary-background-color) 95%, var(--background-color)) !important;
            color: var(--text-color) !important;
            border: 1px solid rgba(128,128,128,0.25) !important;
            border-radius: 6px !important;
        }

        /* --- Dropdown --- */
        div[data-baseweb="select"] {
            background: color-mix(in srgb, var(--secondary-background-color) 92%, var(--background-color));
            border-radius: 10px;
        }

        /* --- Buttons --- */
        .stButton > button {
            color: var(--text-color);
            flex: 1;
            width: 100%;
            border-radius: 10px;
            border: 1px solid rgba(128,128,128,0.25) !important;
            font-weight: 500;
            transition: all 0.2s ease;
            background: color-mix(in srgb, var(--secondary-background-color) 96%, var(--background-color)) !important;
        }

        .stButton > button:hover {
            background-color: var(--primary-color) !important;
            color: #fff !important;
            transform: translateY(-1px);
            box-shadow: 0 4px 8px rgba(66,133,244,0.3);
        }

        /* --- Active Button --- */
        .active-button > button {
            border-left: 4px solid var(--primary-color);
            background-color: color-mix(in srgb, var(--primary-color) 10%, var(--background-color)) !important;
            font-weight: 600;
        }

        /* --- Bottom Buttons - REMOVED BORDER --- */
        .bottom-buttons {
            position: fixed;
            bottom: 20px;
            left: 0;
            width: 15.5rem;
            padding: 12px 16px;
            /* REMOVED: border-top: 1px solid rgba(128,128,128,0.15); */
            background: color-mix(in srgb, var(--secondary-background-color) 97%, var(--background-color));
            z-index: 999;
            transition: all 0.3s ease;
        }

        [data-testid="stSidebar"][aria-expanded="false"] .bottom-buttons {
            width: 4.5rem !important;
            padding: 8px !important;
        }

        [data-testid="stSidebar"][aria-expanded="false"] .stButton > button {
            padding: 0.4rem 0.3rem !important;
            font-size: 0.7rem !important;
            min-height: auto !important;
        }

        /* --- Scrollbar --- */
        [data-testid="stSidebar"]::-webkit-scrollbar {
            width: 6px;
        }
        [data-testid="stSidebar"]::-webkit-scrollbar-thumb {
            background: var(--primary-color);
            border-radius: 10px;
        }

        /* --- Logout and Change Password Buttons --- */
        #ChangePasswordButton {
            background-color: #2ecc71 !important;
            color: white !important;
        }
        #ChangePasswordButton:hover {
            background-color: #27ae60 !important;
        }

        #LogoutButton {
            background-color: #e63946 !important;
            color: white !important;
        }
        #LogoutButton:hover {
            background-color: #c62828 !important;
        }

        /* --- Ensure text doesn't overflow in collapsed state --- */
        [data-testid="stSidebar"][aria-expanded="false"] * {
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }

        /* Allow profile card text to wrap properly */
        [data-testid="stSidebar"][aria-expanded="false"] .profile-card * {
            white-space: normal !important;
            word-break: break-word !important;
        }

        </style>
        """, unsafe_allow_html=True)

        st.markdown("""
        <style>
        /* 🌐 GLOBAL BUTTON DESIGN — works for all buttons (sidebar + main page) */
        .stButton > button {
            width: 100%;
            border: none !important;
            padding: 0.6rem 1rem !important;
            border-radius: 10px !important;
            font-weight: 500 !important;
            transition: all 0.25s ease !important;
            cursor: pointer !important;
            font-size: 0.95rem !important;
        }

        /* 🌞 LIGHT THEME STYLE */
        @media (prefers-color-scheme: light) {
            .stButton > button {
                background: #f5f6f7 !important;          /* clean soft gray */
                color: #222 !important;                  /* dark text */
                border: 1px solid #dcdcdc !important;
                box-shadow: 0 2px 5px rgba(0,0,0,0.05);
            }
            .stButton > button:hover {
                background: linear-gradient(90deg, #4285f4, #6fa8ff) !important; /* blue gradient */
                color: #fff !important;                  /* white text on hover */
                border: none !important;
                box-shadow: 0 6px 18px rgba(66,133,244,0.25);
                transform: translateY(-2px);
            }
        }

        /* 🌙 DARK THEME STYLE */
        @media (prefers-color-scheme: dark) {
            .stButton > button {
                background: #2b2f3a !important;           /* deep gray */
                color: #f4f4f4 !important;                /* bright text */
                border: 1px solid rgba(255,255,255,0.08);
                box-shadow: 0 2px 5px rgba(0,0,0,0.25);
            }
            .stButton > button:hover {
                background: linear-gradient(90deg, #4285f4, #6fa8ff) !important; /* blue gradient */
                color: #fff !important;
                box-shadow: 0 6px 18px rgba(66,133,244,0.35);
                transform: translateY(-2px);
            }
        }

        /* 💫 Active button effect (when pressed) */
        .stButton > button:active {
            transform: scale(0.98);
            box-shadow: 0 2px 6px rgba(0,0,0,0.1);
        }

        /* 🔘 Rounded focus ring */
        .stButton > button:focus {
            outline: 2px solid rgba(66,133,244,0.5);
            outline-offset: 2px;
        }

        /* 🔻Bottom fixed buttons section (sidebar only) */
        [data-testid="stSidebar"] .bottom-buttons {
            position: fixed !important;
            bottom: 20px !important;
            left: 12px !important;
            right: 12px !important;
            padding-top: 10px !important;
        }

        /* 🟢 Change Password button */
        #ChangePasswordButton button {
            background-color: #2ecc71 !important;
            color: white !important;
        }
        #ChangePasswordButton button:hover {
            background-color: #27ae60 !important;
        }

        /* 🔴 Logout button */
        #LogoutButton button {
            background-color: #e63946 !important;
            color: white !important;
        }
        #LogoutButton button:hover {
            background-color: #c62828 !important;
        }
        </style>
        """, unsafe_allow_html=True)


        st.markdown("""
        <style>
        /* Sidebar background & theme-aware text */
        [data-testid="stSidebar"] {
            background-color: var(--background-color);
            color: var(--text-color);
        }

        /* Profile Card - theme aware */
        .sidebar-profile {
            background-color: rgba(255, 255, 255, 0.08); /* Light overlay */
            border: 1px solid rgba(255, 255, 255, 0.1);
            border-radius: 20px;
            padding: 1.2rem;
            text-align: center;
            margin: 1rem 0;
            box-shadow: 0 0 15px rgba(0, 0, 0, 0.1);
            transition: background-color 0.3s ease, box-shadow 0.3s ease;
        }
        body[data-theme="light"] .sidebar-profile {
            background-color: rgba(240, 248, 255, 0.6);
            border: 1px solid rgba(0, 123, 255, 0.15);
        }
        .sidebar-profile:hover {
            box-shadow: 0 0 20px rgba(0, 123, 255, 0.2);
        }

        /* Text inside profile */
        .sidebar-profile h3 {
            color: var(--text-color);
            font-weight: 600;
            margin-bottom: 0.4rem;
        }
        .sidebar-profile small {
            color: var(--secondary-text-color);
            font-size: 0.8rem;
        }

        /* Search box */
        [data-testid="stTextInput"] input {
            border-radius: 10px;
            border: 1px solid rgba(0, 123, 255, 0.4);
        }

        /* Buttons */
        div[data-testid="stButton"] > button {
            width: 100%;
            border-radius: 12px;
            background-color: #007BFF;
            color: white;
            font-weight: 500;
            transition: all 0.2s ease;
        }
        div[data-testid="stButton"] > button:hover {
            background-color: #0056b3;
        }

        /* Section labels */
        .sidebar-section-label {
            color: var(--text-color);
            font-size: 0.8rem;
            opacity: 0.7;
            text-transform: uppercase;
            margin-top: 1rem;
        }
        .stButton > button, .stDownloadButton > button {
            background: #f5f6f7 !important;
            color: #222 !important;
            border: 1px solid #dcdcdc !important;
            border-radius: 12px !important;
            padding: 0.6rem 1.5rem !important;
            font-weight: 500 !important;
            font-size: 0.95rem !important;
            transition: all 0.25s ease !important;
            height: 2.6rem !important;
            white-space: nowrap !important; /* keeps text in one line */
            text-align: center !important;
        }

        .stButton > button:hover, .stDownloadButton > button:hover {
            background: linear-gradient(90deg, #4285f4, #6fa8ff) !important;
            color: white !important;
            border: none !important;
            box-shadow: 0 6px 18px rgba(66,133,244,0.25);
            transform: translateY(-2px);
        }
        </style>
        """, unsafe_allow_html=True)

        st.markdown("""
        <style>
        /* Project switcher styles */
        .project-switcher-card {
            background: rgba(255,255,255,0.05);
            border-radius: 10px;
            padding: 12px;
            margin: 8px 0;
            border: 1px solid rgba(255,255,255,0.1);
        }

        .current-project-badge {
            background: linear-gradient(135deg, #667eea, #764ba2);
            color: white;
            padding: 6px 12px;
            border-radius: 20px;
            font-size: 0.8rem;
            font-weight: 600;
            display: inline-block;
            margin: 4px 0;
        }

        [data-testid="stSidebar"][aria-expanded="false"] .project-switcher-card {
            padding: 8px !important;
            margin: 4px 0 !important;
        }
        </style>
        """, unsafe_allow_html=True)
                



        # --- Sidebar Layout ---
        with st.sidebar:
            # --- Profile Section ---
            st.markdown(f"""
            <div class="profile-card">
                <div class="profile-initial">{username[0].upper()}</div>
                <div><strong>{username}</strong></div>
                <div style="font-size:12px;color:#b3b3b3;">{email}</div>
                <div style="font-size:12px;color:#9a9a9a;">Role: {role}</div>
                <div style="font-size:11px;color:#666;margin-top:8px;padding:4px 8px;background:rgba(255,255,255,0.1);border-radius:6px;">
                    📁 Project: <strong>{st.session_state.get("selected_project", "None")}</strong>
                </div>
            </div>
            """, unsafe_allow_html=True)

            st.markdown("<div class='section-label'>🔍 Filters</div>", unsafe_allow_html=True)
            search_query = st.text_input("Search", placeholder="Search here...", label_visibility="collapsed")

            # === ENHANCED PROJECT SWITCHER WITH MODAL ===
            # if role.upper() == "ADMIN":
            st.markdown("---")
            st.markdown("<div class='section-label'>🚀 Admin Controls</div>", unsafe_allow_html=True)
            
            current_project = st.session_state.get("selected_project", "")
            available_projects = list(schema_value.keys())
            
            # Display current project
            # st.info(f"**Current:** {current_project}")
            
            # Project selection dropdown
            selected_new_project = st.selectbox(
                "Switch to Project",
                available_projects,
                index=available_projects.index(current_project) if current_project in available_projects else 0,
                key="project_selector",
                help="Select a project to switch to",
                label_visibility="collapsed"
            )
            
            #  Direct switch button (no confirmation)
            if selected_new_project != current_project:
                if st.button("🔄 Switch Project", use_container_width=True, key="switch_project_direct"):
                    # Perform the switch immediately
                    st.session_state["selected_project"] = selected_new_project
                    st.session_state["schema"] = schema_value[selected_new_project]
                    
                    # Clear cached data
                    keys_to_clear = ['wkday_raw_df', 'wkend_raw_df', 'filtered_wkday_df', 'filtered_wkend_df']
                    for key in keys_to_clear:
                        if key in st.session_state:
                            del st.session_state[key]
                    
                    # Show success message
                    st.session_state["show_switch_success"] = True
                    st.session_state["success_project_name"] = selected_new_project
                    st.rerun()


            

            st.markdown("<div class='section-label'>📁 Completion Report</div>", unsafe_allow_html=True)

            # --- Menu Items ---
            menu_items = [
                "🏠︎   Home",
                "🗓︎   WEEKDAY-OVERALL",
                "☀︎   WEEKEND-OVERALL",
                "🕒  Time Of Day Details"
            ]

            if 'actransit' in selected_project:
                menu_items.extend(["🚫  Refusal Analysis", "⤓    LOW RESPONSE QUESTIONS",
                "🗺️  Location Maps"])

            if 'kcata' in selected_project or ('actransit' in selected_project and 'rail' not in selected_schema.lower()):
                menu_items.append("↺   Clone Records")

            if any(p in selected_project for p in ['stl', 'kcata', 'actransit']):
                menu_items.extend(["⌗  DAILY TOTALS", "∆   Surveyor/Route/Trend Reports"])

            if 'rail' in selected_schema.lower():
                menu_items.extend(["◉  WEEKDAY StationWise Comparison", "⦾  WEEKEND StationWise Comparison"])

            # --- Session State ---
            if "selected_page" not in st.session_state:
                st.session_state.selected_page = "🏠︎   Home"

            # --- Dropdown for Navigation ---
            selected_page = st.selectbox(
                "",
                menu_items,
                index=menu_items.index(st.session_state.selected_page) if st.session_state.selected_page in menu_items else 0,
                key="sidebar_menu",
                label_visibility="collapsed"
            )

            # --- Detect Page Change ---
            if selected_page != st.session_state.selected_page:
                st.session_state.selected_page = selected_page
                st.query_params["page"] = get_page_key(selected_page)
                st.rerun()

            # --- Bottom Buttons ---
            st.markdown('<div class="bottom-buttons">', unsafe_allow_html=True)
            if st.button('Change Password', key='ChangePasswordButton'):
                send_change_password_email(st.session_state['user']['email'])
            if st.button("Logout", key='LogoutButton'):
                logout()
            st.markdown('</div>', unsafe_allow_html=True) 


        # === ADD PROFESSIONAL HEADER HERE - REPLACE YOUR CURRENT HEADER ===
        def create_professional_header():
            # === DATE SETUP ===
            current_date = datetime.datetime.now()
            formatted_date = current_date.strftime("%Y-%m-%d %H:%M:%S")

            # Get most recent "Completed" date
            if 'kcata' in selected_project or 'kcata_rail' in selected_project or 'actransit' in selected_project:
                completed_dates = pd.concat([wkday_raw_df['DATE_SUBMITTED'], wkend_raw_df['DATE_SUBMITTED']])
            else:
                completed_dates = pd.concat([wkday_raw_df['Completed'], wkend_raw_df['Completed']])
            most_recent_completed_date = pd.to_datetime(completed_dates).max()

            # Determine total records (based on current page)
            if current_page == "weekend":
                total_records = wkend_df["# of Surveys"].sum()
            else:
                total_records = wkday_df["# of Surveys"].sum()

            # === STYLING ===
            st.markdown("""
            <style>
            .professional-header {
                background: linear-gradient(135deg, #356AE6 0%, #7AB8FF 100%);
                border-radius: 18px;
                padding: 2rem 2.5rem;
                margin-bottom: 2.2rem;
                color: #1a2a44;
                position: relative;
                box-shadow: 0 6px 20px rgba(0, 0, 0, 0.08);
                border: 1px solid rgba(170, 200, 255, 0.6);
                overflow: hidden;
                margin-top: -80px;
            }
           
             .professional-header::before {
                content: '';
                position: absolute;
                inset: 0;
                background: radial-gradient(circle at top right, rgba(255,255,255,0.35), transparent 70%);
                z-index: 0;
            }

            .header-content {
                display: flex;
                justify-content: space-between;
                align-items: center;
                position: relative;
                z-index: 1;
                gap: 1rem;
            }

            .header-left h1 {
                font-size: 2.2rem;
                font-weight: 700;
                letter-spacing: -0.5px;
                margin-bottom: 0.3rem;
                color: #0c2c54;
            }

            .header-left p {
                font-size: 1.05rem;
                font-weight: 400;
                opacity: 0.9;
                margin: 0;
                color: #163a5f;
            }

            .metric-group {
                display: flex;
                gap: 0.8rem;
                flex-wrap: wrap;
                justify-content: flex-end;
            }

            .metric-card {
                background: rgba(255,255,255,0.6);
                backdrop-filter: blur(12px);
                border-radius: 10px;
                border: 1px solid rgba(180, 200, 255, 0.5);
                padding: 0.75rem 1rem;
                text-align: center;
                min-width: 160px;
                transition: all 0.25s ease;
                box-shadow: 0 2px 6px rgba(0,0,0,0.05);
        }

        .metric-card:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 12px rgba(0,0,0,0.08);
            background: rgba(255,255,255,0.7);
        }

        .metric-label {
            font-size: 0.8rem;
            font-weight: 500;
            color: #123f72;
            margin-bottom: 4px;
        }

        .metric-value {
            font-size: 1.05rem;
            font-weight: 600;
            color: #1e3a5f;
            background: #ffffff;
            padding: 4px 10px;
            border-radius: 6px;
            display: inline-block;
            font-family: 'Roboto Mono', 'Menlo', monospace;
            letter-spacing: -0.3px;
        }

        @media (max-width: 1100px) {
            .header-content {
                flex-direction: column;
                align-items: flex-start;
                gap: 1.2rem;
            }
            .metric-group {
                justify-content: flex-start;
            }
            .header-left h1 {
                font-size: 1.9rem;
            }
        }
        </style>
        """, unsafe_allow_html=True)

            # === HEADER CONTENT ===
            st.markdown(f"""
            <div class="professional-header">
                <div class="header-content">
                    <div class="header-left">
                        <h1>📘 Completion Report</h1>
                        <p>Comprehensive Route Performance Overview</p>
                    </div>
                    <div class="metric-group">
                        <div class="metric-card">
                            <p class="metric-label">Total Records</p>
                            <p class="metric-value">{total_records:,}</p>
                        </div>
                        <div class="metric-card">
                            <p class="metric-label">⏱ Last Refresh</p>
                            <p class="metric-value">{formatted_date}</p>
                        </div>
                        <div class="metric-card">
                            <p class="metric-label">Last Completed</p>
                            <p class="metric-value">{most_recent_completed_date.strftime('%Y-%m-%d %H:%M:%S')}</p>
                        </div>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)

        # Call the function right after your sidebar
        create_professional_header()
        # === END PROFESSIONAL HEADER ===

        # === SUCCESS MESSAGE (Auto-disappearing) ===
        if st.session_state.get("show_switch_success", False):
            success_project = st.session_state.get("success_project_name", "")
            
            # Success message that auto-disappears
            success_placeholder = st.empty()
            with success_placeholder.container():
                st.success(f"✅ Successfully switched to **{success_project}**! Loading new data...")
            
            # Auto-remove after 3 seconds
            import time
            time.sleep(3)
            success_placeholder.empty()
            
            # Clear the success state
            del st.session_state["show_switch_success"]
            del st.session_state["success_project_name"]
####################################################################################################



        def filter_dataframe(df, query):
            if query:
                df = df[df.apply(lambda row: row.astype(str).str.contains(query, case=False).any(), axis=1)]

            return df

        def time_details(details_df):
            if 'kcata' in selected_project:
                # Check if columns exist before renaming
                column_mapping = {
                    'OPPO_TIME[CODE]': 'Time Period Code',
                    'TIME_ON[Code]': 'Time Code',
                    'TIME_ON': 'Time Description',
                    'TIME_PERIOD[Code]': 'Period Code',
                    'TIME_PERIOD': 'Period Description',
                    'START_TIME': 'Start Time',
                    'AGENCY': 'Agency',
                    'WKEND_TIME_PERIOD[Code]': 'Weekend Period Code',
                    'WKEND_TIME_PERIOD': 'Weekend Period Description'
                }
                details_df = details_df.rename(columns=column_mapping)
            
            st.dataframe(details_df, height=670, use_container_width=True)
            
            if st.button("Home Page"):
                st.query_params["page"] = "main"
                st.rerun()


        
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

        def main_page(data1, data2, data3):
            """Main page display with dynamic data"""
            # Create two columns layout
            col1, col2 = st.columns([2, 1])  # Left column is wider

            # Display the first dataframe on the left full screen (col1)
            with col1:
                if current_page=='main':
                    st.subheader('Route Direction Level Comparison (WeekDAY)')
                else:
                    st.subheader("Route Direction Level Comparison")
                filtered_df1 = filter_dataframe(data1, search_query)


                # render_aggrid(filtered_df1,500,'ROUTE_SURVEYEDCode',1)
                st.dataframe(filtered_df1, use_container_width=True, hide_index=True)
                # csv1, file_name1 = create_csv(filtered_df1, "route_direction_comparison.csv")
                # download_csv(csv1, file_name1, "Download Route Direction Comparison Data")

                filtered_df3 = filter_dataframe(data3, search_query)
                st.subheader("Route Level Comparison")
                # render_aggrid(filtered_df3,400,'ROUTE_SURVEYEDCode',2)
                st.dataframe(filtered_df3, use_container_width=True, hide_index=True)
                # csv3, file_name3 = create_csv(filtered_df3, "route_level_comparison.csv")
                # download_csv(csv3, file_name3, "Download Route Level Comparison Data")

            # Display buttons and dataframes in the second column (col2)
            with col2:

                st.subheader("Time Range Data")
                # Convert relevant columns in both dataframes to numeric values, handling errors
                data1[['(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal', '(5) Goal']] = data1[['(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal', '(5) Goal']].apply(pd.to_numeric, errors='coerce')
                data2[['1', '2', '3', '4', '5']] = data2[['1', '2', '3', '4', '5']].apply(pd.to_numeric, errors='coerce')

                # Fill any NaN values with 0 (or handle them differently if needed)
                data1[['(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal', '(5) Goal']] = data1[['(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal', '(5) Goal']].fillna(0)
                data2[['1', '2', '3', '4', '5']] = data2[['1', '2', '3', '4', '5']].fillna(0)

                # Calculate the sums for expected and collected totals
                expected_totals = data1[['(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal', '(5) Goal']].sum()
                collected_totals = data2[['1', '2', '3', '4', '5']].sum()

                # Calculate the difference, ensuring no negative values
                difference = np.maximum(expected_totals.values - collected_totals.values, 0)
                result_df = pd.DataFrame({
                    'Time Period':  [ '1', '2', '3', '4', '5'],
                    'Collected Totals': collected_totals.values.astype(int),
                    'Expected Totals': expected_totals.values.astype(int),
                    'Remaining': difference.astype(int),
                })



                filtered_df2 = filter_dataframe(data2, search_query)

                # render_aggrid(filtered_df2,500,'Display_Text',3)
                st.dataframe(filtered_df2, use_container_width=True, hide_index=True)
                # csv2, file_name2 = create_csv(filtered_df2, "time_range_data.csv")
                # download_csv(csv2, file_name2, "Download Time Range Data")



                filtered_df4 = filter_dataframe(result_df, search_query)
            
                # Render AgGrid
                st.subheader("Time Period OverAll Data")
                # render_aggrid(filtered_df4,400,'Time Period',4)
                st.dataframe(filtered_df4, use_container_width=True, hide_index=True)

                # csv4, file_name4 = create_csv(filtered_df4, "time_period_overall_data.csv")
                # download_csv(csv4, file_name4, "Download Time Period Overall Data")


        def weekday_page():
            st.title("Weekday OverAll Data")
            if 'uta' in selected_project:
                wkday_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED','STATION_ID',  '(0) Collect', '(0) Remain','(1) Collect', '(1) Remain',
                                        '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain','(5) Collect', '(5) Remain',
                                        '(0) Goal','(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal','(5) Goal']
                wkday_time_columns=['Display_Text', 'Original Text', 'Time Range', '1', '2', '3', '4','5']
            elif 'tucson' in selected_project:
                wkday_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', '(1) Collect', '(1) Remain',
                                        '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain',
                                        '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal']
                wkday_time_columns=['Display_Text', 'Original Text', 'Time Range', '1', '2', '3', '4']
            elif 'stl' in selected_project:
                wkday_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', '(1) Collect', '(1) Remain',
                                        '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain',
                                        '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal']
                wkday_time_columns=['Display_Text', 'Original Text', 'Time Range', '1', '2', '3', '4']
            elif 'kcata' in selected_project or 'actransit' in selected_project:
                wkday_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', '(1) Collect', '(1) Remain',
                                        '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain',
                                        '(5) Collect', '(5) Remain',
                                        '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal', '(5) Goal']
                wkday_time_columns=['Display_Text', 'Original Text', 'Time Range', '1', '2', '3', '4','5']
            else:
                wkday_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', '(0) Collect', '(0) Remain','(1) Collect', '(1) Remain',
                                        '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain','(5) Collect', '(5) Remain',
                                        '(0) Goal','(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal','(5) Goal']
                wkday_time_columns=['Display_Text', 'Original Text', 'Time Range', '1', '2', '3', '4','5']
                
            # day_column_present = check_all_characters_present(wkday_dir_df, ["day"])
            # if day_column_present:
            #     wkday_dir_columns.insert(2,day_column_present[0])
            main_page(wkday_dir_df[wkday_dir_columns],
                        wkday_time_df[wkday_time_columns],
                        wkday_df[['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', 'Route Level Goal', '# of Surveys', 'Remaining']])
            if st.button("Home Page"):
                st.query_params["page"] = "main"
                st.rerun()

        def weekend_page():
            st.title("Weekend OverAll Data")
            day_column_present = check_all_characters_present(wkend_dir_df, ["day"])
            route_level_day_column_present = check_all_characters_present(wkend_df, ["day"])
            if 'uta' in selected_project:

                # day_column_present = check_all_characters_present(wkend_dir_df, ["day"])
                print(day_column_present)
                if day_column_present:
                    # print(wkend_dir_columns)
                    print("Inside If Condition")
                    wkend_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED',day_column_present[0],'STATION_ID',  '(0) Collect', '(0) Remain','(1) Collect', '(1) Remain',
                        '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain','(5) Collect', '(5) Remain',
                        '(0) Goal','(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal','(5) Goal']
                else:
                    wkend_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED','STATION_ID',  '(0) Collect', '(0) Remain','(1) Collect', '(1) Remain',
                                            '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain','(5) Collect', '(5) Remain',
                                            '(0) Goal','(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal','(5) Goal']
                wkend_time_columns=['Display_Text', 'Original Text', 'Time Range', '1', '2', '3', '4','5']
                if route_level_day_column_present:
                    wkend_df_columns=['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED',route_level_day_column_present[0],'Route Level Goal', '# of Surveys', 'Remaining']
                else:
                    wkend_df_columns=['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED','Route Level Goal', '# of Surveys', 'Remaining']
            elif 'tucson' in selected_project:
                if day_column_present:
                    wkend_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', day_column_present[0],'(1) Collect', '(1) Remain',
                                        '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain',
                                        '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal']
                else:
                    wkend_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', '(1) Collect', '(1) Remain',
                            '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain',
                            '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal']
                wkend_time_columns=['Display_Text', 'Original Text', 'Time Range', '1', '2', '3', '4']
                if route_level_day_column_present:
                    wkend_df_columns=['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED',route_level_day_column_present[0],'Route Level Goal', '# of Surveys', 'Remaining']
                else: 
                    wkend_df_columns=['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED','Route Level Goal', '# of Surveys', 'Remaining']
                # wkend_df_columns=['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED','Day' ,'Route Level Goal', '# of Surveys', 'Remaining']
            elif 'stl' in selected_project:
                wkend_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', '(1) Collect', '(1) Remain',
                                        '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain',
                                        '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal']
                wkend_time_columns=['Display_Text', 'Original Text', 'Time Range', '1', '2', '3', '4']
                wkend_df_columns=['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED','Route Level Goal', '# of Surveys', 'Remaining']
            elif 'kcata' in selected_project or 'actransit' in selected_project:
                wkend_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', '(1) Collect', '(1) Remain',
                                        '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain','(5) Collect', '(5) Remain',
                                        '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal', '(5) Goal']
                wkend_time_columns=['Display_Text', 'Original Text', 'Time Range', '1', '2', '3', '4','5']
                wkend_df_columns=['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED','Route Level Goal', '# of Surveys', 'Remaining']
            else:
                if day_column_present:
                    wkend_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', day_column_present[0],'(0) Collect', '(0) Remain','(1) Collect', '(1) Remain',
                                            '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain','(5) Collect', '(5) Remain',
                                            '(0) Goal','(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal','(5) Goal']
                else:
                    wkend_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', '(0) Collect', '(0) Remain','(1) Collect', '(1) Remain',
                        '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain','(5) Collect', '(5) Remain',
                        '(0) Goal','(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal','(5) Goal']
                wkend_time_columns=['Display_Text', 'Original Text', 'Time Range', '1', '2', '3', '4','5']
                if route_level_day_column_present:
                    wkend_df_columns=['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED',route_level_day_column_present,'Route Level Goal', '# of Surveys', 'Remaining']
                else:
                    wkend_df_columns=['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED','Route Level Goal', '# of Surveys', 'Remaining']

            main_page(wkend_dir_df[wkend_dir_columns],
                    wkend_time_df[wkend_time_columns],
                    wkend_df[wkend_df_columns])

            if st.button("Home Page"):
                st.query_params["page"] = "main"
                st.rerun()

        # if "page_type" not in st.session_state:
        #     st.session_state["page_type"] = "weekday"  # Default page
        def weekday_station_page():
            st.subheader('Route StationWise Comparison(WeekDAY)')
            if 'tucson' in selected_project:
                wkday_stationwise_columns=[ 'ROUTE_SURVEYEDCode','ROUTE_SURVEYED','STATION_ID','STATION_NAME','(1) Collect', '(1) Remain',
                                    '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain', 
                                     '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal']
            elif 'kcata rail' in selected_project:    
                wkday_stationwise_columns=[ 'ROUTE_SURVEYEDCode','ROUTE_SURVEYED','STATION_ID','STATION_NAME','(1) Collect', '(1) Remain',
                                    '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain', '(5) Collect', '(5) Remain',
                                     '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal', '(5) Goal']
            else:
                wkday_stationwise_columns=[ 'ROUTE_SURVEYEDCode','ROUTE_SURVEYED','STATION_ID','STATION_NAME', '(0) Collect', '(0) Remain', '(1) Collect', '(1) Remain',
                    '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain', '(5) Collect', '(5) Remain',
                    '(0) Goal', '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal', '(5) Goal']
            filtered_df = filter_dataframe(wkday_stationwise_df[wkday_stationwise_columns], search_query)

            # render_aggrid(filtered_df,500,'ROUTE_SURVEYEDCode',1)
            st.dataframe(filtered_df, use_container_width=True, hide_index=True)

            if st.button("Home Page"):
                st.query_params()
                st.rerun()

        def weekend_station_page():
            st.subheader('Route StationWise Comparison(WeekEND)')
            if 'tucson' in selected_project or 'kcata_rail' in selected_project:
                wkend_stationwise_columns=[ 'ROUTE_SURVEYEDCode','ROUTE_SURVEYED','STATION_ID','STATION_NAME','(1) Collect', '(1) Remain',
                                    '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain', 
                                     '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal']
            elif 'kcata rail' in selected_project:    
                wkend_stationwise_columns=[ 'ROUTE_SURVEYEDCode','ROUTE_SURVEYED','STATION_ID','STATION_NAME','(1) Collect', '(1) Remain',
                                    '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain', '(5) Collect', '(5) Remain',
                                     '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal', '(5) Goal']
            else:
                wkend_stationwise_columns=[ 'ROUTE_SURVEYEDCode','ROUTE_SURVEYED','STATION_ID','STATION_NAME', '(0) Collect', '(0) Remain', '(1) Collect', '(1) Remain',
                    '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain', '(5) Collect', '(5) Remain',
                    '(0) Goal', '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal', '(5) Goal']

            filtered_df = filter_dataframe(wkend_stationwise_df[wkend_stationwise_columns], search_query)

            # render_aggrid(filtered_df,500,'ROUTE_SURVEYEDCode',1)
            st.dataframe(filtered_df, use_container_width=True, hide_index=True)
            if st.button("Home Page"):
                st.query_params()
                st.rerun()

        def low_response_questions_page():
            st.title("📋 LOW RESPONSE QUESTIONS")
            # Load the low response questions dataframe
            low_response_questions_df = dataframes['low_response_questions_df']
            
            # Check if the dataframe exists and is not empty
            if low_response_questions_df is not None and not low_response_questions_df.empty:
                # st.subheader("📋 LOW RESPONSE QUESTIONS")
                st.dataframe(low_response_questions_df, use_container_width=True, hide_index=True)
            else:
                st.warning("No low response questions data available.")

            # Navigation
            if st.button("🔙 Home Page"):
                st.query_params["page"] = "main"
                st.rerun()

        def daily_totals_page():
            if 'stl' in selected_project or 'kcata' in selected_project or 'actransit' in selected_project:
                st.title("📊 Daily Totals - Interviewer and Route Level")
                # Load Snowflake-extracted DataFrames
                by_interv_totals_df = dataframes['by_interv_totals_df']
                by_route_totals_df = dataframes['by_route_totals_df']
                survey_detail_totals_df = dataframes['survey_detail_totals_df']

                # Standardize column names to uppercase for consistent access
                survey_detail_totals_df.columns = survey_detail_totals_df.columns.astype(str).str.strip().str.upper()

                # Ensure DATE column exists
                if 'DATE' not in survey_detail_totals_df.columns:
                    st.error("❌ 'DATE' column not found in survey_detail_totals_df.")
                    st.stop()

                # Convert DATE column to datetime.date and handle errors
                survey_detail_totals_df['DATE'] = pd.to_datetime(survey_detail_totals_df['DATE'], errors='coerce').dt.date

                # Extract unique values for filters
                all_dates = sorted(survey_detail_totals_df['DATE'].dropna().astype(str).unique())
                all_intervs = sorted(survey_detail_totals_df['INTERV_INIT'].dropna().unique())

                # Summary cards
                total_surveys = survey_detail_totals_df['COUNT'].sum()
                unique_interviewers = survey_detail_totals_df['INTERV_INIT'].nunique()
                active_routes = survey_detail_totals_df['ROUTE'].nunique()

                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Surveys", f"{total_surveys:,}")
                with col2:
                    st.metric("Unique Interviewers", unique_interviewers)
                with col3:
                    st.metric("Active Routes", active_routes)

                # Create filter controls in a single row at the top - Interviewer first, then Date
                filter_col1, filter_col2 = st.columns(2)
                with filter_col1:
                    selected_interv = st.selectbox(
                        "Filter by Interviewer:",
                        options=[None] + list(all_intervs),
                        format_func=lambda x: x if x else "— All Interviewers —"
                    )
                with filter_col2:
                    selected_date = st.selectbox(
                        "Filter by Date:",
                        options=[None] + list(all_dates),
                        format_func=lambda x: x if x else "— All Dates —"
                    )

                # Function to add total row
                def add_total_row(df, index_col):
                    if not df.empty:
                        total_row = df.select_dtypes(include=['number']).sum()
                        total_row[index_col] = 'Total'
                        return pd.concat([df, total_row.to_frame().T], ignore_index=True)
                    return df

                # Function to filter columns while keeping all rows
                def filter_date_columns(df, date_col, selected_date_str):
                    if selected_date_str is None:
                        return df
                    
                    # Keep index column, selected date column (if exists), and Total column
                    cols_to_keep = [df.columns[0]]  # First column (index)
                    
                    # Find the matching date column
                    date_cols = [col for col in df.columns if str(col).split()[0] == selected_date_str]
                    if date_cols:
                        cols_to_keep.append(date_cols[0])
                    
                    # Keep Total column if it exists
                    if 'Total' in df.columns:
                        cols_to_keep.append('Total')
                    
                    return df[cols_to_keep]

                # Function to format date columns
                def format_date_columns(df):
                    # Format date columns (those that can be parsed as dates)
                    for col in df.columns:
                        try:
                            # Try to parse the column name as a date
                            pd.to_datetime(col)
                            # If successful, format it
                            df = df.rename(columns={col: str(col).split()[0]})
                        except:
                            continue
                    return df

                # Process data based on filters
                if selected_date or selected_interv:
                    # Filter the detail data based on selections
                    filtered_detail = survey_detail_totals_df.copy()
                    
                    if selected_date:
                        filtered_detail = filtered_detail[filtered_detail['DATE'].astype(str) == selected_date]
                    
                    if selected_interv:
                        filtered_detail = filtered_detail[filtered_detail['INTERV_INIT'] == selected_interv]

                    # Process interviewer data
                    if not filtered_detail.empty:
                        # Pivot interviewer data
                        interv_filtered = filtered_detail.pivot_table(
                            index='INTERV_INIT',
                            columns='DATE',
                            values='COUNT',
                            aggfunc='sum',
                            fill_value=0
                        ).reset_index()
                        
                        # Add Total column
                        interv_filtered['Total'] = interv_filtered.select_dtypes(include=['number']).sum(axis=1)
                        
                        # Format date columns
                        interv_filtered = format_date_columns(interv_filtered)
                        
                        # Add Total row
                        interv_filtered_with_total = add_total_row(interv_filtered, 'INTERV_INIT')
                        
                        # Filter columns if date is selected
                        if selected_date:
                            interv_filtered_with_total = filter_date_columns(interv_filtered_with_total, 'DATE', selected_date)
                    else:
                        # Create empty DataFrame with correct structure
                        columns = ['INTERV_INIT'] + ([selected_date] if selected_date else []) + ['Total']
                        interv_filtered_with_total = pd.DataFrame(columns=columns)

                    # Process route data
                    if not filtered_detail.empty:
                        # Pivot route data
                        route_filtered = filtered_detail.pivot_table(
                            index='ROUTE',
                            columns='DATE',
                            values='COUNT',
                            aggfunc='sum',
                            fill_value=0
                        ).reset_index()
                        
                        # Add Total column
                        route_filtered['Total'] = route_filtered.select_dtypes(include=['number']).sum(axis=1)
                        
                        # Format date columns
                        route_filtered = format_date_columns(route_filtered)
                        
                        # Add Total row
                        route_filtered_with_total = add_total_row(route_filtered, 'ROUTE')
                        
                        # Filter columns if date is selected
                        if selected_date:
                            route_filtered_with_total = filter_date_columns(route_filtered_with_total, 'DATE', selected_date)
                    else:
                        # Create empty DataFrame with correct structure
                        columns = ['ROUTE'] + ([selected_date] if selected_date else []) + ['Total']
                        route_filtered_with_total = pd.DataFrame(columns=columns)
                else:
                    
                    # Format date columns in the original dataframes if needed
                    interv_filtered_with_total = format_date_columns(by_interv_totals_df)
                    route_filtered_with_total = format_date_columns(by_route_totals_df)
                    

                # Main content layout
                col1, col2 = st.columns([1.3, 2])
                
                with col1:
                    st.subheader("👤 Interviewer Totals")
                    st.dataframe(interv_filtered_with_total, use_container_width=True, hide_index=True)

                with col2:
                    st.subheader("🛣️ Route Totals")
                    st.dataframe(route_filtered_with_total, use_container_width=True, hide_index=True)

                # Navigation
                if st.button("🔙 Home Page"):
                    st.query_params["page"] = "main"
                    st.rerun()

        # Function to extract date and clean a column
        # This function extracts the date from a column and creates a new column with the cleaned data
        def extract_date_and_clean_column(df, column_name, new_column_name, split_char='_'):
            df = df.copy()
            df['date'] = df[column_name].str.split(split_char).str[0]
            df[new_column_name] = df[column_name].str.split(split_char).str[1]
            return df

        # Function to display report with optional filtering
        def display_filtered_or_unfiltered_report(
            unfiltered_df: pd.DataFrame,
            filtered_df: pd.DataFrame,
            filter_column_name: str,
            display_column_name: str,
            section_title: str,
            date_label: str
        ):
            st.subheader(section_title)

            # Debug print
            print(f"\nProcessing {section_title}")
            print(f"Filter column in DataFrame: {filter_column_name in filtered_df.columns}")
            print(f"All columns: {filtered_df.columns.tolist()}")

            # Handle case where filter column doesn't exist
            if filter_column_name not in filtered_df.columns:
                # Try alternative column names
                possible_date_columns = ['Date', 'DATE', 'date', 'Survey_Date']
                for col in possible_date_columns:
                    if col in filtered_df.columns:
                        filter_column_name = col
                        break
                else:
                    st.error(f"Could not find date column in {section_title} data")
                    return

            # Prepare date filter options
            temp_df = filtered_df.copy()
            
            # Ensure we have a date column
            temp_df['date'] = pd.to_datetime(temp_df[filter_column_name], errors='coerce')
            
            # Drop rows with invalid dates
            temp_df = temp_df.dropna(subset=['date'])
            
            # Convert to date strings for display
            temp_df['date'] = temp_df['date'].dt.strftime('%Y-%m-%d')
            
            # Get unique dates (now guaranteed to be valid)
            unique_dates = sorted(temp_df['date'].unique())

            # Show filter
            selected_date = st.selectbox(f"{date_label} Date", ["All"] + unique_dates)

            if selected_date == "All":
                st.dataframe(unfiltered_df, use_container_width=True)
            else:
                df_filtered = temp_df[temp_df['date'] == selected_date]
                df_filtered = df_filtered.drop(columns=[filter_column_name])
                
                # Rename and reorder columns
                df_filtered = df_filtered.rename(columns={
                    display_column_name: display_column_name.upper(),
                    'date': 'Date'
                })
                
                # Reorder columns
                first_cols = ['Date', display_column_name.upper()]
                remaining_cols = [col for col in df_filtered.columns if col not in first_cols]
                reordered_df = df_filtered[first_cols + remaining_cols]

                st.dataframe(reordered_df, use_container_width=True)

        def route_comparison_page():
            st.title("Route Comparison")
            
            # Use the pre-fetched dataframe
            if not route_comparison_df.empty:
                # Display the dataframe
                st.dataframe(route_comparison_df, use_container_width=True, height=600)
                
                # Download button
                csv_data, file_name = create_csv(route_comparison_df, "route_comparison.csv")
                download_csv(csv_data, file_name, "Download Route Comparison Data")
            else:
                st.warning("No route comparison data available")
            
            if st.button("Home Page"):
                st.query_params["page"] = "main"
                st.rerun()

        def reverse_routes_page():
            st.title("Reverse Routes Comparison")
            
            # Use the pre-fetched dataframes
            if not reverse_routes_df.empty or not reverse_routes_difference_df.empty:
                
                # Create a merged view of both dataframes
                if not reverse_routes_df.empty and not reverse_routes_difference_df.empty:
                    # Merge both dataframes and remove exact duplicates
                    merged_df = pd.concat([reverse_routes_df, reverse_routes_difference_df], ignore_index=True)
                    
                    # Remove exact duplicates (keeping first occurrence)
                    merged_df = merged_df.drop_duplicates(keep='first')
                    
                    # Sort by ID for better organization
                    if 'id' in merged_df.columns:
                        merged_df = merged_df.sort_values('id')
                    
                elif not reverse_routes_df.empty:
                    merged_df = reverse_routes_df.copy()
                elif not reverse_routes_difference_df.empty:
                    merged_df = reverse_routes_difference_df.copy()
                
                # Reset index to show row numbers
                merged_df = merged_df.reset_index(drop=True)
                merged_df.index = merged_df.index + 1  # Start index from 1 instead of 0
                
                # Display the merged table
                st.subheader("Reverse Routes View")
                
                # Create filters - now with 5 columns
                col1, col2, col3, col4, col5 = st.columns(5)
                
                with col1:
                    # Filter by Type
                    type_options = ['All'] + sorted(merged_df['Type'].dropna().unique().tolist())
                    selected_type = st.selectbox("Filter by Type:", type_options)
                    
                with col2:
                    # Filter by Day Type
                    if 'DAY_TYPE' in merged_df.columns:
                        day_type_options = ['All'] + sorted(merged_df['DAY_TYPE'].dropna().unique().tolist())
                        selected_day_type = st.selectbox("Filter by Day Type:", day_type_options)
                    else:
                        selected_day_type = 'All'
                
                with col3:
                    # Filter by Time Period
                    if 'TIME_PERIOD' in merged_df.columns:
                        time_period_options = ['All'] + sorted(merged_df['TIME_PERIOD'].dropna().unique().tolist())
                        selected_time_period = st.selectbox("Filter by Time Period:", time_period_options)
                    else:
                        selected_time_period = 'All'
                
                with col4:
                    # Filter by Route
                    if 'ROUTE_SURVEYED' in merged_df.columns:
                        route_options = ['All'] + sorted(merged_df['ROUTE_SURVEYED'].dropna().unique().tolist())
                        selected_route = st.selectbox("Filter by Route:", route_options)
                    else:
                        selected_route = 'All'
                
                with col5:
                    # Filter by Direction
                    if 'FINAL_DIRECTION_CODE' in merged_df.columns:
                        direction_options = ['All'] + sorted(merged_df['FINAL_DIRECTION_CODE'].dropna().unique().tolist())
                        selected_direction = st.selectbox("Filter by Direction:", direction_options)
                    else:
                        selected_direction = 'All'
                
                # Apply filters
                filtered_df = merged_df.copy()
                
                if selected_type != 'All':
                    filtered_df = filtered_df[filtered_df['Type'] == selected_type]
                
                if selected_day_type != 'All' and 'DAY_TYPE' in filtered_df.columns:
                    filtered_df = filtered_df[filtered_df['DAY_TYPE'] == selected_day_type]
                
                if selected_time_period != 'All' and 'TIME_PERIOD' in filtered_df.columns:
                    filtered_df = filtered_df[filtered_df['TIME_PERIOD'] == selected_time_period]
                
                if selected_route != 'All' and 'ROUTE_SURVEYED' in filtered_df.columns:
                    filtered_df = filtered_df[filtered_df['ROUTE_SURVEYED'] == selected_route]
                
                if selected_direction != 'All' and 'FINAL_DIRECTION_CODE' in filtered_df.columns:
                    filtered_df = filtered_df[filtered_df['FINAL_DIRECTION_CODE'] == selected_direction]
                
                # Additional search filter
                search_term = st.text_input("Search across all columns:", "")
                if search_term:
                    # Create a mask for searching across all string columns
                    mask = pd.Series(False, index=filtered_df.index)
                    for col in filtered_df.columns:
                        if filtered_df[col].dtype == 'object':  # String columns
                            mask = mask | filtered_df[col].astype(str).str.contains(search_term, case=False, na=False)
                    filtered_df = filtered_df[mask]
                
                # Reset index for display
                display_df = filtered_df.reset_index(drop=True)
                display_df.index = display_df.index + 1
                
                # Display the filtered dataframe with index
                st.dataframe(display_df, use_container_width=True, height=400)
                
                # Show statistics
                st.subheader("Statistics")
                col1, col2, col3, col4, col5 = st.columns(5)  # Updated to 5 columns
                
                with col1:
                    st.metric("Total Records", len(filtered_df))
                
                with col2:
                    if 'Type' in filtered_df.columns and len(filtered_df) > 0:
                        type_counts = filtered_df['Type'].value_counts()
                        if len(type_counts) > 0:
                            st.metric("Most Common Type", type_counts.index[0])
                
                with col3:
                    if 'TIME_PERIOD' in filtered_df.columns and len(filtered_df) > 0:
                        time_period_counts = filtered_df['TIME_PERIOD'].value_counts()
                        if len(time_period_counts) > 0:
                            st.metric("Most Common Time Period", time_period_counts.index[0])
                
                with col4:
                    if 'ROUTE_SURVEYED' in filtered_df.columns and len(filtered_df) > 0:
                        route_counts = filtered_df['ROUTE_SURVEYED'].value_counts()
                        if len(route_counts) > 0:
                            st.metric("Most Common Route", route_counts.index[0])
                
                with col5:
                    if 'FINAL_DIRECTION_CODE' in filtered_df.columns and len(filtered_df) > 0:
                        direction_counts = filtered_df['FINAL_DIRECTION_CODE'].value_counts()
                        if len(direction_counts) > 0:
                            st.metric("Most Common Direction", direction_counts.index[0])
                
                # Download buttons for both individual and merged data
                st.subheader("Download Data")
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    if not reverse_routes_df.empty:
                        csv_reverse, file_reverse = create_csv(reverse_routes_df, "reverse_routes.csv")
                        download_csv(csv_reverse, file_reverse, "Download Reverse Routes")
                
                with col2:
                    if not reverse_routes_difference_df.empty:
                        csv_difference, file_difference = create_csv(reverse_routes_difference_df, "reverse_routes_difference.csv")
                        download_csv(csv_difference, file_difference, "Download Reverse Routes Difference")
                
                with col3:
                    csv_merged, file_merged = create_csv(filtered_df, "merged_reverse_routes.csv")
                    download_csv(csv_merged, file_merged, "Download Filtered View")
                
            else:
                st.warning("No reverse routes data available")
            
            if st.button("Home Page"):
                st.query_params["page"] = "main"
                st.rerun()

        def show_refusal_analysis(refusal_analysis_df, refusal_race_df):
            """
            Display comprehensive refusal analysis statistics using the master tables
            """
            st.title("📊 Refusal Analysis Dashboard")
            refusal_analysis_df['INTERV_INIT'] = refusal_analysis_df['INTERV_INIT'].astype(str)
            refusal_analysis_df = refusal_analysis_df[refusal_analysis_df['INTERV_INIT'] != "999"]
            refusal_analysis_df = refusal_analysis_df.iloc[1:].copy()
            
            if refusal_analysis_df.empty:
                st.warning("No refusal data available. Please sync data first.")
                if st.button("Sync Data"):
                    st.query_params["page"] = "main"
                    st.rerun()
                return
            
            # Filter for refusals only (HAVE_5_MIN_FOR_SURVECode != '1')
            refusal_df = refusal_analysis_df[refusal_analysis_df['HAVE_5_MIN_FOR_SURVECode'].astype(str) != '1'].copy()
            
            if refusal_df.empty:
                st.info("No refusal records found in the dataset (all responses were participations).")
                return
            
            # FIX: Convert DATE_SUBMITTED to datetime safely
            if 'DATE_SUBMITTED' in refusal_df.columns:
                refusal_df['DATE_SUBMITTED'] = pd.to_datetime(refusal_df['DATE_SUBMITTED'], errors='coerce')
            
            # Create tabs for different refusal statistics
            tab1, tab2, tab3, tab4 = st.tabs([
                "📋 Refusal Overview", "👥 Interviewer Refusals", "🛣️ Route Refusals", "🧑‍🤝‍🧑 Demographics"
            ])
            
            with tab1:
                st.subheader("Refusal Overview")
                
                # Overall refusal statistics
                total_refusals = len(refusal_df)
                total_approaches = len(refusal_analysis_df)
                refusal_rate = (total_refusals / total_approaches * 100) if total_approaches > 0 else 0
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Refusals", f"{total_refusals:,}")
                with col2:
                    st.metric("Total Approaches", f"{total_approaches:,}")
                with col3:
                    st.metric("Overall Refusal Rate", f"{refusal_rate:.1f}%")
                
                # Refusal reasons breakdown
                st.subheader("Refusal Reasons Breakdown")
                
                if 'HAVE_5_MIN_LABEL' in refusal_df.columns:
                    refusal_reasons = refusal_df['HAVE_5_MIN_LABEL'].value_counts().reset_index()
                    refusal_reasons.columns = ['Reason', 'Count']
                    refusal_reasons['Percentage'] = (refusal_reasons['Count'] / refusal_reasons['Count'].sum() * 100).round(2)
                    
                    col1, col2 = st.columns([1, 2])
                    
                    with col1:
                        st.dataframe(refusal_reasons, use_container_width=True, hide_index=True)
                    
                    with col2:
                        if not refusal_reasons.empty:
                            fig_reasons = px.pie(refusal_reasons, values='Count', names='Reason',
                                            title="Refusal Reasons Distribution")
                            st.plotly_chart(fig_reasons, use_container_width=True)
                else:
                    st.warning("Refusal reason data not available.")
                
                # Daily refusal trend - FIXED datetime conversion
                st.subheader("Daily Refusal Trend")
                
                if 'DATE_SUBMITTED' in refusal_df.columns and not refusal_df['DATE_SUBMITTED'].isna().all():
                    # Remove rows with invalid dates
                    valid_dates_df = refusal_df.dropna(subset=['DATE_SUBMITTED'])
                    
                    if not valid_dates_df.empty:
                        daily_refusals = valid_dates_df.groupby(valid_dates_df['DATE_SUBMITTED'].dt.date).size().reset_index()
                        daily_refusals.columns = ['Date', 'Refusals']
                        
                        if not daily_refusals.empty:
                            fig_trend = px.line(daily_refusals, x='Date', y='Refusals',
                                            title="Daily Refusal Count Trend")
                            st.plotly_chart(fig_trend, use_container_width=True)
                        else:
                            st.info("No valid date data available for trend analysis.")
                    else:
                        st.info("No valid dates found in the data.")
                else:
                    st.info("Date data not available for trend analysis.")
            
            with tab2:
                st.subheader("Interviewer Refusal Analysis")
                
                # FIX: Check if required columns exist
                if 'INTERV_INIT' not in refusal_analysis_df.columns:
                    st.warning("Interviewer data not available.")
                else:
                    # Get ALL interviewers from the full dataset (including those with 0 refusals)
                    all_interviewers = refusal_analysis_df['INTERV_INIT'].unique()
                    
                    # Interviewer-level refusal statistics - include ALL interviewers
                    interviewer_refusals = refusal_df.groupby('INTERV_INIT').size().reset_index(name='Total Refusals')
                    
                    # Calculate success rates for comparison - for ALL interviewers
                    interviewer_success = refusal_analysis_df.groupby('INTERV_INIT').agg({
                        'HAVE_5_MIN_LABEL': lambda x: (x == 'Yes I can participate in the survey (have 5 min+)').sum() if 'HAVE_5_MIN_LABEL' in refusal_analysis_df.columns else 0,
                        'HAVE_5_MIN_FOR_SURVECode': 'count'
                    }).reset_index()
                    
                    interviewer_success.columns = ['INTERV_INIT', 'Successful Surveys', 'Total Approaches']
                    interviewer_success['Success Rate %'] = (interviewer_success['Successful Surveys'] / interviewer_success['Total Approaches'] * 100).round(2)
                    
                    # FIX: Use outer merge to include ALL interviewers, even those with 0 refusals
                    interviewer_stats = pd.merge(interviewer_success, interviewer_refusals, on='INTERV_INIT', how='left')
                    
                    # Fill NaN values for interviewers with 0 refusals
                    interviewer_stats['Total Refusals'] = interviewer_stats['Total Refusals'].fillna(0)
                    interviewer_stats['Refusal Rate %'] = (interviewer_stats['Total Refusals'] / interviewer_stats['Total Approaches'] * 100).round(2)
                    
                    # Rename for display
                    interviewer_stats_display = interviewer_stats.rename(columns={'INTERV_INIT': 'Interviewer'})
                    
                    # FIX: Show ALL interviewers, not just top 10
                    st.write(f"**All Interviewers Refusal Statistics ({len(interviewer_stats_display)} total)**")
                    
                    # Add search and filter functionality for large tables
                    search_interviewer = st.text_input("Search Interviewers:", "")
                    
                    if search_interviewer:
                        filtered_interviewers = interviewer_stats_display[
                            interviewer_stats_display['Interviewer'].str.contains(search_interviewer, case=False, na=False)
                        ]
                    else:
                        filtered_interviewers = interviewer_stats_display
                    
                    # Display all interviewers with pagination or scroll
                    st.dataframe(
                        filtered_interviewers[['Interviewer', 'Total Approaches', 'Successful Surveys', 'Success Rate %', 'Total Refusals', 'Refusal Rate %']],
                        use_container_width=True,
                        hide_index=True,
                        height=400
                    )
                    
                    # Chart: Interviewers with highest refusal rates (still show top for visualization)
                    if not interviewer_stats_display.empty:
                        st.subheader("Top Interviewers by Refusal Rate")
                        # Filter out interviewers with 0 approaches to avoid division errors
                        valid_interviewers = interviewer_stats_display[interviewer_stats_display['Total Approaches'] > 0]
                        if not valid_interviewers.empty:
                            top_refusal_interviewers = valid_interviewers.nlargest(min(10, len(valid_interviewers)), 'Refusal Rate %')
                            
                            if not top_refusal_interviewers.empty:
                                fig_interv = px.bar(top_refusal_interviewers, 
                                                x='Interviewer', y='Refusal Rate %',
                                                title=f"Top {len(top_refusal_interviewers)} Interviewers by Refusal Rate",
                                                hover_data=['Total Refusals', 'Total Approaches'])
                                fig_interv.update_layout(xaxis_tickangle=-45)
                                st.plotly_chart(fig_interv, use_container_width=True)

            with tab3:
                st.subheader("Route Refusal Analysis")
                
                # FIX: Check if required columns exist
                if 'ROUTE_MAIN' not in refusal_df.columns:
                    st.warning("Route data not available.")
                else:
                    # Get ALL routes from the full dataset (including those with 0 refusals)
                    all_routes = refusal_analysis_df['ROUTE_MAIN'].unique()
                    
                    # Route-level refusal statistics
                    route_refusals = refusal_df.groupby('ROUTE_MAIN').size().reset_index(name='Total Refusals')
                    
                    # Calculate success rates for comparison - for ALL routes
                    route_success = refusal_analysis_df.groupby('ROUTE_MAIN').agg({
                        'HAVE_5_MIN_LABEL': lambda x: (x == 'Yes I can participate in the survey (have 5 min+)').sum() if 'HAVE_5_MIN_LABEL' in refusal_analysis_df.columns else 0,
                        'HAVE_5_MIN_FOR_SURVECode': 'count'
                    }).reset_index()
                    
                    route_success.columns = ['ROUTE_MAIN', 'Successful Surveys', 'Total Approaches']
                    route_success['Success Rate %'] = (route_success['Successful Surveys'] / route_success['Total Approaches'] * 100).round(2)
                    
                    # FIX: Use outer merge to include ALL routes, even those with 0 refusals
                    route_stats = pd.merge(route_success, route_refusals, on='ROUTE_MAIN', how='left')
                    
                    # Fill NaN values for routes with 0 refusals
                    route_stats['Total Refusals'] = route_stats['Total Refusals'].fillna(0)
                    route_stats['Refusal Rate %'] = (route_stats['Total Refusals'] / route_stats['Total Approaches'] * 100).round(2)
                    
                    # Rename for display
                    route_stats_display = route_stats.rename(columns={'ROUTE_MAIN': 'Route'})
                    
                    # FIX: Show ALL routes, not just top 10
                    st.write(f"**All Routes Refusal Statistics ({len(route_stats_display)} total)**")
                    
                    # Add search and filter functionality for large tables
                    search_route = st.text_input("Search Routes:", "")
                    
                    if search_route:
                        filtered_routes = route_stats_display[
                            route_stats_display['Route'].astype(str).str.contains(search_route, case=False, na=False)
                        ]
                    else:
                        filtered_routes = route_stats_display
                    
                    # Display all routes with pagination or scroll
                    st.dataframe(
                        filtered_routes[['Route', 'Total Approaches', 'Successful Surveys', 'Success Rate %', 'Total Refusals', 'Refusal Rate %']],
                        use_container_width=True,
                        hide_index=True,
                        height=400
                    )
                    
                    # Chart: Routes with highest refusal rates (still show top for visualization)
                    if not route_stats_display.empty:
                        st.subheader("Top Routes by Refusal Rate")
                        # Filter out routes with 0 approaches to avoid division errors
                        valid_routes = route_stats_display[route_stats_display['Total Approaches'] > 0]
                        if not valid_routes.empty:
                            top_refusal_routes = valid_routes.nlargest(min(10, len(valid_routes)), 'Refusal Rate %')
                            
                            if not top_refusal_routes.empty:
                                fig_routes = px.bar(top_refusal_routes, 
                                                x='Route', y='Refusal Rate %',
                                                title=f"Top {len(top_refusal_routes)} Routes by Refusal Rate",
                                                hover_data=['Total Refusals', 'Total Approaches'])
                                fig_routes.update_layout(xaxis_tickangle=-45)
                                st.plotly_chart(fig_routes, use_container_width=True) 

            with tab4:
                st.subheader("Demographic Analysis of Refusals")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    # Age distribution of refusals - UPDATED to show total vs refusals
                    st.write("**Age Distribution Analysis**")
                    if 'AGE_GROUP_LABEL' in refusal_analysis_df.columns:
                        # Calculate total counts from full dataset
                        age_totals = refusal_analysis_df['AGE_GROUP_LABEL'].value_counts().reset_index()
                        age_totals.columns = ['Age Group', 'Total Count']
                        
                        # Calculate refusal counts
                        age_refusals = refusal_df['AGE_GROUP_LABEL'].value_counts().reset_index()
                        age_refusals.columns = ['Age Group', 'Refusal Count']
                        
                        # Merge total and refusal counts
                        age_analysis = pd.merge(age_totals, age_refusals, on='Age Group', how='left')
                        age_analysis['Refusal Count'] = age_analysis['Refusal Count'].fillna(0)
                        age_analysis['Refusal Rate %'] = (age_analysis['Refusal Count'] / age_analysis['Total Count'] * 100).round(2)
                        age_analysis['Participation Count'] = age_analysis['Total Count'] - age_analysis['Refusal Count']
                        age_analysis['Participation Rate %'] = (age_analysis['Participation Count'] / age_analysis['Total Count'] * 100).round(2)
                        
                        # Display the comprehensive analysis
                        st.dataframe(
                            age_analysis[['Age Group', 'Total Count', 'Participation Count', 'Participation Rate %', 'Refusal Count', 'Refusal Rate %']],
                            use_container_width=True,
                            hide_index=True,
                            height=300
                        )
                        
                        # Chart: Refusal rate by age group
                        if not age_analysis.empty:
                            fig_age = px.bar(age_analysis, x='Age Group', y='Refusal Rate %',
                                        title="Refusal Rate by Age Group",
                                        hover_data=['Total Count', 'Refusal Count'])
                            st.plotly_chart(fig_age, use_container_width=True)
                    else:
                        st.info("Age data not available.")
                
                with col2:
                    # Gender distribution of refusals - UPDATED to show total vs refusals
                    st.write("**Gender Distribution Analysis**")
                    
                    if 'YOUR_GENDERCode' in refusal_analysis_df.columns:
                        # Calculate total counts from full dataset
                        gender_totals = refusal_analysis_df['YOUR_GENDERCode'].value_counts().reset_index()
                        gender_totals.columns = ['Gender Code', 'Total Count']
                        
                        # Calculate refusal counts
                        gender_refusals_counts = refusal_df['YOUR_GENDERCode'].value_counts().reset_index()
                        gender_refusals_counts.columns = ['Gender Code', 'Refusal Count']
                        
                        # Map codes to labels
                        gender_mapping = {
                            '1': 'Male',
                            '2': 'Female', 
                            '3': 'Other'
                        }
                        
                        # Merge total and refusal counts
                        gender_analysis = pd.merge(gender_totals, gender_refusals_counts, on='Gender Code', how='left')
                        gender_analysis['Refusal Count'] = gender_analysis['Refusal Count'].fillna(0)
                        gender_analysis['Gender'] = gender_analysis['Gender Code'].map(gender_mapping)
                        gender_analysis['Gender'] = gender_analysis['Gender'].fillna('Not Specified')
                        gender_analysis['Refusal Rate %'] = (gender_analysis['Refusal Count'] / gender_analysis['Total Count'] * 100).round(2)
                        gender_analysis['Participation Count'] = gender_analysis['Total Count'] - gender_analysis['Refusal Count']
                        gender_analysis['Participation Rate %'] = (gender_analysis['Participation Count'] / gender_analysis['Total Count'] * 100).round(2)
                        
                        # Display the comprehensive analysis
                        st.dataframe(
                            gender_analysis[['Gender', 'Total Count', 'Participation Count', 'Participation Rate %', 'Refusal Count', 'Refusal Rate %']],
                            use_container_width=True,
                            hide_index=True,
                            height=300
                        )
                        
                        # Chart: Refusal rate by gender
                        if not gender_analysis.empty:
                            fig_gender = px.bar(gender_analysis, x='Gender', y='Refusal Rate %',
                                            title="Refusal Rate by Gender",
                                            hover_data=['Total Count', 'Refusal Count'])
                            st.plotly_chart(fig_gender, use_container_width=True)
                    
                    else:
                        st.info("Gender data not available.")
                
                # Language distribution of refusals - UPDATED to show total vs refusals
                st.write("**Language Distribution Analysis**")
                if 'LANGUAGE_LABEL' in refusal_analysis_df.columns:
                    # Calculate total counts from full dataset
                    language_totals = refusal_analysis_df['LANGUAGE_LABEL'].value_counts().reset_index()
                    language_totals.columns = ['Language', 'Total Count']
                    
                    # Calculate refusal counts
                    language_refusals_counts = refusal_df['LANGUAGE_LABEL'].value_counts().reset_index()
                    language_refusals_counts.columns = ['Language', 'Refusal Count']
                    
                    # Merge total and refusal counts
                    language_analysis = pd.merge(language_totals, language_refusals_counts, on='Language', how='left')
                    language_analysis['Refusal Count'] = language_analysis['Refusal Count'].fillna(0)
                    language_analysis['Refusal Rate %'] = (language_analysis['Refusal Count'] / language_analysis['Total Count'] * 100).round(2)
                    language_analysis['Participation Count'] = language_analysis['Total Count'] - language_analysis['Refusal Count']
                    language_analysis['Participation Rate %'] = (language_analysis['Participation Count'] / language_analysis['Total Count'] * 100).round(2)
                    
                    # Display top languages (show all if less than 10, otherwise top 10)
                    display_languages = language_analysis.nlargest(10, 'Total Count') if len(language_analysis) > 10 else language_analysis
                    
                    st.dataframe(
                        display_languages[['Language', 'Total Count', 'Participation Count', 'Participation Rate %', 'Refusal Count', 'Refusal Rate %']],
                        use_container_width=True,
                        hide_index=True,
                        height=300
                    )
                    
                    # Chart: Refusal rate by language (top 10)
                    if not display_languages.empty:
                        fig_language = px.bar(display_languages, x='Language', y='Refusal Rate %',
                                        title="Refusal Rate by Language (Top 10)",
                                        hover_data=['Total Count', 'Refusal Count'])
                        fig_language.update_layout(xaxis_tickangle=-45)
                        st.plotly_chart(fig_language, use_container_width=True)
                else:
                    st.info("Language data not available.")
                
                # Race distribution of refusals - UPDATED to show total vs refusals
                st.write("**Race/Ethnicity Distribution Analysis**")
                if not refusal_race_df.empty and 'RACE_CATEGORY' in refusal_race_df.columns:
                    # For race data, we need to handle it differently since it's in a separate table
                    # Count total race records (assuming refusal_race_df contains all race data)
                    race_totals = refusal_race_df['RACE_CATEGORY'].value_counts().reset_index()
                    race_totals.columns = ['Race/Ethnicity', 'Total Count']
                    
                    # For refusal counts, we need to merge with refusal data
                    if 'RESPONDENT_ID' in refusal_race_df.columns and 'RESPONDENT_ID' in refusal_df.columns:
                        refusal_race_merged = refusal_race_df[refusal_race_df['RESPONDENT_ID'].isin(refusal_df['RESPONDENT_ID'])]
                        race_refusals_counts = refusal_race_merged['RACE_CATEGORY'].value_counts().reset_index()
                        race_refusals_counts.columns = ['Race/Ethnicity', 'Refusal Count']
                        
                        # Merge total and refusal counts
                        race_analysis = pd.merge(race_totals, race_refusals_counts, on='Race/Ethnicity', how='left')
                        race_analysis['Refusal Count'] = race_analysis['Refusal Count'].fillna(0)
                        race_analysis['Refusal Rate %'] = (race_analysis['Refusal Count'] / race_analysis['Total Count'] * 100).round(2)
                        race_analysis['Participation Count'] = race_analysis['Total Count'] - race_analysis['Refusal Count']
                        race_analysis['Participation Rate %'] = (race_analysis['Participation Count'] / race_analysis['Total Count'] * 100).round(2)
                        
                        # Display top races (show all if less than 10, otherwise top 10)
                        display_races = race_analysis.nlargest(10, 'Total Count') if len(race_analysis) > 10 else race_analysis
                        
                        st.dataframe(
                            display_races[['Race/Ethnicity', 'Total Count', 'Participation Count', 'Participation Rate %', 'Refusal Count', 'Refusal Rate %']],
                            use_container_width=True,
                            hide_index=True,
                            height=300
                        )
                        
                        # Chart: Refusal rate by race (top 10)
                        if not display_races.empty:
                            fig_race = px.bar(display_races, x='Race/Ethnicity', y='Refusal Rate %',
                                        title="Refusal Rate by Race/Ethnicity (Top 10)",
                                        hover_data=['Total Count', 'Refusal Count'])
                            fig_race.update_layout(xaxis_tickangle=-45)
                            st.plotly_chart(fig_race, use_container_width=True)
                    else:
                        st.info("Cannot link race data with refusal data - RESPONDENT_ID missing.")
                else:
                    st.info("Race/ethnicity data not available.")
            
            # Summary insights
            st.subheader("📈 Key Insights")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("**Refusal Statistics:**")
                st.write(f"• Total refusals: {total_refusals:,}")
                st.write(f"• Overall refusal rate: {refusal_rate:.1f}%")
                
                if 'interviewer_stats_display' in locals() and not interviewer_stats_display.empty:
                    max_refusal_interviewer = interviewer_stats_display.nlargest(1, 'Refusal Rate %')
                    if not max_refusal_interviewer.empty:
                        st.write(f"• Highest refusal rate interviewer: {max_refusal_interviewer.iloc[0]['Interviewer']} ({max_refusal_interviewer.iloc[0]['Refusal Rate %']}%)")
                
                if 'route_stats_display' in locals() and not route_stats_display.empty:
                    max_refusal_route = route_stats_display.nlargest(1, 'Refusal Rate %')
                    if not max_refusal_route.empty:
                        st.write(f"• Highest refusal rate route: {max_refusal_route.iloc[0]['Route']} ({max_refusal_route.iloc[0]['Refusal Rate %']}%)")
            
            with col2:
                st.write("**Recommendations:**")
                st.write("• Focus training on interviewers with high refusal rates")
                st.write("• Investigate routes with consistently high refusal rates")
                st.write("• Consider language assistance for non-English speakers")
                st.write("• Review approach techniques in high-refusal demographic groups")
            
            # Navigation
            if st.button("🔙 Home Page"):
                st.query_params["page"] = "main"
                st.rerun()

        def location_maps_page():
            """
            Display the location maps interface integrated with existing filter structure
            """
            st.title("🗺️ Location Maps")
            
            try:
                # Load the Elvis data for mapping
                from automated_refresh_flow_new import PROJECTS, fetch_data
                from automated_sync_flow_constants_maps import KCATA_HEADER_MAPPING
                
                project_config = PROJECTS[st.session_state["selected_project"]]
                elvis_config = project_config['databases']["elvis"]
                table_name = elvis_config['table']
                database_name = elvis_config["database"]
                
                with st.spinner("🔄 Loading location data..."):
                    # Fetch the Elvis data
                    csv_buffer = fetch_data(database_name, table_name)
                    
                    if csv_buffer:
                        # Convert CSV buffer to DataFrame
                        csv_buffer.seek(0)  # Reset buffer position
                        elvis_df = pd.read_csv(csv_buffer, low_memory=False)
                        elvis_df = elvis_df.drop(index=0).reset_index(drop=True)
                        
                        # Apply column renaming
                        try:
                            elvis_df.columns = elvis_df.columns.str.strip()
                            # Apply the header mapping
                            elvis_df = elvis_df.rename(columns=KCATA_HEADER_MAPPING)
                            st.success("✅ Data loaded and columns renamed successfully!")
                        except Exception as e:
                            st.warning(f"Column renaming failed: {str(e)}. Using original column names.")
                        
                        # Prepare location data using the utility function
                        from automated_sync_flow_utils import prepare_location_data
                        location_df, unique_routes = prepare_location_data(elvis_df)
                        
                        if location_df.empty:
                            st.warning("No location data available after filtering.")
                            if st.button("🔙 Home Page", key="location_maps_empty_home"):
                                st.query_params["page"] = "main"
                                st.rerun()
                            return
                        
                        # Apply search filter (consistent with other pages)
                        filtered_locations = filter_dataframe(location_df, search_query)
                        
                        # Display statistics (consistent with other pages)
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            st.metric("📍 Total Points", len(filtered_locations))
                        with col2:
                            st.metric("🛣️ Unique Routes", filtered_locations['route_code'].nunique())
                        with col3:
                            st.metric("📊 Location Types", filtered_locations['location_type'].nunique())
                        with col4:
                            st.metric("👥 Survey Records", filtered_locations['id'].nunique())
                        
                        # Initialize session state for filters
                        if 'location_routes' not in st.session_state:
                            st.session_state.location_routes = []
                        if 'location_types' not in st.session_state:
                            st.session_state.location_types = sorted(location_df['location_type'].unique().tolist())
                        
                        # Create filters section (similar to reverse_routes_page structure)
                        st.subheader("📍 Map Filters")
                        
                        # Initialize filter variables
                        route_options = sorted(unique_routes['ROUTE_SURVEYEDCode'].unique().tolist())
                        location_type_options = sorted(location_df['location_type'].unique().tolist())
                        
                        # Create filter columns (similar to reverse_routes_page)
                        col1, col2, col3 = st.columns(3)
                        
                        with col1:
                            # Route filter - MULTI-SELECT
                            selected_routes = st.multiselect(
                                "Select Routes:", 
                                options=route_options,
                                default=st.session_state.location_routes,
                                key="location_routes_multiselect"
                            )
                        
                        with col2:
                            # Location type filter - MULTI-SELECT
                            selected_location_types = st.multiselect(
                                "Select Location Types:", 
                                options=location_type_options,
                                default=st.session_state.location_types,
                                key="location_types_multiselect"
                            )
                        
                        with col3:
                            # Quick actions - Use session state to manage filter state
                            st.write("**Quick Actions**")
                            col3a, col3b = st.columns(2)
                            with col3a:
                                if st.button("Select All Routes", key="location_select_all_routes"):
                                    st.session_state.location_routes = route_options
                                    st.rerun()
                            with col3b:
                                if st.button("Clear Routes", key="location_clear_routes"):
                                    st.session_state.location_routes = []
                                    st.rerun()
                        
                        # Additional quick actions for location types
                        col4, col5 = st.columns(2)
                        with col4:
                            if st.button("Select All Types", key="location_select_all_types"):
                                st.session_state.location_types = location_type_options
                                st.rerun()
                        with col5:
                            if st.button("Clear Types", key="location_clear_types"):
                                st.session_state.location_types = []
                                st.rerun()
                        
                        # Quick filter buttons for common scenarios
                        st.write("**Quick Filters**")
                        quick_col1, quick_col2, quick_col3 = st.columns(3)
                        with quick_col1:
                            if st.button("Just Alighting", key="just_alighting_btn"):
                                st.session_state.location_types = ['Alighting']
                                st.session_state.location_routes = []  # Show for all routes
                                st.rerun()
                        with quick_col2:
                            if st.button("Just Origin", key="just_origin_btn"):
                                st.session_state.location_types = ['Origin']
                                st.session_state.location_routes = []  # Show for all routes
                                st.rerun()
                        with quick_col3:
                            if st.button("Show All", key="show_all_btn"):
                                st.session_state.location_routes = []
                                st.session_state.location_types = location_type_options
                                st.rerun()
                        
                        # Update session state with current selections (for persistence)
                        st.session_state.location_routes = selected_routes
                        st.session_state.location_types = selected_location_types
                        
                        # Apply route and location type filters
                        temp_filtered = filtered_locations.copy()
                        
                        if selected_routes:
                            temp_filtered = temp_filtered[temp_filtered['route_code'].isin(selected_routes)]
                        
                        if selected_location_types:
                            temp_filtered = temp_filtered[temp_filtered['location_type'].isin(selected_location_types)]
                        
                        # Show filter summary
                        filter_info = []
                        if selected_routes:
                            if len(selected_routes) <= 3:
                                filter_info.append(f"Routes: {', '.join(selected_routes)}")
                            else:
                                filter_info.append(f"Routes: {len(selected_routes)} selected")
                        if selected_location_types:
                            if len(selected_location_types) <= 3:
                                filter_info.append(f"Types: {', '.join(selected_location_types)}")
                            else:
                                filter_info.append(f"Types: {len(selected_location_types)} selected")
                        
                        if filter_info:
                            st.info(f"**Active Filters:** {', '.join(filter_info)}")
                        else:
                            st.info("**Showing all routes and location types**")
                        
                        # Display the data table (consistent with other pages)
                        st.subheader("📍 Location Data")
                        display_columns = ['route_code', 'route_name', 'location_type', 'latitude', 'longitude', 'address', 'city']
                        st.dataframe(temp_filtered[display_columns], use_container_width=True, hide_index=True)
                        
                        # Create the map
                        st.subheader("🗺️ Interactive Map")
                        
                        if not temp_filtered.empty:
                            # Convert coordinates to numeric
                            temp_filtered['latitude'] = pd.to_numeric(temp_filtered['latitude'], errors='coerce')
                            temp_filtered['longitude'] = pd.to_numeric(temp_filtered['longitude'], errors='coerce')
                            
                            # Remove invalid coordinates
                            map_data = temp_filtered.dropna(subset=['latitude', 'longitude'])
                            
                            if not map_data.empty:
                                # Color mapping
                                color_map = {
                                    'Home': '#1f77b4',      # blue
                                    'Origin': '#2ca02c',    # green
                                    'Boarding': '#ff7f0e',  # orange
                                    'Alighting': '#d62728', # red
                                    'Destination': '#9467bd' # purple
                                }
                                
                                # Add color column
                                map_data['color_hex'] = map_data['location_type'].map(color_map)
                                
                                # Display the map
                                try:
                                    st.map(
                                        map_data,
                                        latitude='latitude',
                                        longitude='longitude',
                                        color='color_hex',
                                        size=100,
                                        use_container_width=True
                                    )
                                except Exception as e:
                                    st.warning(f"Map rendering issue: {str(e)}")
                                    # Fallback
                                    st.map(
                                        map_data,
                                        latitude='latitude',
                                        longitude='longitude',
                                        size=100,
                                        use_container_width=True
                                    )
                                
                                # Legend
                                st.sidebar.subheader("🎨 Map Legend")
                                for loc_type, color in color_map.items():
                                    if loc_type in map_data['location_type'].unique():
                                        st.sidebar.markdown(f"<span style='color:{color}'>■</span> {loc_type}", unsafe_allow_html=True)
                                
                                # Show map statistics
                                st.sidebar.subheader("📊 Map Stats")
                                st.sidebar.metric("Points on Map", len(map_data))
                                st.sidebar.metric("Routes on Map", map_data['route_code'].nunique())
                                
                            else:
                                st.warning("No valid coordinates to display on map.")
                        else:
                            st.warning("No data available for the selected filters.")
                        
                        # Download option (consistent with other pages)
                        st.subheader("📥 Data Export")
                        if st.button("Download Location Data as CSV", key="location_download_btn"):
                            csv_data, file_name = create_csv(temp_filtered, "location_data.csv")
                            download_csv(csv_data, file_name, "Download Location Data")
                        
                    else:
                        st.error("❌ Failed to fetch location data from Elvis table")
                        
            except Exception as e:
                st.error(f"❌ Error loading location data: {str(e)}")
                st.info("Please ensure the Elvis table is available and accessible.")
                
                # Show debug information
                with st.expander("🔧 Technical Details"):
                    st.write("Error details:", str(e))
                    import traceback
                    st.code(traceback.format_exc())

            # Navigation (consistent with other pages)
            if st.button("🔙 Home Page", key="location_maps_home_btn"):
                st.query_params["page"] = "main"
                st.rerun()


        # Layout columns
        header_col1, header_col2, header_col3 = st.columns([2, 2, 1])

        # Header Section
        with header_col1:
            # st.header('Completion Report')
            # Button to trigger the entire script
        
            # if st.session_state['user']["role"].lower()=='admin':
            if st.button("Sync"):
                with st.spinner("Data engines are spinning up ⚙️📡 … syncing will be wrapped in 2–3 mins!"):
                    result = fetch_and_process_data(st.session_state["selected_project"],st.session_state["schema"])
                    if "cache_key" not in st.session_state:
                        st.session_state["cache_key"] = 0
                    st.session_state["cache_key"] += 1                
                    # Fetch and process data again
                    dataframes = fetch_dataframes_from_snowflake(st.session_state["cache_key"])
                    print("Data fetched successfully")  # Debug statement
                    
                    # Example: Access DataFrames
                    wkday_df = dataframes.get('wkday_df', pd.DataFrame())
                    wkday_dir_df = dataframes.get('wkday_dir_df', pd.DataFrame())
                    wkend_df = dataframes.get('wkend_df', pd.DataFrame())
                    wkend_dir_df = dataframes.get('wkend_dir_df', pd.DataFrame())
                    wkend_time_df = dataframes.get('wkend_time_df', pd.DataFrame())
                    wkday_time_df = dataframes.get('wkday_time_df', pd.DataFrame())
                    wkend_raw_df = dataframes.get('wkend_raw_df', pd.DataFrame())
                    wkday_raw_df = dataframes.get('wkday_raw_df', pd.DataFrame())
                    detail_df = dataframes.get('detail_df', pd.DataFrame())
                    wkday_stationwise_df = dataframes.get('wkday_stationwise_df', pd.DataFrame())
                    wkend_stationwise_df = dataframes.get('wkend_stationwise_df', pd.DataFrame())
                    surveyor_report_trends_df = dataframes.get('surveyor_report_trends_df', pd.DataFrame())
                    route_report_trends_df = dataframes.get('route_report_trends_df', pd.DataFrame())
                    surveyor_report_date_trends_df = dataframes.get('surveyor_report_date_trends_df', pd.DataFrame())
                    route_report_date_trends_df = dataframes.get('route_report_date_trends_df', pd.DataFrame())
                    low_response_questions_df = dataframes.get('low_response_questions_df', pd.DataFrame())
                st.success(f"Data synced successfully 🎉 … pipelines are tidy, tables are aligned, and we’re good to go ✅📂")

            # if current_page != 'timedetails':
            #     if current_page == "weekend":
            #         st.header(f'Total Records: {wkend_df["# of Surveys"].sum()}')
            #     else:  # Default to weekday data for main and weekday pages
            #         st.header(f'Total Records: {wkday_df["# of Surveys"].sum()}')
            # current_date = datetime.datetime.now()
            # formatted_date = current_date.strftime("%Y-%m-%d %H:%M:%S")
            # st.markdown(f"##### **Last Refresh DATE**: {formatted_date}")

            # Get the most recent "Completed" date from both wkday_raw_df and wkend_raw_df
            # if 'kcata' in selected_project or 'kcata_rail' in selected_project or 'actransit' in selected_project:
            #     completed_dates = pd.concat([wkday_raw_df['DATE_SUBMITTED'], wkend_raw_df['DATE_SUBMITTED']])
            # else:
            #     completed_dates = pd.concat([wkday_raw_df['Completed'], wkend_raw_df['Completed']])
            # most_recent_completed_date = pd.to_datetime(completed_dates).max()

            # # # # Display the most recent "Completed" date
            # st.markdown(f"##### **Completed**: {most_recent_completed_date.strftime('%Y-%m-%d %H:%M:%S')}")

            # ADD THE ELVIS EXPORT BUTTON RIGHT HERE - BELOW SYNC BUTTON

        # Page Content Section
        # with header_col2:
            

                # Button for Time OF Day Details
                # if st.button('Time OF Day Details'):
                #     st.query_params["page"] = "timedetails"
                #     st.rerun()
                    # st.markdown(f'<meta http-equiv="refresh" content="0;url=/?page=timedetails">', unsafe_allow_html=True)
            # else:
            #     st.header(f'Time OF Day Details')

            # if 'actransit' in selected_project:
            #     if st.button("LOW RESPONSE QUESTIONS"):
            #         st.query_params["page"] = "low_response_questions_tab"
            #         st.rerun()

        # Button Section
        with header_col2:
            if st.button("📊 Export Elvis Data"):
                export_elvis_data()
            # if current_page != 'timedetails':
            #     if current_page == "weekend":
            #         csv_weekend_raw, week_end_raw_file_name = create_csv(wkend_raw_df, "wkend_raw_data.csv")
            #         download_csv(csv_weekend_raw, week_end_raw_file_name, "Download WeekEnd Raw Data")
            #     else:  # Default to weekday data for main and weekday pages
            #         st.header(f'Total Records: {wkday_df["# of Surveys"].sum()}')
            #         csv_weekday_raw, week_day_raw_file_name = create_csv(wkday_raw_df, "wkday_raw_data.csv")
            #         download_csv(csv_weekday_raw, week_day_raw_file_name, "Download WeekDay Raw Data")
            # WEEKDAY-OVERALL button
            # if st.button("WEEKDAY-OVERALL"):
            #     st.query_params["page"] = "weekday"
            #     st.rerun()
                # st.markdown(f'<meta http-equiv="refresh" content="0;url=/?page=weekday">', unsafe_allow_html=True)

            # WEEKEND-OVERALL button
            # if st.button("WEEKEND-OVERALL"):
            #     st.query_params["page"] = "weekend"
            #     st.rerun()
                # st.markdown(f'<meta http-equiv="refresh" content="0;url=/?page=weekend">', unsafe_allow_html=True)
            # if 'actransit' in selected_project:
            #     if st.button("REFUSAL ANALYSIS"):
            #         st.query_params["page"] = "refusal"
            #         st.rerun()

            
            # # Add these two new buttons for kcata simple project
            # if 'kcata' in selected_project or 'actransit' in selected_project and 'rail' not in selected_schema.lower():
            #     # if st.button("Route Comparison"):
            #     #     st.query_params["page"] = "route_comparison"
            #     #     st.rerun()
                    
            #     if st.button("Reverse Routes"):
            #         st.query_params["page"] = "reverse_routes"
            #         st.rerun()

            # if 'stl' in selected_project or 'kcata' in selected_project or 'actransit' in selected_project:
            #     if st.button("DAILY TOTALS"):
            #         st.query_params["page"] = "dailytotals"
            #         st.rerun()
            
            #     if st.button("Surveyor/Route/Trend Reports"):
            #         st.query_params["page"] = "surveyreport"
            #         st.rerun()

            # if 'rail' in selected_schema.lower():
            #     if st.button("WEEKDAY StationWise Comparison"):
            #         st.query_params["page"] = "weekday_station"
            #         st.rerun()
            #         # st.markdown(f'<meta http-equiv="refresh" content="0;url=/?page=weekday_station">', unsafe_allow_html=True)

            #     if st.button("WEEKEND StationWise Comparison"):
            #         st.query_params["page"] = "weekend_station"
            #         st.rerun()
                    # st.markdown(f'<meta http-equiv="refresh" content="0;url=/?page=weekend_station">', unsafe_allow_html=True)
            with header_col3:
                if current_page == "weekend":
                    csv_weekend_raw, week_end_raw_file_name = create_csv(wkend_raw_df, "wkend_raw_data.csv")
                    st.download_button(
                        "⬇ Download Weekend Raw Data",
                        data=csv_weekend_raw,
                        file_name=week_end_raw_file_name,
                        mime="text/csv",
                        key="download_weekend"
                    )
                else:
                    csv_weekday_raw, week_day_raw_file_name = create_csv(wkday_raw_df, "wkday_raw_data.csv")
                    st.download_button(
                        "⬇ Download Weekday Raw Data",
                        data=csv_weekday_raw,
                        file_name=week_day_raw_file_name,
                        mime="text/csv",
                        key="download_weekday"
                    )

            st.markdown('</div>', unsafe_allow_html=True)
        # === End of Unified Button Row ===





            
        if 'rail' in selected_schema.lower():
        
            if current_page == "weekday":
                weekday_page()

            elif current_page == "weekend":
                weekend_page()
            elif current_page=='timedetails':
                time_details(detail_df)
            elif current_page=='weekday_station':
                weekday_station_page()
            elif current_page=='weekend_station':
                weekend_station_page()
            elif current_page == "location_maps":  # Add this line
                location_maps_page()
            else:
                # wkday_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', '(1) Collect', '(1) Remain',
                #                         '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain', '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal']
                if 'uta' in selected_project:
                    wkday_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED','STATION_ID',  '(0) Collect', '(0) Remain','(1) Collect', '(1) Remain',
                                            '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain','(5) Collect', '(5) Remain',
                                            '(0) Goal','(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal','(5) Goal']
                    wkday_time_columns=['Display_Text', 'Original Text', 'Time Range', '1', '2', '3', '4','5']
                elif 'tucson' in selected_project:
                    wkday_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', '(1) Collect', '(1) Remain',
                                            '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain',
                                            '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal']
                    wkday_time_columns=['Display_Text', 'Original Text', 'Time Range', '1', '2', '3', '4']
                
                elif 'stl' in selected_project:
                    wkday_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', '(1) Collect', '(1) Remain',
                                            '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain',
                                            '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal']
                    wkday_time_columns=['Display_Text', 'Original Text', 'Time Range', '1', '2', '3', '4']

                elif 'kcata' in selected_project:
                    wkday_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', '(1) Collect', '(1) Remain',
                                            '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain',
                                            '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal']
                    wkday_time_columns=['Display_Text', 'Original Text', 'Time Range', '1', '2', '3', '4']

                else:
                    wkday_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', '(0) Collect', '(0) Remain','(1) Collect', '(1) Remain',
                                            '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain','(5) Collect', '(5) Remain',
                                            '(0) Goal','(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal','(5) Goal']
                    wkday_time_columns=['Display_Text', 'Original Text', 'Time Range', '1', '2', '3', '4','5']
            


                main_page(wkday_dir_df[wkday_dir_columns],
                                wkday_time_df[wkday_time_columns],
                                wkday_df[['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', 'Route Level Goal', '# of Surveys', 'Remaining']])
        else:
            if current_page == "weekday":
                weekday_page()
            elif current_page == "weekend":
                weekend_page()
            elif current_page=='timedetails':
                time_details(detail_df)
            elif current_page == "dailytotals":
                if 'stl' in selected_project or 'kcata' in selected_project or 'actransit' in selected_project:
                    daily_totals_page()
            elif current_page == "low_response_questions_tab":
                if 'actransit' in selected_project:  # Add this new route
                    low_response_questions_page()
            elif current_page == "refusal":  # ADD THIS NEW PAGE FOR REFUSAL ANALYSIS
                if 'actransit' in selected_project:
                    show_refusal_analysis(refusal_analysis_df, refusal_race_df)
            elif current_page == "surveyreport":
                if 'stl' in selected_project or 'kcata' in selected_project or 'actransit' in selected_project:
                    # 📌 Fields you want to show
                    percentage_fields = [
                        "% of Incomplete Home Address", "% of 0 Transfers",
                        "% of Access Walk", "% of Egress Walk",
                        "% of LowIncome", "% of No Income",
                        "% of Hispanic", "% of Black", "% of White",
                        "% of Follow-Up Survey", "% of Contest - Yes"
                    ]

                    time_fields = [
                        "SurveyTime (All)", "SurveyTime (TripLogic)", "SurveyTime (DemoLogic)"
                    ]

                    count_fields = [
                        "# of Records", "# of Supervisor Delete", "# of Records Remove",
                        "# of Records Reviewed", "# of Records Not Reviewed"
                    ]

                    # 📌 Columns to exclude
                    excluded_columns = [
                        "INTERV_INIT",
                        "Route",
                        "# of Records",
                        "# of Supervisor Delete",
                        "# of Records Remove",
                        "# of Records Reviewed",
                        "# of Records Not Reviewed",
                        "% of LowIncome",
                        "% of Contest - Yes",
                        "% of Follow-Up Survey",
                        "% of Contest - (Yes & Good Info)/Overall # of Records"
                    ]

                    
                    def render_metrics(row, title):
                        st.markdown(f"### {title}")

                        # ---- Force black text no matter what ----
                        text_color = "#000000"

                        filtered_items = [(k, v) for k, v in row.items() if k not in excluded_columns]

                        for i in range(0, len(filtered_items), 4):
                            cols = st.columns(min(4, len(filtered_items) - i))
                            for col, (field, value) in zip(cols, filtered_items[i:i+4]):
                                with col:
                                    st.markdown(
                                        f"""
                                        <div style="padding:2px 0; margin-bottom:4px;">
                                            <span style="font-size:0.65rem; font-weight:600; color:{text_color} !important;">{field}</span><br>
                                            <span style="font-size:0.8rem; color:{text_color} !important;">{value}</span>
                                        </div>
                                        """,
                                        unsafe_allow_html=True
                                    )



                    # 📌 Layout: add spacer
                    col1, _, col2 = st.columns([1, 0.1, 1])

                    surveyor_last_row = surveyor_report_trends_df.iloc[-1].to_dict()
                    route_last_row = route_report_trends_df.iloc[-1].to_dict()




                    col1, col2 = st.columns(2)

                    with col1:
                        # print("Columns in surveyor_report_trends_df:", dataframes['surveyor_report_trends_df'].columns.tolist())
                        # print("Columns in surveyor_report_date_trends_df:", dataframes['surveyor_report_date_trends_df'].columns.tolist())

                        render_metrics(surveyor_last_row, "TRIP LOGIC & QAQC REPORT - SURVEYOR REPORT")
                        filter_col = "Date_Surveyor" if 'stl' in selected_project else "Date"
                        display_filtered_or_unfiltered_report(
                            unfiltered_df=dataframes['surveyor_report_trends_df'],
                            filtered_df=dataframes['surveyor_report_date_trends_df'],
                            filter_column_name=filter_col,
                            display_column_name="INTERV_INIT",
                            section_title="Surveyor Report",
                            date_label="Surveyor"
                        )

                    with col2:
                        render_metrics(route_last_row, "TRIP LOGIC & QAQC REPORT - ROUTE REPORT")
                        display_filtered_or_unfiltered_report(
                            unfiltered_df=dataframes['route_report_trends_df'],
                            filtered_df=dataframes['route_report_date_trends_df'],
                            filter_column_name="Date_Route",
                            display_column_name="ROUTE",
                            section_title="Route Report",
                            date_label="Route"
                        )

            elif current_page == "route_comparison":
                if 'kcata' in selected_project or 'actransit' in selected_project and 'rail' not in selected_schema.lower():
                    route_comparison_page()
            elif current_page == "reverse_routes":
                if 'kcata' in selected_project or 'actransit' in selected_project and 'rail' not in selected_schema.lower():
                    reverse_routes_page()
            elif current_page == "location_maps":  # Add this line
                location_maps_page()

            else:
                if 'tucson' in selected_project:
                    wkday_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', '(1) Collect', '(1) Remain',
                                            '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain',
                                            '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal']
                    wkday_time_columns=['Display_Text', 'Original Text', 'Time Range', '1', '2', '3', '4']
                elif 'stl' in selected_project:
                    wkday_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', '(1) Collect', '(1) Remain',
                                            '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain',
                                            '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal']
                    wkday_time_columns=['Display_Text', 'Original Text', 'Time Range', '1', '2', '3', '4']

                elif 'kcata' in selected_project or 'actransit' in selected_project:
                    wkday_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', '(1) Collect', '(1) Remain',
                                            '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain', '(5) Collect', '(5) Remain',
                                            '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal', '(5) Goal']
                    wkday_time_columns=['Display_Text', 'Original Text', 'Time Range', '1', '2', '3', '4', '5']

                else:
                    wkday_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', '(0) Collect', '(0) Remain','(1) Collect', '(1) Remain',
                                            '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain','(5) Collect', '(5) Remain',
                                            '(0) Goal','(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal','(5) Goal']
                    wkday_time_columns=['Display_Text', 'Original Text', 'Time Range', '1', '2', '3', '4','5']
                
                try:
                    main_page(wkday_dir_df[wkday_dir_columns],
                            wkday_time_df[wkday_time_columns],
                            wkday_df[['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', 'Route Level Goal', '# of Surveys', 'Remaining']])
                    
                except KeyError as e:
                    st.error(f"⚠️ Missing columns in data: {e}")
                    st.error("Available columns in weekday direction data:")
                    st.write(wkday_dir_df.columns.tolist())
                    st.stop()  # Prevent further execution