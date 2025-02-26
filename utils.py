import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, ColumnsAutoSizeMode,JsCode
from st_aggrid.grid_options_builder import GridOptionsBuilder
from streamlit.runtime.scriptrunner import get_script_run_ctx


column_name_patterns=['(0) Remain', '(1) Remain', '(2) Remain', 
    '(3) Remain', '(4) Remain', '(5) Remain' ,'Remaining']

def render_aggrid(dataframe, height, pinned_column,key):
    """
    Render an AgGrid with the specified dataframe, height, and pinned column.
    
    Args:
        dataframe (pd.DataFrame): The dataframe to display in AgGrid.
        height (int): The height of the grid in pixels.
        pinned_column (str): The column to pin to the left.

    Returns:
        None: Displays the AgGrid.
    """
    # JavaScript code for cell styling
    cellStyle = JsCode("""
        function(params) {
            const val = params.value;

            if (val >= -10000 && val < 1) {
                return {'background-color': '#BCE29E', 'color': 'black'};
            } else if (val >= 1 && val < 6) {
                return {'background-color': '#E5EBB2', 'color': 'black'};
            } else if (val >= 6 && val < 35) {
                return {'background-color': '#F8C4B4', 'color': 'black'};
            } else if (val >= 35 && val < 10000) {
                return {'background-color': '#FF8787', 'color': 'black'};
            }
            return null;  // Default style
        }
    """)

    # Create GridOptionsBuilder
    gb = GridOptionsBuilder.from_dataframe(dataframe)
    gb.configure_default_column(editable=False, groupable=False, autoSizeColumns=True)

    # Pin the specified column
    if pinned_column in dataframe.columns:
        gb.configure_column(pinned_column, pinned='left')
    else:
        print(f"Column '{pinned_column}' not found in the dataframe")

    # Apply cell style to target columns
    valid_columns = [col for col in dataframe.columns if any(pattern in col for pattern in column_name_patterns)]
    for column in valid_columns:
        gb.configure_column(column, cellStyle=cellStyle)

    other_options = {'suppressColumnVirtualisation': True}

    # Build grid options
    gb.configure_grid_options(
        alwaysShowHorizontalScroll=True,
        enableRangeSelection=True,
        pagination=True,
        paginationPageSize=10000,
        domLayout='normal',
        **other_options
    )

    grid_options = gb.build()

    grid_options["autoSizeAllColumns"] = True
    
    # Render AgGrid
    AgGrid(
        dataframe,
        gridOptions=grid_options,
        height=height,
        theme="streamlit",  # Choose theme: 'streamlit', 'light', 'dark', etc.
        allow_unsafe_jscode=True,  # Required to enable custom JsCode
        key=f'grid_{key}',  # Unique key for the grid instance
        suppressHorizontalScroll=False,
        fit_columns_on_grid_load=False,
        columns_auto_size_mode=ColumnsAutoSizeMode.FIT_CONTENTS,
        reload_data=True,  # Allow horizontal scrolling
        width='100%',  # Ensure the grid takes full width
    )



def create_csv(df, file_name):
    """
    Convert the DataFrame to CSV and return it for downloading.
    Append (weekend) or (weekday) to the file name based on page_type.
    """
    # Add page type to the file name
    # suffix = "(weekend)" if page_type == "weekend" else "(weekday)"
    file_name = f"{file_name}.csv"
    
    # Convert DataFrame to CSV
    csv = df.to_csv(index=False)
    return csv, file_name

def download_csv(csv, file_name, label):
    """
    Create a Streamlit download button for a given CSV.
    """
    with st.empty():  # Using st.container() to wrap the button
        st.download_button(
            label=label,
            data=csv,
            file_name=file_name,
            mime="text/csv"
        )



def update_query_params(new_page):
    ctx = get_script_run_ctx()
    if ctx:
        st.query_params["page"] = new_page
        st.experimental_rerun()