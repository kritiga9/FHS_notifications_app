import streamlit as st
import pandas as pd
from kbcstorage.client import Client


def run():  
    # Set up Keboola client
    KEBOOLA_STACK = st.secrets["kbc_url"]
    KEBOOLA_TOKEN = st.secrets["kbc_token"]
    keboola_client = Client(KEBOOLA_STACK, KEBOOLA_TOKEN)
    # Function to read DataFrame from Keboola
    @st.cache_data
    def read_df(table_id, filter_col_name=None, filter_col_value=None, index_col=None, date_col=None, dtype=None):
        keboola_client.tables.export_to_file(table_id, '.')
        table_name = table_id.split(".")[-1]
        df = pd.read_csv(table_name, index_col=index_col, parse_dates=date_col, dtype=dtype, keep_default_na=False, na_values=[''])
        if filter_col_name:
            return df.loc[df[filter_col_name] == filter_col_value]
        else:
            return df



    flows = read_df('out.c-notifications.flow_jobs')
    flows['job_created_at'] = pd.to_datetime(flows['job_created_at'])

    # Streamlit app
    st.title("Operational Dashboard")


    # Top filters in columns
    st.subheader("Filters")
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        project_names = flows["project_name"].unique()
        with st.expander("Select Project"):
            selected_project = st.multiselect("Select Project", project_names, project_names, label_visibility="collapsed")

    with col2:
        job_status = flows["job_status"].unique()
        with st.expander("Select Status"):
            selected_job = st.multiselect("Select Status", job_status, job_status, label_visibility="collapsed")    

    with col3:
        flow_status = flows["component_name"].unique()
        with st.expander("Select Flow"):
            selected_status = st.multiselect("Select Flow", flow_status, flow_status, label_visibility="collapsed")    

    with col4:
        start_date = st.date_input("Start Date", value=flows['job_created_at'].min().date())
        end_date = st.date_input("End Date", value=flows['job_created_at'].max().date())


    # Filter dataframe based on selections
    filtered_df = flows[
        (flows["project_name"].isin(selected_project)) &
        (flows["job_status"].isin(selected_job)) &
        (flows["component_name"].isin(selected_status)) &
        (flows["job_created_at"].dt.date >= start_date) &
        (flows["job_created_at"].dt.date <= end_date)
    ]
    filtered_df = filtered_df[["project_name","component_name","job_run_id","job_status","job_created_at", "link"]]

    # Kritiga, below is a commented code to turn links into clickable text, 
    # but it won't work with pretty st.dataframe


    # Display the filtered data
    st.write("## Filtered Data")
    # filtered_df["configuration_id_num"] = filtered_df["configuration_id_num"].astype(int)
    #filtered_df = filtered_df.merge(pivot_df,how="left",left_on="configuration_id_num",right_on="job_configuration_id")

    #edited_data = st.data_editor(filtered_df[["project_name","component_name","job_run_id","job_status","job_created_at"]])

    # Function to apply conditional formatting
    def highlight_status(val):
        color = 'black'
        if val == 'success':
            color = 'green'
        elif val == 'warning':
            color = 'orange'
        elif val == 'error':
            color = 'red'
        return f'color: {color}'

    # Apply the style
    styled_df = filtered_df.style.applymap(highlight_status, subset=['job_status'])

    # Display the styled DataFrame in Streamlit
    #need to figure out how to display the name of the flow as display_text
    st.dataframe(styled_df, column_config={"link":st.column_config.LinkColumn()},hide_index=True)