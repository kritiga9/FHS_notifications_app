import streamlit as st
import pandas as pd
from kbcstorage.client import Client
from datetime import datetime

# Set up Streamlit page
st.set_page_config(layout="wide")

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



df = read_df('in.c-notifications.notifications_full')
component_config = read_df('in.c-keboola-ex-telemetry-data-8947841.kbc_component_configuration')
component_config = component_config[(component_config["kbc_component"]=='Orchestrator') &  (component_config["branch_type"]=='default') &  (component_config["kbc_configuration_is_deleted"]==False)]
jobs = read_df('in.c-keboola-ex-telemetry-data-8947841.kbc_job')

jobs = jobs[jobs["kbc_component_configuration_id"].isin(component_config["kbc_component_configuration_id"])]
jobs["job_created_at"] = pd.to_datetime(jobs["job_created_at"])

# Group by 'kbc_component_configuration_id' and get the row with max 'job_created_at'
result = jobs.loc[jobs.groupby(['kbc_component_configuration_id'])['job_created_at'].idxmax()]

# Calculate the difference between the current date and 'job_created_at'
result['days_since_job_created'] = (datetime.now() - result['job_created_at']).dt.days

# Create a new column to flag whether the job is active or inactive based on the 30-day threshold
result['status'] = result['days_since_job_created'].apply(lambda x: 'inactive' if x > 30 else 'active')


# Reset index if needed
result = result.reset_index(drop=True)
configs = component_config.merge(result[["status","kbc_component_configuration_id","job_created_at","job_status"]],how="left",on="kbc_component_configuration_id")
configs["status"].fillna("inactive",inplace=True)

# Streamlit app
st.title("Operational Dashboard")

# Top filters in columns
st.subheader("Filters")
col1, col2 = st.columns(2)

with col1:
    project_names = configs["kbc_project_id"].unique()
    selected_project = st.multiselect("Select Project", project_names, project_names)

with col2:
    flow_status = configs["status"].unique()
    selected_status = st.multiselect("Select Status", flow_status, flow_status)    


# Filter dataframe based on selections
filtered_df = configs[
    (configs["kbc_project_id"].isin(selected_project)) &
    (configs["status"].isin(selected_status)) 
]

# Display the filtered data
st.write("## Filtered Data")
filtered_df[["job-failed,job-succeeded","job-warning","job-processing-long"]] = ''
edited_data = st.data_editor(filtered_df[["kbc_project_id","kbc_component_configuration","status","job_created_at","job_status","job-failed,job-succeeded","job-warning","job-processing-long"]])

st.write(edited_data)

#need a button that will update the notification

#need to figure out the logic for setting up notification