import streamlit as st
import pandas as pd
from kbcstorage.client import Client
from datetime import datetime
import requests

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

def send_notification(event, job_component_id, job_configuration_id, email_address):
    url = "https://notification.north-europe.azure.keboola.com/project-subscriptions"
    
    payload = {
        "event": event,
        "filters": [
            {
                "field": "job.component.id",
                "value": "keboola.orchestrator"
            },
            {
                "field": "job.configuration.id",
                "value": job_configuration_id
            }
        ],
        "recipient": {
            "channel": "email",
            "address": email_address
        }
    }
    
    headers = {
        'X-StorageApi-Token': KEBOOLA_TOKEN,
        'Content-Type': 'application/json'
    }
    
    response = requests.post(url, headers=headers, json=payload)
    
    return response.text


#df = read_df('in.c-notifications.notifications_full')
#aggregated_df = df.groupby(['job_configuration_id', 'event'])['recipient_address'].agg(', '.join).reset_index()
#pivot_df = aggregated_df.pivot(index="job_configuration_id", columns="event", values="recipient_address")
#st.write(pivot_df)

flows = read_df('out.c-notifications.flow_jobs')
flows['job_created_at'] = pd.to_datetime(flows['job_created_at'])

# Streamlit app
st.title("Operational Dashboard")

st.title("Tab_1: Flow runs")

# Top filters in columns
st.subheader("Filters")
col1, col2, col3, col4 = st.columns(4)

with col1:
    project_names = flows["project_name"].unique()
    selected_project = st.multiselect("Select Project", project_names, project_names)

with col2:
    job_status = flows["job_status"].unique()
    selected_job = st.multiselect("Select Status", job_status, job_status)    

with col3:
    flow_status = flows["component_name"].unique()
    selected_status = st.multiselect("Select Flow", flow_status, flow_status)    

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

# Add a column with clickable links for component_name
# filtered_df['component_name'] = filtered_df.apply(
#     lambda x: f'<a href="{x["link"]}" target="_blank">{x["component_name"]}</a>', axis=1)

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
st.dataframe(styled_df)

# Render and Display the DataFrame with clickable links using st.markdown
st.markdown(styled_df, unsafe_allow_html=True)
#Kritiga, you can see here how html looks with links
#st.markdown(styled_df.to_html(escape=False), unsafe_allow_html=True)

st.title("Tab_2: Notifications")
notifications = read_df('out.c-notifications.components_notif')
# Pivot the DataFrame
pivot_df = notifications.pivot_table(index=['project_name', 'flow_name'], 
                          columns='event', 
                          values='recipient_address', 
                          aggfunc=lambda x: ', '.join(x) if x.notnull().any() else '').reset_index()

# Get the columns to preserve (including flows without notifications)
preserve_columns = notifications[['project_name', 'flow_name', 'last_job_status', 'days_since_last_job', 'status', 'link']].drop_duplicates()

# Merge with the original DataFrame to include other columns
result_df = pd.merge(preserve_columns, pivot_df, on=['project_name', 'flow_name'], how='left')

# Fill NaN with empty strings if necessary
result_df = result_df.fillna('')

# Top filters in columns
st.subheader("Filters")
col1, col2, col3, col4 = st.columns(4)

with col1:
    project_names = result_df["project_name"].unique()
    selected_project = st.multiselect("Select Project", project_names, project_names)

with col2:
    job_status = result_df["status"].unique()
    selected_job = st.multiselect("Select Status", job_status, job_status)    

with col3:
    flow_status = result_df["last_job_status"].unique()
    selected_status = st.multiselect("Last Job Status", flow_status, flow_status)    

with col4:
    failed_job = result_df["job-failed"].unique()
    selected_failed_job = st.multiselect("Failed Job email", failed_job, failed_job)    


# Filter dataframe based on selections
filtered_notif_df = result_df[
    (result_df["project_name"].isin(selected_project)) &
    (result_df["status"].isin(selected_job)) &
    (result_df["last_job_status"].isin(selected_status)) &
    (result_df["job-failed"].isin(selected_failed_job))
]

edited_data = st.data_editor(filtered_notif_df[['project_name', 'flow_name', 'last_job_status', 'days_since_last_job', 'status', "job-failed","job-succeeded", "job-succeeded-with-warning","job-processing-long", 'link']])
#st.write(edited_data)
# original_df_reset = filtered_df.reset_index(drop=True)
# edited_data_reset = edited_data.reset_index(drop=True)

# changes = original_df_reset[["kbc_project_id","kbc_component_configuration","configuration_id_num","status","job_created_at","job_status","job-failed","job-succeeded","job-processing-long"]].compare(edited_data_reset)
# changed_indices = changes.index.get_level_values(0).unique()
# edited_rows = edited_data_reset.loc[changed_indices]

# st.write(edited_rows)
#TODO need to take only the new changes and push them in a loop

# if st.button("Save changes"):
#         response = send_notification(event, job_configuration_id, email_address)
#         st.text("Response from API:")
#         st.text(response)
