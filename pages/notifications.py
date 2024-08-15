import streamlit as st
import pandas as pd
from kbcstorage.client import Client
from datetime import datetime
import requests


def run():    
    # Set up Keboola client
    KEBOOLA_STACK = st.secrets["kbc_url"]
    KEBOOLA_ORG_TOKEN = st.secrets["kbc_data_token"]
    keys_to_exclude = ["kbc_url","kbc_data_token"]
    KEBOOLA_TOKENS = {key: st.secrets[key] for key in st.secrets if key not in keys_to_exclude}

    keboola_client = Client(KEBOOLA_STACK, KEBOOLA_ORG_TOKEN)
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

    def send_notification(event, job_configuration_id, email_address, project_id):
        url = 'https://notification.keboola.com/project-subscriptions'
        
        keboola_token = KEBOOLA_TOKENS.get(str(project_id))
        
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
            'X-StorageApi-Token': keboola_token,
            'Content-Type': 'application/json'
        }
        
        response = requests.post(url, headers=headers, json=payload)
        if response.status_code == 201:
            return "Success"
        else:
            return f"Error: {response.text}"



    st.title("Notifications")
    notifications = read_df('out.c-notifications.components_notif')

    # Define the expected columns based on events
    expected_columns = ["job-failed", "job-succeeded", "job-succeeded-with-warning", "job-processing-long"]

    # Pivot the DataFrame
    pivot_df = notifications.pivot_table(index=['project_name', 'flow_name'], 
                            columns='event', 
                            values='recipient_address', 
                            aggfunc=lambda x: ', '.join(x) if x.notnull().any() else '').reset_index()

    # Check for missing columns and add them with empty values if necessary
    for col in expected_columns:
        if col not in pivot_df.columns:
            pivot_df[col] = ''
            
    # Get the columns to preserve (including flows without notifications)
    preserve_columns = notifications[['project_name',"configuration_id_num", 'flow_name', 'last_job_status', 'days_since_last_job', 'status', 'link', 'project_id',]].drop_duplicates()

    # Merge with the original DataFrame to include other columns
    result_df = pd.merge(preserve_columns, pivot_df, on=['project_name', 'flow_name'], how='left')

    # Fill NaN with empty strings if necessary
    result_df = result_df.fillna('')

    # Top filters in columns
    st.subheader("Filters")
    col1, col2  = st.columns(2)

    with col1:
        project_names = result_df["project_name"].unique()
        with st.expander("Select Project"):
            selected_project = st.multiselect("Select Project", project_names, project_names, label_visibility="hidden")

    with col2:
        job_status = result_df["status"].unique()
        with st.expander("Select Status"):
            # we should keep active flows as default
            selected_job = st.multiselect("Select Status", job_status, 'active', label_visibility="hidden")
    
    col3, col4 = st.columns(2)
    
    with col3:
        flow_status = result_df["last_job_status"].unique()
        with st.expander("Select Last Job Status"):
            selected_status = st.multiselect("Select Last Job Status", flow_status, flow_status, label_visibility="hidden")    

    with col4:
        failed_job = result_df["job-failed"].unique()
        with st.expander("Failed Job email"):
            selected_failed_job = st.multiselect("Failed Job email", failed_job, failed_job, label_visibility="hidden")


    # Filter dataframe based on selections
    filtered_notif_df = result_df[
        (result_df["project_name"].isin(selected_project)) &
        (result_df["status"].isin(selected_job)) &
        (result_df["last_job_status"].isin(selected_status)) &
        (result_df["job-failed"].isin(selected_failed_job))
    ]

    def status_lj_color(status):
        if status == "success":
            return "color: #34A853"
        if status == "cancelled":
            return "color: #FBBC05"
        else:
            return "color: #EA4335"
        
    filtered_notif_df["add_notification"] = False
    edited_data = st.data_editor(filtered_notif_df[["add_notification",'project_name', 'flow_name', 
                                                    'last_job_status', 'days_since_last_job', 'status', 
                                                    "job-failed","job-succeeded", "job-succeeded-with-warning",
                                                    "job-processing-long", 'link', "configuration_id_num", 
                                                    "project_id"]],
                                 column_order=("add_notification",'project_name', 'flow_name', 
                                                    'last_job_status', #'days_since_last_job', 
                                                    'status', 
                                                    "job-failed","job-succeeded", "job-succeeded-with-warning",
                                                    "job-processing-long", 'link'),
                                 column_config={'add_notification': 'Select',
                                                'project_name': 'Project',
                                                'flow_name': 'Flow',
                                                'last_job_status': 'Status of Last Job',
                                                #'days_since_last_job':st.column_config.NumberColumn('Days Since Last Job', format="%d"),
                                                'status': st.column_config.TextColumn('Status',
                                                                                      help="Active are flows with <30 days since last run"),
                                                'job-failed': 'Email: Failed',
                                                'job-succeeded': 'Email: Success',
                                                'job-succeeded-with-warning': 'Email: Warning',
                                                "job-processing-long":  "Email: Long Processing",
                                                "link":st.column_config.LinkColumn("Link", display_text="Click here")},
                                 disabled=['last_job_status'],
                                 hide_index=True)

    updated_df = edited_data[edited_data["add_notification"]==True]

    # Dropdown to select job status
    job_status_options = ["job-failed", "job-succeeded", "job-succeeded-with-warning"]
    selected_job_status = st.selectbox("Select Job Status for Notification", job_status_options)

    # Input field for email ID
    email_id = st.text_input("Enter email")

    # Button to save changes and send notifications
    if st.button("Save changes"):
        for index, row in updated_df.iterrows():
            job_configuration_id = str(row["configuration_id_num"]) 
            project_id = row["project_id"]  # Change: Retrieve project_id for the current row
            response = send_notification(selected_job_status, job_configuration_id, email_id, project_id)  # Change: Pass project_id to send_notification
            st.text(response)
