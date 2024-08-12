import streamlit as st
import pages.flow_runs as flow_runs
import pages.notifications as notifications

# Set up Streamlit page
st.set_page_config(layout="wide")


# Define the pages available in the app
pages = {
    "Flow Runs": flow_runs.run,
    "Notifications": notifications.run
}

# Sidebar for navigation
st.sidebar.title("Navigation")
selected_page = st.sidebar.radio("Go to", list(pages.keys()))

# Run the selected page
page_function = pages[selected_page]
page_function()
