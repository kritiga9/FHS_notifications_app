import streamlit as st
import pages.flow_runs as flow_runs
import pages.notifications as notifications
import os 
import base64

# Set up Streamlit page
st.set_page_config(layout="wide")

# Get the path to the logo image
IMAGE_PATH = os.path.dirname(os.path.abspath(__file__))
KEBOOLA_LOGO_PATH = IMAGE_PATH + "/static/keboola_logo.png"

# Display the logo at the top right
logo_html = f'<div style="display: flex; justify-content: flex-end;"><img src="data:image/png;base64,{base64.b64encode(open(KEBOOLA_LOGO_PATH, "rb").read()).decode()}" style="width: 200px; margin-bottom: 10px;"></div>'
st.markdown(f"{logo_html}", unsafe_allow_html=True)

# Define the pages available in the app
pages = {
    "Flow Runs": flow_runs.run,
    "Notifications": notifications.run
}

# Sidebar for navigation
# Only the radio button should appear in the sidebar
st.sidebar.title("Navigation")
selected_page = st.sidebar.radio("Go to", list(pages.keys()))

# Run the selected page's function
page_function = pages[selected_page]
page_function()
