import structured_data_ingestion  
import streaming_data_ingestion  
import unstructured_data_ingestion  
import nosqldata
import medical_healthcare_data
## Import necessary libraries 

import streamlit as st

PAGES = {
    "Structured Data Ingestion": structured_data_ingestion, 
    "Streaming Data Ingestion": streaming_data_ingestion,
    "UnStructured Data Ingestion": unstructured_data_ingestion,  
    "NoSQL Data Ingestion": nosqldata,  
    "Medical HealthCare Data": medical_healthcare_data  
}

st.sidebar.title('Navigation')
selection = st.sidebar.radio("Go to", list(PAGES.keys()))
page = PAGES[selection]
page.app()


