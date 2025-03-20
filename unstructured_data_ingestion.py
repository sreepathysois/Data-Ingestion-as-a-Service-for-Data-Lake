import streamlit as st
import streamlit.components.v1 as components
from PIL import Image
import paramiko
import os
import pandas as pd
import csv
import mysql.connector
from io import StringIO
from minio import Minio
# from minio.error import ResponseError
import psycopg2
import requests
from urllib.parse import urlparse
import streamlit.components.v1 as stc
from datetime import datetime
# File Processing Pkgs
import pandas as pd
import docx2txt
from PIL import Image
from PyPDF2 import PdfFileReader
import pdfplumber
from PIL import Image

def load_image(image_file):
	img = Image.open(image_file)
	return img


def app():
    image = Image.open('msis.jpeg')

    st.image(image, width=100)

    st.title('Welcome to Data Ingestion of Un-Structured Data Types')

    st.sidebar.title('Un-Structured DataTypes')
    option = st.sidebar.selectbox(
        'select subjects', ('Text', 'Videos', 'Images'))

    if option == 'Text':

        st.sidebar.title('Text Upload Types')
        option = st.sidebar.selectbox(
            'select text_upload_types', ('Local File Browse', 'URL Link', 'Object Storage Types'))
        if option == 'Local File Browse':
            st.subheader("DocumentFiles")
            client = Minio('172.16.51.28:9000',
                           access_key='minio',
                           secret_key='miniostorage',
                           secure=False)

            docx_files = st.file_uploader("Upload Document", type=[
                "pdf", "docx", "txt"], accept_multiple_files=True)
            if docx_files is not None:
                for docx_file in docx_files:
                    file_details = {"filename": docx_file.name, "filetype": docx_file.type,
                                    "filesize": docx_file.size}
                    st.write(file_details['filename'])

                    if docx_file.type == "text/plain":
                        raw_text = str(docx_file.read(), "utf-8")
                        st.text(raw_text)
                        st.write(file_details['filename'])
                        #tempDir = "/home/msis/sreephd_data_ingestion_service/dis_version_1_ingestion_hetrogenous_types/."
                        start_time = datetime.now()
                        tempDir = os.getcwd() 
                        with open(os.path.join(tempDir, file_details['filename']), "wb") as f:
                            f.write(docx_file.getbuffer())
                        with open(file_details['filename'], 'rb') as file_data:
                            file_stat = os.stat(file_details['filename'])
                            client.put_object(
                                'sree', file_details['filename'], file_data, file_stat.st_size,)
                            st.write(f"my_app render: {(datetime.now() - start_time).total_seconds()}s")
                    elif docx_file.type == "application/pdf":
                        try:
                            with pdfplumber.open(docx_file) as pdf:
                                pages = pdf.pages[0]
                                st.write(pages.extract_text())
                                start_time = datetime.now()
                                tempDir = os.getcwd() 
                                #tempDir = "/home/msis/sreephd_data_ingestion_service/."
                                with open(os.path.join(tempDir, file_details['filename']), "wb") as f:
                                    f.write(docx_file.getbuffer())
                                with open(file_details['filename'], 'rb') as file_data:
                                    file_stat = os.stat(
                                        file_details['filename'])
                                    client.put_object(
                                        'sree', file_details['filename'], file_data, file_stat.st_size,)
                                    st.write(f"my_app render: {(datetime.now() - start_time).total_seconds()}s")
                        except:
                            st.write("None")
                    else:
                        raw_text = docx2txt.process(docx_file)
                        st.write(raw_text)
                        #tempDir = "/home/msis/sreephd_data_ingestion_service/."
                        start_time = datetime.now()
                        tempDir = os.getcwd() 
                        with open(os.path.join(tempDir, file_details['filename']), "wb") as f:
                            f.write(docx_file.getbuffer())
                        with open(file_details['filename'], 'rb') as file_data:
                            file_stat = os.stat(file_details['filename'])
                            client.put_object(
                                'sree', file_detals['filename'], file_data, file_stat.st_size,)
                            st.write(f"my_app render: {(datetime.now() - start_time).total_seconds()}s")
    if option == 'Images':
        st.sidebar.title('Image Upload Types')
        option = st.sidebar.selectbox(
            'select image_upload_types', ('Local File Browse', 'URL Link', 'Object Storage Types'))
        if option == 'Local File Browse':
            st.subheader("Image Dataset")
            client = Minio('172.16.51.28:9000', access_key='minio',
                           secret_key='miniostorage', secure=False)
            st.subheader("Image")
            uploaded_files = st.file_uploader(
                "Upload Images", type=["png", "jpg", "jpeg"], accept_multiple_files=True)

            if uploaded_files is not None:
                for image_file in uploaded_files:
                    file_details = {"filename": image_file.name, "filetype": image_file.type,
                                    "filesize": image_file.size}
                    st.write(file_details)
                    st.image(load_image(image_file), width=250)
                    #fileDir = "/home/msis/sreephd_data_ingestion_service/."
                    start_time = datetime.now()
                    fileDir = os.getcwd() 
                    with open(os.path.join(fileDir, image_file.name), "wb") as f:
                        f.write((image_file).getbuffer())

                    with open(file_details['filename'], 'rb') as file_data:
                        file_stat = os.stat(file_details['filename'])
                        client.put_object(
                            'sree', file_details['filename'], file_data, file_stat.st_size)

                        st.write(f"my_app render: {(datetime.now() - start_time).total_seconds()}s")

    if option == 'Videos':
        st.sidebar.title('Video Upload Types')
        option = st.sidebar.selectbox(
            'select video_upload_types', ('Local File Browse', 'URL Link', 'Object Storage Types'))
        if option == 'Local File Browse':
            st.subheader("Video Dataset")
            client = Minio('172.16.51.28:9000', access_key='minio',
                           secret_key='miniostorage', secure=False)
            st.subheader("Image")
            uploaded_files = st.file_uploader(
                "Upload Images", type=["mp4", "mpeg"], accept_multiple_files=True)

            if uploaded_files is not None:
                for video_file in uploaded_files:
                    file_details = {"filename": video_file.name, "filetype": video_file.type,
                                    "filesize": video_file.size}
                    st.write(file_details)
                    st.video(video_file)
                    start_time = datetime.now()
                    fileDir = os.getcwd() 
                    #fileDir = "/home/msis/sreephd_data_ingestion_service/."
                    with open(os.path.join(fileDir, video_file.name), "wb") as f:
                        f.write((video_file).getbuffer())

                    with open(file_details['filename'], 'rb') as file_data:
                        file_stat = os.stat(file_details['filename'])
                        client.put_object(
                            'sree', file_details['filename'], file_data, file_stat.st_size)
                        st.write(f"my_app render: {(datetime.now() - start_time).total_seconds()}s")
