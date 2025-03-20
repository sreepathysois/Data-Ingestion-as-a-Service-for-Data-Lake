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
from pymongo import MongoClient
import pymongo
import json
from bson import json_util, ObjectId
from datetime import datetime

def app():
    image = Image.open('msis.jpeg')

    st.image(image, width=100)

    st.title('Welcome to Data Ingestion of NoSQL Data Types')

    st.sidebar.title('NoSQL DataTypes')
    option = st.sidebar.selectbox(
        'select subjects', ('MongoDB', 'Casaandra', 'DynamoDB', 'JSON/Avro/XML'))

    if option == 'MongoDB':
        minio_client = Minio('172.16.51.28:9000',
                             access_key='minio',
                             secret_key='miniostorage',
                             secure=False)
        client = MongoClient()
        mongo_host = st.text_input("Mongodb Host")
        # mongo_port = st.text_input("Mongdb Port")
        mongo_db = st.text_input("Mongdb Database")
        # mongo_collections = st.text_input("Mongdb Collections")
        connection_querry = "mongodb://" + mongo_host + ":27017/"
        myclient = pymongo.MongoClient(connection_querry)
        mydb = myclient[mongo_db]
        # mycol = mydb.mongo_collections
        cursor = mydb.list_collection_names()
        for collections in cursor:
            st.write(collections)
        options = []
        for (collection) in cursor:
            options.append(collection)
        collection_selected = st.multiselect("Collection List", options)
        for collection in collection_selected:
            mycol = mydb[collection]
            mycollcursor = mycol.find()
            for doc in mycollcursor:
                st.write(doc)
                data = json.loads(json_util.dumps(doc))
                filename = collection + ".json"
                start_time = datetime.now()
                with open(filename, "w+") as f:
                    json.dump(data, f)
                with open(filename, 'rb') as file_data:
                    file_stat = os.stat(filename)
                    minio_client.put_object(
                        'sree', filename, file_data, file_stat.st_size,)
                    st.write(f"my_app render: {(datetime.now() - start_time).total_seconds()}s")

    if option == 'JSON/Avro/XML':
        st.sidebar.title('JSON Upload Types')
        option = st.sidebar.selectbox(
            'json_upload_types', ('File Browse', 'URL Link', 'Cloud Storage'))
        if option == 'File Browse':
            uploaded_file = st.file_uploader(
                "Choose a file", type=["json", "avro", "xml"])
            if uploaded_file is not None:
                # bytes_data = uploaded_file.getvalue()
                # st.write(bytes_data)

                # To convert to a string based IO:
                # stringio = StringIO(uploaded_file.getvalue().decode("utf-8"))
                # st.write(stringio)

                # To read file as string:
                # string_data = stringio.read()
                # st.write(string_data)

                # Can be used wherever a "file-like" object is accepted:
                st.write("Filename: ", uploaded_file.name)
                st.write(uploaded_file)
                # dataframe.to_csv(uploaded_file.name, index=False, header=True)
                data = json.loads(json_util.dumps(uploaded_file))
                start_time = datetime.now()
                with open(uploaded_file.name, "w+") as f:
                    json.dump(data, f)
                client = Minio('172.16.51.28:9000',
                               access_key='minio',
                               secret_key='miniostorage',
                               secure=False)

                with open(uploaded_file.name, 'rb') as file_data:
                    file_stat = os.stat(uploaded_file.name)

                    client.put_object('sree', uploaded_file.name, file_data,
                                      file_stat.st_size,)
                    st.write(f"my_app render: {(datetime.now() - start_time).total_seconds()}s")
        if option == 'URL Link':
            url = st.text_input("Provide URL Link of JSON Files")
            # url = 'http://google.com/favicon.ico'
            r = requests.get(url, allow_redirects=True)
            a = urlparse(url)
            st.write(a.path)
            st.write(os.path.basename(a.path))
            file_name_path = os.path.basename(a.path)
            data = json.loads(json_util.dumps(r))
            with open(file_name_path, "w+") as f:
                json.dump(data, f)
            client = Minio('172.16.51.28:9000',
                           access_key='minio',
                           secret_key='miniostorage',
                           secure=False)

            with open(file_name_path, 'rb') as file_data:
                file_stat = os.stat(file_name_path)

                start_time = datetime.now()
                client.put_object('sree', file_name_path, file_data,
                                  file_stat.st_size,)
                st.write(f"my_app render: {(datetime.now() - start_time).total_seconds()}s")

        if option == 'Cloud Storage':
            st.sidebar.title('Cloud Storage Types')
            option = st.sidebar.selectbox(
                'select cloud_storage_types', ('Minio', 'S3 Bucket', 'Google Cloud Storage'))
            if option == 'Minio':
                a_key = st.text_input("Minio Access Key")
                s_key = st.text_input("Minio Secret Key")
                minio_host = st.text_input("Minio Host")
                connect_minio = minio_host + ":9000"
                client = Minio(connect_minio,
                               access_key=a_key,
                               secret_key=s_key,
                               secure=False)

                # List buckets
                buckets = client.list_buckets()
                for bucket in buckets:
                    # st.write('bucket:', bucket.name, bucket.creation_date)
                    st.write('bucket:', bucket.name)
                options = []
                for bucket in buckets:
                    options.append(bucket)
                bucket_selected = st.multiselect(
                    "Bucket List of Minio Storage", options)
                for mybucket in bucket_selected:
                    bucket_name = str(mybucket)
                    objects = client.list_objects(bucket_name,
                                                  recursive=True)
                    list_objects = []
                    for obj in objects:
                        # st.write(obj.bucket_name, obj.object_name.encode('utf-8'), obj.last_modified,
                        #         obj.etag, obj.size, obj.content_type)
                        st.write(obj.object_name, obj.size)
                        list_objects.append(obj.object_name)
                    objects_selected = st.multiselect(
                        "Objects Selected For Ingestion", list_objects)
                    for my_obj in objects_selected:
                        object_name = str(my_obj)
                        object_data = client.get_object(
                            bucket_name, object_name)
                        file_name = object_name
                        with open(file_name, 'wb') as file_data:
                            for d in object_data.stream(32*1024):
                                file_data.write(d)

                        with open(file_name, 'rb') as file_data:
                            file_stat = os.stat(file_name)
                            start_time = datetime.now()

                            client.put_object('msisbucket', file_name, file_data,
                                              file_stat.st_size,)
                            st.write(f"my_app render: {(datetime.now() - start_time).total_seconds()}s")
