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
import boto3
from datetime import datetime


def app():
    image = Image.open('msis.jpeg')

    st.image(image, width=100)

    st.title('Welcome to Data Ingestion of Structured Data Types')

    st.sidebar.title('DataTypes')
    option = st.sidebar.selectbox(
        'select subjects', ('Databases', 'Data Warehouses', 'CSV', 'Excel'))

    if option == 'Databases':

        st.sidebar.title('Databases Types')
        option = st.sidebar.selectbox(
            'select databases_types', ('Mysql', 'Postgres SQL'))
        if option == 'Mysql':
            host_name = st.text_input("Database Server Name")
            db_user_name = st.text_input("Database User Name")
            db_user_password = st.text_input("Database User Password")
            db_name = st.text_input("Database Name")
            st.write(db_user_name)
            mydb = mysql.connector.connect(
                host=host_name,
                user=db_user_name,
                password=db_user_password,
                # database=sys.argv[3]
                database=db_name, auth_plugin='mysql_native_password')  # Name of t
            cursor = mydb.cursor()
            tables_display_querry = "show tables from" + " " + db_name
            st.write(tables_display_querry)
            sql = tables_display_querry
            # sql = "INSERT INTO virtuallabs.student VALUES (%s,%s,%s,%s,%s,%s)"
            cursor.execute(tables_display_querry)
            tables = cursor.fetchall()
            # for table_name in cursor:
            # st.write(table_name)
            options = []
            for (table_name,) in tables:
                options.append(table_name)
            tables_selected = st.multiselect("Tables List", options)
            # mydb.commit()
            # st.table(table_list)
            st.write(tables_selected)
            # st.write(tables_selected[1])
            for tables in tables_selected:
                st.write(tables)
            st.write(
                "Export Tables of Your Choice in to CSV for Ingestion to Data Lake ")
            for table in tables_selected:
                table_export_querry = "select * from " + " " + db_name + "." + table
                sql_query = pd.read_sql_query(table_export_querry, mydb)
                df = pd.DataFrame(sql_query)
                st.table(df.head())
                #file_path_name = "/home/msis/sreephd_data_ingestion_service/dis_version_1_ingestion_hetrogenous_types/" + table + "." + "csv"
                start_time = datetime.now()
                file_path_name = os.getcwd() +  "/" + table + "." + "csv"
                df.to_csv(file_path_name, index=True)
                object_name = table + "." + "csv"
                client = Minio('172.16.51.28:9000',
                               access_key='minio',
                               secret_key='miniostorage',
                               secure=False)

                with open(object_name, 'rb') as file_data:
                    file_stat = os.stat(object_name)

                    client.put_object('sree', object_name, file_data,
                                      file_stat.st_size,)
                    print(f"my_app render: {(datetime.now() - start_time).total_seconds()}s")
                    st.write(f"my_app render: {(datetime.now() - start_time).total_seconds()}s")

        if option == 'Postgres SQL':

            host_name = st.text_input("Database Server Name")
            db_user_name = st.text_input("Database User Name")
            db_user_password = st.text_input("Database User Password")
            db_name = st.text_input("Database Name")
            db_port = st.text_input("Database Port Number")
            conn = psycopg2.connect(
                host=host_name, port=db_port, dbname=db_name, user=db_user_name, password=db_user_password)
            cur = conn.cursor()
            list_table_querry = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name"
            cur.execute(list_table_querry)
            records = cur.fetchall()
            for table_list in records:
                st.write(table_list)
            options = []
            for (tables_post,) in records:
                options.append(tables_post)
            tables_selected = st.multiselect("Tables List", options)
            # mydb.commit()
            # st.table(table_list)
            st.write(tables_selected)
            # st.write(tables_selected[1])
            for tables in tables_selected:
                st.write(tables)
            st.write(
                "Export Tables of Your Choice in to CSV for Ingestion to Data Lake ")
            for tables in tables_selected:
                # sql = "COPY (SELECT * FROM a_table WHERE month=6) TO STDOUT WITH CSV DELIMITER ';'"
                table_export_querry = " COPY ( select * FROM " + \
                    tables + ")" + " TO STDOUT WITH CSV DELIMITER ';' "
                file_path_name = tables + "." + "csv"
                with open(file_path_name, "w") as file:
                    cur.copy_expert(table_export_querry, file)
                client = Minio('172.16.51.28:9000',
                               access_key='minio',
                               secret_key='miniostorage',
                               secure=False)

                with open(file_path_name, 'rb') as file_data:
                    file_stat = os.stat(file_path_name)

                    client.put_object('sree', file_path_name, file_data,
                                      file_stat.st_size,)

    if option == 'CSV':
        st.sidebar.title('CSV Upload Types')
        option = st.sidebar.selectbox(
            'csv_upload_types', ('File Browse', 'URL Link', 'Cloud Storage'))
        if option == 'File Browse':
            uploaded_file = st.file_uploader("Choose a file")
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
                dataframe = pd.read_csv(uploaded_file)
                st.write(dataframe)
                dataframe.to_csv(uploaded_file.name, index=False, header=True)
                client = Minio('172.16.51.28:9000',
                               access_key='minio',
                               secret_key='miniostorage',
                               secure=False)

                with open(uploaded_file.name, 'rb') as file_data:
                    file_stat = os.stat(uploaded_file.name)

                    client.put_object('sree', uploaded_file.name, file_data,
                                      file_stat.st_size,)
        if option == 'URL Link':
            url = st.text_input("Provide URL Link of CSV Files")
            # url = 'http://google.com/favicon.ico'
            r = requests.get(url, allow_redirects=True)
            a = urlparse(url)
            st.write(a.path)
            st.write(os.path.basename(a.path))
            file_name_path = os.path.basename(a.path)
            client = Minio('172.16.51.28:9000',
                           access_key='minio',
                           secret_key='miniostorage',
                           secure=False)
            # EDIT - because comment.
            df = pd.read_csv(url)
            df.to_csv(file_name_path, index=False, header=True)
            with open(file_name_path, 'rb') as file_data:
                file_stat = os.stat(file_name_path)

                client.put_object('sree', file_name_path, file_data,
                                  file_stat.st_size,)

        if option == 'Cloud Storage':
            st.sidebar.title('Cloud Storage Types')
            option = st.sidebar.selectbox(
                'select cloud_storage_types', ('Minio', 'S3 Bucket', 'Google Cloud Storage'))
            if option == 'S3 Bucket':
                minio_client = Minio('172.16.51.28:9000',
                                     access_key='minio',
                                     secret_key='miniostorage',
                                     secure=False)
                #aws_access_key = st.text_input("AWS Access Key")
                #aws_secret_key = st.text_input("AWS Secret Key")
                #aws_region_name = st.text_input("AWS Region Code")
                session = boto3.Session(
                    #aws_access_key_id='',
                    #aws_secret_access_key='',
                    aws_access_key_id=aws_access_key,
                    aws_secret_key=aws_secret_key,
                    region_name='us-east-1')
                # Then use the session to get the resource
                s3 = session.resource('s3')
                buckets_list = s3.buckets.all()
                for bucket in buckets_list:
                    st.write(bucket.name)
                list_buckets = []
                for bucket in buckets_list:
                    list_buckets.append(bucket.name)
                bucket_selected = st.multiselect("Buckets List", list_buckets)
                for mybucket in bucket_selected:
                    st.write("Bucket Selected For Ingestion of Objects", mybucket)
                for mybucket in bucket_selected:
                    my_bucket = s3.Bucket(mybucket)
                    list_object = []
                    for s3_object in my_bucket.objects.all():
                        list_object.append(s3_object.key)
                        object_selected = st.multiselect("Objects List", list_object)
                        for object in object_selected:
                            filename = s3_object.key
                            my_bucket.download_file(s3_object.key, filename)
                            with open(filename, 'rb') as file_data:
                                file_stat = os.stat(filename)
                                minio_client.put_object('sree', filename, file_data, file_stat.st_size) 
