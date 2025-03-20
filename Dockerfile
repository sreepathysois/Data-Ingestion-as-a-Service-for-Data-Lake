FROM python:3.9-slim
WORKDIR /app
COPY . /app/
RUN apt-get update -y
RUN apt-get install python3-pip -y
RUN pip3 install -r requirnments.txt
EXPOSE 8501
ENTRYPOINT ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
