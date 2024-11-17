import streamlit as st
import asyncio
import psycopg2
import mysql.connector
import pyodbc
import sqlite3
from pymongo import MongoClient
import boto3
import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud import bigquery
import ollama
import os
import pandas as pd

# Template to guide Ollama in query generation
template = (
    "Translate the following question into an appropriate SQL or NoSQL query based on the chosen database. "
    "The chosen database is: {chosen_db}. "
    "Your response should be the relevant query according to the database. "
    "**No Extra Content:** Do not include any additional text, comments, or explanations in your response. "
    "**Empty Response:** If no information matches the description, return an empty string ('')."
    "**Direct Data Only:** Your output should contain only the data that is explicitly requested, with no other text."
)

# Asynchronous function for database connection
async def connect_to_db(chosen_db, host, port, db_name, username, password, db_file, aws_access_key, aws_secret_key, region):
    try:
        if chosen_db == "PostgreSQL":
            conn = psycopg2.connect(
                host=host, port=port, dbname=db_name, user=username, password=password
            )
        elif chosen_db == "MySQL":
            conn = mysql.connector.connect(
                host=host, port=port, database=db_name, user=username, password=password
            )
        elif chosen_db == "Microsoft SQL Server":
            conn = pyodbc.connect(
                f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={host};PORT={port};DATABASE={db_name};UID={username};PWD={password}"
            )
        elif chosen_db == "SQLite":
            conn = sqlite3.connect(db_file)
        elif chosen_db == "MongoDB":
            client = MongoClient(f"mongodb://{username}:{password}@{host}:{port}/{db_name}")
            conn = client[db_name]
        elif chosen_db == "Amazon DynamoDB":
            session = boto3.Session(
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key,
                region_name=region
            )
            dynamodb = session.resource('dynamodb')
            conn = dynamodb.Table(db_name)
        elif chosen_db == "Firestore":
            cred = credentials.Certificate("path_to_your_service_account_file.json")
            firebase_admin.initialize_app(cred)
            conn = firestore.client()
        elif chosen_db == "BigQuery":
            client = bigquery.Client.from_service_account_json("path_to_your_service_account_file.json")
            conn = client

        return conn
    except Exception as e:
        st.error(f"Error connecting to {chosen_db}: {str(e)}")
        return None

# Asynchronous function to generate query with Ollama
async def generate_query(user_input, chosen_db):
    prompt = template.format(chosen_db=chosen_db) + f"\nQuestion: '{user_input}'"

    try:
        response = ollama.chat(model="llama-3.1", messages=[{"role": "user", "content": prompt}])
        return response['text']
    except Exception as e:
        st.error(f"Error with Ollama: {e}")
        return None

# Asynchronous function to execute query on the database
async def execute_query(modified_query, chosen_db, conn):
    try:
        if chosen_db == "PostgreSQL":
            cursor = conn.cursor()
            cursor.execute(modified_query)
            result = cursor.fetchall()
            cursor.close()
        elif chosen_db == "MySQL":
            cursor = conn.cursor()
            cursor.execute(modified_query)
            result = cursor.fetchall()
            cursor.close()
        elif chosen_db == "Microsoft SQL Server":
            cursor = conn.cursor()
            cursor.execute(modified_query)
            result = cursor.fetchall()
            cursor.close()
        elif chosen_db == "SQLite":
            cursor = conn.cursor()
            cursor.execute(modified_query)
            result = cursor.fetchall()
            cursor.close()
        elif chosen_db == "MongoDB":
            result = conn.command(modified_query)
        elif chosen_db == "Amazon DynamoDB":
            result = conn.query(KeyConditionExpression=modified_query)
        elif chosen_db == "Firestore":
            result = conn.collection(db_name).where(modified_query)
        elif chosen_db == "BigQuery":
            result = conn.query(modified_query).result()

        return result
    except Exception as e:
        st.error(f"Error executing the query: {e}")
        return None

# Frontend of the Streamlit app
st.title("Database Chatbot with Ollama")

# Database selection
db_choice = st.selectbox("Select a database", 
                         ["PostgreSQL", "MySQL", "Microsoft SQL Server", "SQLite", "MongoDB", 
                          "Amazon DynamoDB", "Firestore", "BigQuery"])

# Connection information input
st.subheader("Enter the connection details for the selected database")

host = st.text_input("Host")
port = st.number_input("Port", min_value=1, max_value=65535, value=5432)
db_name = st.text_input("Database Name")
username = st.text_input("Username")
password = st.text_input("Password", type="password")
db_file = st.text_input("Database File (SQLite only)")

aws_access_key = st.text_input("AWS Access Key (if DynamoDB)")
aws_secret_key = st.text_input("AWS Secret Key (if DynamoDB)")
region = st.text_input("AWS Region (if DynamoDB)")

# Natural language query input
st.subheader("Enter your query in natural language")
user_input = st.text_area("Your question")

# Response format choice
response_format = st.radio(
    "How would you like the response to be displayed?",
    ('Table', 'Plain Text')
)

# Generate query if the user has submitted one
if user_input:
    generated_query = asyncio.run(generate_query(user_input, db_choice))
    if generated_query:
        st.subheader(f"Generated Query for {db_choice}:")
        
        # Display and edit the generated query
        modified_query = st.text_area("Modify the query (if necessary)", generated_query)

        # Button to execute the query on the database
        if st.button("Execute Query on the DB"):
            # Asynchronous connection to the database
            conn = asyncio.run(connect_to_db(db_choice, host, port, db_name, username, password, db_file, aws_access_key, aws_secret_key, region))

            if conn:
                # Execute the query asynchronously
                result = asyncio.run(execute_query(modified_query, db_choice, conn))

                if result:
                    st.subheader("Query Results:")

                    # Display in chosen format
                    if response_format == 'Table':
                        # Convert results to DataFrame for table display
                        df = pd.DataFrame(result)
                        st.table(df)
                    else:
                        # Display plain text
                        for row in result:
                            st.write(row)

                    # Send results to Ollama for summarization
                    ollama_prompt = f"Here are the results from the query on {db_choice}: {result}. Summarize and format the result concisely."
                    try:
                        ollama_response = ollama.chat(model="llama-3.1", messages=[{"role": "user", "content": ollama_prompt}])
                        st.subheader("Summary generated by Ollama:")
                        st.write(ollama_response['text'])
                    except Exception as e:
                        st.error(f"Error with Ollama for generating the summary: {e}")
            else:
                st.error("Unable to connect to the database.")
