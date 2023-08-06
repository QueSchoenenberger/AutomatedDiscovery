import os
import openai
import pymysql
import prettytable
from extract_metadata import get_metadata, CustomJsonEncoder
import json

OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
openai.api_key = OPENAI_API_KEY


def chat(system_content, prompt_content):
    completion = openai.ChatCompletion.create(
        model="gpt-3.5-turbo-16k",
        messages=[
            {
                "role": "system",
                "content": system_content
            },
            {
                "role": "user",
                "content": prompt_content
            }
        ],
        temperature=0,
        max_tokens=600,
        top_p=1,
        frequency_penalty=0,
        presence_penalty=0
    )
    return completion["choices"][0]["message"]["content"]


def get_queries(metadata_parameter, relations_parameter, first_name, last_name):
    results = []
    if metadata_parameter and relations_parameter:
        metadata_json = json.dumps(metadata_parameter, cls=CustomJsonEncoder)
        relations_json = json.dumps(relations_parameter, cls=CustomJsonEncoder)

        system_template: str = (
            "Your task is to construct queries based on a user's request, utilizing the specified Data. "
            "Here's what you need to know: \\n Metadata: [METADATA] \\n Relations: [RELATIONS]. Your responses should align "
            "with the user's requirements and make use of the given information.")


        prompt_template: str = (
            "You have been provided with Relational Databases Metadata and Relations. Can you craft a query to retrieve all "
            "information about a person, using placeholders for the Firstname and Lastname as [Firstname] and [Lastname]? "
            "Your query should include all tables related to the user and adhere to the following format: 'SELECT * FROM table_name;'. "
            "Please ensure that each query is presented on a separate line and that the response contains only the queries themselves.")


        system_message = system_template.replace("[METADATA]", metadata_json)
        system_message = system_message.replace("[RELATIONS]", relations_json)

        prompt_message = prompt_template.replace("[Firstname]", first_name)
        prompt_message = prompt_message.replace("[Lastname]", last_name)

        result_text = chat(system_message, prompt_message)
        
        # Split result_text into separate queries
        result_queries = result_text.strip().split(';')
        
        # Remove any empty queries
        result_queries = [q.strip() for q in result_queries if q.strip()]

        return result_queries
    else:
        return None


def format_output(output_text):
    lines = output_text.strip().split('\n')
    formatted_output = []

    for line in lines:
        line = line.strip()
        if line and line not in formatted_output:
            formatted_output.append(line)

    formatted_output_str = '\n'.join(formatted_output)
    return formatted_output_str


def run_query(host, user, password, database, queries):
    connection = pymysql.connect(host=host, user=user, password=password, database=database)
    cursor = connection.cursor()

    tables = []
    try:
        # Execute each SQL query
        for query in queries:
            cursor.execute(query)
            result = cursor.fetchall()

            # Store the result in a table
            if result:
                field_names = [desc[0] for desc in cursor.description]
                table = [dict(zip(field_names, row)) for row in result]
                tables.append(table)
            else:
                tables.append("No results for the query.")
    except Exception as e:
        tables.append(f"Error executing query: {str(e)}")

    # Close the database connection
    cursor.close()
    connection.close()

    return tables

