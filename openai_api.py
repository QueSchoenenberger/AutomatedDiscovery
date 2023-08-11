import os
import openai
import pymysql
from extract_metadata import CustomJsonEncoder
import json
import pyodbc
import psycopg2

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
        max_tokens=1000,
        top_p=1,
        frequency_penalty=0,
        presence_penalty=0
    )
    return completion["choices"][0]["message"]["content"]


def remove_personal_data(file_content):
    system_template: str = (
        "You are a text redaction assistant. Your task is to process this text [FILE-CONTENT] and return the same "
        "text but replace all instances of personal data such as names, addresses, IBANs and social security numbers "
        "with [MASKED]. Ensure that no sensitive information is left visible and return the modified text in plain "
        "text form."
    )

    prompt_template: str = (
        "Please redact or mask any personal data in the text"
    )

    max_tokens = 16385  # Maximum token limit for GPT-3

    # Split file content into chunks that fit within the token limit
    chunks = [file_content[i:i + max_tokens] for i in range(0, len(file_content), max_tokens)]

    results = []

    for chunk in chunks:
        system_message = system_template.replace("[FILE-CONTENT]", chunk)
        prompt_message = prompt_template

        result = chat(system_message, prompt_message)
        results.append(result)

    return ''.join(results)


def get_queries(metadata_parameter, relations_parameter, first_name, last_name, actions):
    result_text = []
    result_metadata = []
    if metadata_parameter:
        metadata_json = json.dumps(metadata_parameter, cls=CustomJsonEncoder)
        relations_json = json.dumps(relations_parameter, cls=CustomJsonEncoder)

        system_message: str = (
            "Your task is to execute the USER's request without adding any extraneous information. For example, "
            "if the user requests a SELECT query, simply provide that and then stop."
            "Next, you will be presented with Metadata. Your responsibility is to return this information in a "
            "specific format, where each table's information is provided on a separate line. The format is as follows:"
            "'TABLENAME1, USERDATA: TRUE/FALSE, NAME: TRUE/FALSE' "
            "This format is constructed with two key fields: USERDATA and NAME. "
            "1. USERDATA: This field indicates whether the table may contain personal information such as Name, "
            "Address, Email Address, Contact Data, Birthdate, Bank Data (e.g., IBAN, Credit Card Number), "
            "or Social Numbers (e.g., AHV Number in Switzerland). If the table has this information, the value is "
            "TRUE; otherwise, it's FALSE."
            "2. NAME: This field represents whether the table has columns that can be linked to a person's first and "
            "last name (even if the columns are not named that way). This might be found in tables related to "
            "customers or staff. If such columns exist, the value is TRUE; otherwise, it's FALSE."
            "Please present the information for each table on a separate line, following the above guidelines.")

        prompt_template: str = (
            "Metadata: [Metadata]")

        chunks = split_data(metadata_json)

        for chunk in chunks:
            prompt_message = prompt_template.replace("[Metadata]", chunk)
            result_metadata += "\n" + chat(system_message, prompt_message)

        if actions == "showPersonalData":
            result_text = process_tables(metadata_json, relations_json, result_metadata, first_name, last_name)

        elif actions == "showTables":
            result_text = process_tables2(metadata_json)

        # Split result_text into separate queries
        result_queries = result_text.strip().split(';')

        # Remove any empty queries
        result_queries = [q.strip() for q in result_queries if q.strip()]

        return result_queries
    else:
        return None


def process_tables(table_string, table_relations, table_definitions, first_name, last_name):
    table_info = json.loads(table_string)
    table_relation_info = json.loads(table_relations)
    table_definitions_lines = ''.join(table_definitions).strip().split('\n')
    print(table_definitions_lines)
    table_definitions_info = {}
    for line in table_definitions_lines:
        table_name, user_data, name = line.split(', ')
        table_name = table_name.strip()
        user_data = user_data.split(': ')[1].strip() == 'TRUE'
        name = name.split(': ')[1].strip() == 'TRUE'
        table_definitions_info[table_name] = {'USERDATA': user_data, 'NAME': name}

    chat_result = ""
    for table_name, definitions in table_definitions_info.items():
        if definitions['NAME']:
            for relation_table, properties in table_info.items():
                if definitions['USERDATA']:
                    selected_relation = relation_table
                    selected_tables = [table_name]
                    system_template: str = (
                        "Don't repeat 'SELECT * FROM Table1'; consider alternatives and consider the database type. "
                        "Respond precisely to USER's"
                        "request. Target columns for first/last names, even if not labeled, formatted as: Firstname: "
                        "[Firstname], Lastname: [Lastname]."
                        "For multiple queries on the same table, use nested queries or different tables, e.g., "
                        "'SELECT A.* FROM table_A WHERE A.id IN (SELECT B.id FROM table_B WHERE B.tag = 'chair');'."
                        "Include 'WHERE' clause for first/last names, e.g., 'SELECT * FROM table_name WHERE "
                        "column_name_for_first_name = [Firstname] AND column_name_for_last_name = [Lastname];'. No "
                        "duplicates on the same table. And most importantly dont write anything but the select query "
                        "because that would ruint the mood"
                    )

                    system_message = system_template.replace("[Firstname]", first_name)
                    system_message = system_message.replace("[Lastname]", last_name)

                    prompt_template: str = (
                        "TABLES for the RELATION: [TABLES] /n RELATION: [RELATIONS] /n CHECK DUPLICATED: [CHATRESULTS]")

                    selected_tables_str = ', '.join(selected_tables)
                    prompt_message = prompt_template.replace("[TABLES]", selected_tables_str)
                    prompt_message = prompt_message.replace("[RELATIONS]", selected_relation)
                    prompt_message = prompt_message.replace("[CHATRESULTS]", chat_result)

                    chat_result += "\n" + chat(system_message, prompt_message)
                    break

    return chat_result


def process_tables2(table_string):
    tables = json.loads(table_string)
    go_threw = 1
    response_full = ""
    for table_name, table_data in tables.items():
        system_message: str = (
            "Can you give me the Tables and all the attributes of that Table that could contain personal data when I "
            "send you the prompt and list them as shown below and dont say anything like 'Based on the provided..' or "
            "'Please note that' neither befor or after just give the information"
            "[TABLENUMMER]. Tablename1 \n - attributename1 \n - attributename2 \n 2. Tablename2"
            "Please when There is no Personal DATA in A TABLE just give back only 'END' without anything else not "
            "with 2. at the start or some other thing just 'END' because I need it with just 'END'"
        )
        prompt_template: str = (
            "Am schluss de queries bitte kein \n machen IF no USERDATA found give 'END' back just 'END' for real just "
            "'END' and nothing more. USERDATA IS Name, Address, Email-Address, Contact data, Birthdate, "
            "Bank data such as IBAN, Credit Card Number and Social Number such as AHVNumber in Switzerland. And only "
            "give back what I ask for. The Table number given at the end of the prompt should be the number before "
            "the Table Name. TABLE NAME: [TABLENAME] /n RELATION: [TABLEDATA] /n Tablenumber: [TABLENUMMER]")

        prompt_message = prompt_template.replace("[TABLENAME]", table_name)
        table_data_str = '\n'.join([f"{key}: {value}" for key, value in table_data.items()])
        prompt_message = prompt_message.replace("[TABLEDATA]", table_data_str)
        prompt_message = prompt_message.replace("[TABLENUMMER]", str(go_threw))
        system_message = system_message.replace("[TABLENUMMER]", str(go_threw))

        response = chat(system_message, prompt_message)
        if response != "END":
            response_full += "\n" + response
            go_threw = go_threw + 1
    filtered_text = '\n'.join(line for line in response_full.split('\n') if line.strip() not in ('', 'END'))
    print(filtered_text)
    return filtered_text


def split_data(data, max_length=8000):
    chunks = []
    current_chunk = ""
    data = json.loads(data)
    for table_name, table_content in data.items():
        table_string = json.dumps({table_name: table_content})

        if len(current_chunk) + len(table_string) > max_length:
            chunks.append(current_chunk)
            current_chunk = ""

        current_chunk += table_string

    if current_chunk:
        chunks.append(current_chunk)

    return chunks


def format_output(output_text):
    lines = output_text.strip().split('\n')
    formatted_output = []

    for line in lines:
        line = line.strip()
        if line and line not in formatted_output:
            formatted_output.append(line)

    formatted_output_str = '\n'.join(formatted_output)
    return formatted_output_str


def run_query_mysql(host, user, password, database, queries):
    connection = pymysql.connect(host=host, user=user, password=password, database=database)
    cursor = connection.cursor()

    return run_queries(connection, cursor, queries)


def run_query_mssql(host, user, password, database, queries):
    conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={host};DATABASE={database};UID={user};PWD={password}'
    connection = pyodbc.connect(conn_str)
    cursor = connection.cursor()

    return run_queries(connection, cursor, queries)


def run_query_postgresql(host, user, password, database, queries):
    connection = psycopg2.connect(host=host, user=user, password=password, dbname=database)
    cursor = connection.cursor()

    return run_queries(connection, cursor, queries)


def run_queries(connection, cursor, queries):
    tables = []
    try:
        for query in queries:
            cursor.execute(query)
            result = cursor.fetchall()
            if result:
                field_names = [desc[0] for desc in cursor.description]
                table = [dict(zip(field_names, row)) for row in result]
                tables.append(table)
            else:
                tables.append("No results for the query.")
    except Exception as e:
        tables.append(f"Error executing query: {str(e)}")

    cursor.close()
    connection.close()
    return tables
