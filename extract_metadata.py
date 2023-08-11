import mysql.connector
import json
import pyodbc
import psycopg2
import pymysql


class CustomJsonEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, bytes):
            return o.decode('utf-8')  # Convert bytes to str
        return super().default(o)


def get_metadata_mssql(mssql_host, mssql_user, mssql_password, mssql_database):
    conn = None
    try:
        conn_string = \
            (f"DRIVER={{ODBC Driver 17 for SQL Server}};"
             f"SERVER={mssql_host};"
             f"DATABASE={mssql_database};"
             f"UID={mssql_user};"
             f"PWD={mssql_password}")
        conn = pyodbc.connect(conn_string)
        cursor = conn.cursor()

        # Get metadata
        cursor.execute("""
            SELECT 
                s.name AS schema_name,
                t.name AS table_name,
                c.name AS column_name,
                CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 'PRI' ELSE '' END AS column_key,
                CASE WHEN c.is_nullable = 1 THEN 'YES' ELSE 'NO' END AS is_nullable,
                ty.name AS column_type
            FROM 
                sys.tables t
            INNER JOIN 
                sys.columns c ON t.object_id = c.object_id
            INNER JOIN 
                sys.schemas s ON t.schema_id = s.schema_id
            LEFT JOIN 
                sys.types ty ON c.system_type_id = ty.system_type_id
            LEFT JOIN (
                SELECT 
                    ku.TABLE_NAME, ku.COLUMN_NAME
                FROM 
                    INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
                INNER JOIN 
                    INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS ku
                    ON tc.CONSTRAINT_TYPE = 'PRIMARY KEY' AND tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
            ) AS pk
            ON  t.name = pk.TABLE_NAME AND c.name = pk.COLUMN_NAME
        """)
        metadata_rows = cursor.fetchall()

        mssql_metadata = {}
        for row in metadata_rows:
            schema_name = row.schema_name
            table_name = row.table_name
            column_name = row.column_name
            primary_key = row.column_key == 'PRI'
            nullable = row.is_nullable == 'YES'
            data_type = row.column_type

            full_table_name = f"{schema_name}.{table_name}"

            if full_table_name not in mssql_metadata:
                mssql_metadata[full_table_name] = {}

            mssql_metadata[full_table_name][column_name] = {
                "type": data_type,
                "pk": primary_key,
                "nullable": nullable
            }

        # Get relations
        cursor.execute("""
            SELECT 
                FK.CONSTRAINT_NAME AS constraint_name,
                FK.TABLE_NAME AS table_name,
                CU.COLUMN_NAME AS column_name,
                PK.TABLE_NAME AS referenced_table_name,
                PT.COLUMN_NAME AS referenced_column_name
            FROM 
                INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS C
            INNER JOIN 
                INFORMATION_SCHEMA.TABLE_CONSTRAINTS FK
                ON C.CONSTRAINT_NAME = FK.CONSTRAINT_NAME
            INNER JOIN 
                INFORMATION_SCHEMA.TABLE_CONSTRAINTS PK
                ON C.UNIQUE_CONSTRAINT_NAME = PK.CONSTRAINT_NAME
            INNER JOIN 
                INFORMATION_SCHEMA.KEY_COLUMN_USAGE CU
                ON C.CONSTRAINT_NAME = CU.CONSTRAINT_NAME
            INNER JOIN (
                SELECT 
                    i1.TABLE_NAME, i2.COLUMN_NAME
                FROM 
                    INFORMATION_SCHEMA.TABLE_CONSTRAINTS i1
                INNER JOIN 
                    INFORMATION_SCHEMA.KEY_COLUMN_USAGE i2
                        ON i1.CONSTRAINT_NAME = i2.CONSTRAINT_NAME
                WHERE 
                    i1.CONSTRAINT_TYPE = 'PRIMARY KEY'
            ) PT
            ON PT.TABLE_NAME = PK.TABLE_NAME
        """)
        relations_rows = cursor.fetchall()

        mssql_relations = {}
        for row in relations_rows:
            constraint_name = row.constraint_name
            table_name = row.table_name
            column_name = row.column_name
            ref_table_name = row.referenced_table_name
            ref_column_name = row.referenced_column_name

            if table_name not in mssql_relations:
                mssql_relations[table_name] = {}

            mssql_relations[table_name][column_name] = {
                "constraint_name": constraint_name,
                "table": ref_table_name,
                "column": ref_column_name
            }

        return mssql_metadata, mssql_relations

    except pyodbc.Error as e:
        print(f"Error: {e}")
        return None, None

    finally:
        if conn:
            conn.close()


def get_metadata_postgresql(postgresql_host, postgresql_user, postgresql_password, postgresql_database):
    conn = []
    try:
        conn = psycopg2.connect(
            host=postgresql_host,
            user=postgresql_user,
            password=postgresql_password,
            dbname=postgresql_database)

        cursor = conn.cursor()

        # Get the metadata
        cursor.execute("""
            SELECT 
                table_name, 
                column_name, 
                column_default, 
                is_nullable, 
                data_type 
            FROM 
                information_schema.columns 
            WHERE 
                table_schema = 'public'
        """)
        rows = cursor.fetchall()

        postgresql_metadata = {}
        for row in rows:
            table_name = row[0]
            column_name = row[1]
            primary_key = 'nextval' in (row[2] or '')
            nullable = row[3] == 'YES'
            data_type = row[4]

            if table_name not in postgresql_metadata:
                postgresql_metadata[table_name] = {}

            postgresql_metadata[table_name][column_name] = {
                "type": data_type,
                "pk": primary_key,
                "nullable": nullable
            }

        # Get the relations
        cursor.execute("""
            SELECT 
                kcu.table_name, 
                kcu.column_name, 
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name
            FROM 
                information_schema.table_constraints AS tc 
            JOIN 
                information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
            JOIN 
                information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
            WHERE 
                constraint_type = 'FOREIGN KEY'
        """)
        rows = cursor.fetchall()

        postgresql_relations = {}
        for row in rows:
            table_name = row[0]
            column_name = row[1]
            ref_table_name = row[2]
            ref_column_name = row[3]

            if table_name not in postgresql_relations:
                postgresql_relations[table_name] = {}

            postgresql_relations[table_name][column_name] = {
                "table": ref_table_name,
                "column": ref_column_name
            }

        return postgresql_metadata, postgresql_relations

    except psycopg2.Error as e:
        print(f"Error: {e}")
        return None, None

    finally:
        if conn:
            conn.close()


def get_metadata_mysql(mysql_host, mysql_user, mysql_password, mysql_database):
    db = []
    try:
        db = pymysql.connect(host=mysql_host, user=mysql_user, password=mysql_password, database=mysql_database)
        cursor = db.cursor()

        # Get the metadata
        cursor.execute(
            """SELECT 
                        TABLE_NAME, 
                        COLUMN_NAME, 
                        COLUMN_KEY, 
                        IS_NULLABLE, 
                        COLUMN_TYPE 
                    FROM 
                        INFORMATION_SCHEMA.COLUMNS 
                    WHERE 
                    TABLE_SCHEMA = %s
                    """, (mysql_database,))
        rows = cursor.fetchall()

        mysql_metadata = {}
        for row in rows:
            table_name = row[0]
            column_name = row[1]
            primary_key = row[2] == 'PRI'
            nullable = row[3] == 'YES'
            data_type = row[4]

            if table_name not in mysql_metadata:
                mysql_metadata[table_name] = {}

            mysql_metadata[table_name][column_name] = {
                "type": data_type,
                "pk": primary_key,
                "nullable": nullable
            }

        # Get the relations
        cursor.execute("""
            SELECT 
                TABLE_NAME, 
                COLUMN_NAME, 
                REFERENCED_TABLE_NAME, 
                REFERENCED_COLUMN_NAME 
            FROM 
                INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
            WHERE 
                TABLE_SCHEMA = %s AND 
                REFERENCED_TABLE_NAME IS NOT NULL
        """, (mysql_database,))
        rows = cursor.fetchall()

        mysql_relations = {}
        for row in rows:
            table_name = row[0]
            column_name = row[1]
            ref_table_name = row[2]
            ref_column_name = row[3]

            if table_name not in mysql_relations:
                mysql_relations[table_name] = {}

            mysql_relations[table_name][column_name] = {
                "table": ref_table_name,
                "column": ref_column_name
            }

        return mysql_metadata, mysql_relations

    except mysql.connector.Error as e:
        print(f"Error: {e}")
        return None, None

    finally:
        if db:
            db.close()
