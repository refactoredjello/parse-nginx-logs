import sqlite3
from sqlite3 import Error

DB_NAME = "nginx_report.db"

#TODO
# improve table types
# create report and print output

def insert_values(conn, log_entry):
    sql = ''' INSERT INTO logs(remote_addr, user, local_time, http_method, path, http_version, status, bytes_sent, \
                               http_referer, user_agent)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) '''
    cursor = conn.cursor()

    try:
        data_tuple = (
            log_entry.remote_addr,
            log_entry.user,
            log_entry.local_time.isoformat(),
            log_entry.request.http_method,
            log_entry.request.path,
            log_entry.request.http_version,
            int(log_entry.status),
            int(log_entry.bytes_sent),
            log_entry.http_referer,
            log_entry.user_agent
        )
        cursor.execute(sql, data_tuple)
        conn.commit()
    except Error as e:
        print(f"Failed to insert log entry: {e}")

    return cursor.lastrowid


def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(f"Successfully connected to SQLite database: {db_file}")
    except Error as e:
        print(e)
    return conn


def create_table(conn):
    try:
        sql_create_logs_table = """
        CREATE TABLE IF NOT EXISTS logs (
            id integer PRIMARY KEY,
            remote_addr text NOT NULL,
            user text,
            local_time text NOT NULL,
            http_method text NOT NULL,
            path text NOT NULL,
            http_version text NOT NULL,
            status integer NOT NULL,
            bytes_sent integer NOT NULL,
            http_referer text,
            user_agent text
        );  
        """
        cursor = conn.cursor()
        cursor.execute(sql_create_logs_table)
        print("Table 'logs' created successfully.")
    except Error as e:
        print(e)


def main():
    conn = create_connection(DB_NAME)

    if conn is not None:
        create_table(conn)
        conn.close()
    else:
        print("Error! Cannot create the database connection.")


if __name__ == '__main__':
    main()