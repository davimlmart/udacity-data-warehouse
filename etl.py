#%% Imports
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, quality_checks_queries

#%% Script functions

def load_staging_tables(cur, conn):
    """Runs specified copy queries to load the datasets.
    Args:
        cur: connector cursor.
        conn: connector."""
    for query in copy_table_queries:
        print(query)
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(f"Error executing query: {e}")

def insert_tables(cur, conn):
    """Runs specified queries to insert data into tables.
    Args:
        cur: connector cursor.
        conn: connector."""
    for query in insert_table_queries:
        print(query)
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(f"Error executing query: {e}")

def run_quality_checks(cur, conn):
    """Runs specified queries of quality checks.
    Args:
        cur: connector cursor.
        conn: connector."""
    for query in quality_checks_queries:
        print(query)
        try:
            cur.execute(query)
            print(cur.fetchall())
            conn.commit()
        except Exception as e:
            print(f"Error executing query: {e}")   

def main():
    """Create connector, prints IAM role ARN, cluster and database informations, 
    runs functions to load staging talbles and populate analytic tables, runs 
    quality, closes connectors and prints final message."""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print("Role ARN:", config['IAM_ROLE']['ARN'])

    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()

        print("Cluster endpoint:", config['CLUSTER']['HOST'])
        print("PostgreSQL connection details:")
        print("  Host:", config['CLUSTER']['HOST'])
        print("  Database name:", config['CLUSTER']['DB_NAME'])
        print("  User:", config['CLUSTER']['DB_USER'])
        print("  Port:", config['CLUSTER']['DB_PORT'])

        # ETL functions
        load_staging_tables(cur, conn)
        insert_tables(cur, conn)
        run_quality_checks(cur, conn)

    except psycopg2.OperationalError as e:
        print(f"Error: {e}")
        print("Please run create_tables.py before running etl.py.")

    finally:
        if conn:
            cur.close()
            conn.close()

    print('ETL process finished')

if __name__ == "__main__":
    main()
