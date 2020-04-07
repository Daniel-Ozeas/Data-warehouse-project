import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Copy all files from S3 to a staging table in Redshift.
    """
    for query in copy_table_queries:
        print('Copying to stage table:', query)
        cur.execute(query)
        conn.commit()
        print('Copy finished')

def insert_tables(cur, conn):
    """
    Insert all data from staging table in fact and dimensions tables.
    """
    for query in insert_table_queries:
        print('Insert into table', query)
        cur.execute(query)
        conn.commit()
        print('Insert finished')

def main():
    """
    Connect to the cluster and execute the load_staging_tables and insert_tables functions.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)

    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()