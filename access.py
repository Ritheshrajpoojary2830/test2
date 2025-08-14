from sshtunnel import SSHTunnelForwarder
import psycopg2
import pandas as pd
import random

_tunnel = None
_conn = None

def start_connection():
    global _tunnel, _conn
    port_random=random.randint(1000, 9999)
    if _tunnel is None:
        _tunnel = SSHTunnelForwarder(
            ('34.100.144.185', 22),
            ssh_username='ubuntu',
            ssh_private_key=r"C:\Users\rithe\Downloads\ritesh_bastion.pem",
            remote_bind_address=('10.1.2.10', 5432),
            local_bind_address=('127.0.0.1', port_random),
            set_keepalive=10000000
        )
        _tunnel.start()
        print("SSH tunnel established.")

    if _conn is None:
        _conn = psycopg2.connect(
            user='rithesh_data',
            password='h7sKY60B',
            database='credresolve2',
            host='127.0.0.1',
            port=port_random
        )
        _conn.autocommit = True
        print("PostgreSQL connection established.")

def run_query(query, params=None):
    if _conn is None:
        raise Exception("Connection not established. Call `start_connection()` first.")

    with _conn.cursor() as cursor:
        cursor.execute(query, params)
        result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        return pd.DataFrame(result, columns=columns)