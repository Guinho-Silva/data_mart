import mysql.connector
from mysql.connector import Error

# Conecção do banco OLTP
def conn_oltp():
    try:
        conn = mysql.connector.connect(
            host = 'localhost',
            database = 'kair_pizzaria',
            user = "root",
            password = "" 
        )
    except Error as erro:
        print(f"Falha ao se conecatar com o banco: {erro}")

    return conn


# Conecção do banco OLAP
def conn_olap():
    try:
        conn = mysql.connector.connect(
            host = 'localhost',
            # database = 'kair_pizzaria',
            database = 'kair_pizzaria_mart_v2',
            user = "root",
            password = "" 
        )
    except Error as erro:
        print(f"Falha ao se conecatar com o banco: {erro}")

    return conn

# if conn.is_connected():
#     db_info = conn.server_info
#     print("conectado ao servidor", db_info)

#     cursor = conn.cursor()

#     cursor.execute("SELECT DATABASE();")

#     linha = cursor.fetchone()
#     print("Conectado ao banco de dados", linha)

# if conn.is_connected():
#     cursor.close()
#     conn.close()
#     print("Conexão MySQL encerrada!")