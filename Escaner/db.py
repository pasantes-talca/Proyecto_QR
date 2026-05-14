import psycopg2
from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

def get_db():
    """Devuelve una conexión a PostgreSQL"""
    if not DB_PASSWORD:
        raise RuntimeError("DB_PASSWORD no configurada en .env")
    
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        client_encoding="WIN1252"
    )
    conn.autocommit = True
    return conn