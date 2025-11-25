import psycopg2
from config import database
from config.loader import ModelLoader
from setup.check_user import ensure_user
from setup.check_database import ensure_database
from setup.check_schema import sync_schema
from config.database import DatabaseConnection

def run_setup():
    # 1. Cargar el modelo JSON
    loader = ModelLoader("model/model.json")
    model = loader.load()

    db_name = model["database"]
    db_user = model["user"]
    db_pass = model["password"]
    db_host = model["host"]
    db_port = model["port"]

    # ------------------------------------------------
    # 2. Conexión como administrador
    # ------------------------------------------------
    database = DatabaseConnection(db_host, db_port)
    admin_conn = database.get_connection()

    # Crear/actualizar usuario
    ensure_user(admin_conn, db_user, db_pass)

    # Crear DB si no existe
    ensure_database(admin_conn, db_name, db_user)

    admin_conn.close()

    # ------------------------------------------------
    # 3. Conectar a la base de datos final
    # ------------------------------------------------
    
    database = DatabaseConnection(db_host, db_port, db_name, db_user, db_pass, False)
    conn = database.get_connection()

    # Sincronizar tablas
    sync_schema(conn, model["classes"])

    conn.close()
    print("\n🎉 ESQUEMA COMPLETO SINCRONIZADO CON ÉXITO")
