def ensure_database(admin_conn, db_name, db_user):
    cur = admin_conn.cursor()
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
    exists = cur.fetchone()

    if not exists:
        cur.execute(f'CREATE DATABASE "{db_name}" OWNER "{db_user}";')
        print(f"✔ Base de datos creada: {db_name}")
    else:
        print(f"✔ Base de datos existente: {db_name}")

    cur.close()
