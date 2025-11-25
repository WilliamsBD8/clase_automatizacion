import psycopg2

def ensure_user(admin_conn, username, password):
    cur = admin_conn.cursor()

    # Verificar si existe el usuario
    cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (username,))
    exists = cur.fetchone()

    if not exists:
        cur.execute(f'CREATE USER "{username}" WITH PASSWORD %s;', (password,))
        print(f"✔ Usuario creado: {username}")
    else:
        cur.execute(f'ALTER USER "{username}" WITH PASSWORD %s;', (password,))
        print(f"✔ Contraseña actualizada para usuario: {username}")

    cur.close()
