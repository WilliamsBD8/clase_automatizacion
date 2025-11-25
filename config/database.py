import psycopg2

class DatabaseConnection:
    """
    Clase que maneja un pool de conexiones a PostgreSQL.
    Puede ser usada con 'with DatabaseConnection() as conn:'.
    """

    _connection_pool = None

    def __init__(self, host, port, database = "postgres", user = "postgres", password = "root", autocommit = True):
        self.host        = host
        self.port        = port
        self.database    = database
        self.user        = user
        self.password    = password
        self.autocommit  = autocommit

    def get_connection(self):
        """Obtiene una conexión."""
        conn = psycopg2.connect(
            host        = self.host,
            port        = self.port,
            database    = self.database,
            user        = self.user,
            password    = self.password
        )
        if(self.autocommit):
            conn.autocommit = True
        return conn
