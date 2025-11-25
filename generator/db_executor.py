import psycopg2

class DBExecutor:
    def __init__(self, config):
        self.config = config

    def run(self, sql_list):
        conn = psycopg2.connect(
            host=self.config["host"],
            port=self.config["port"],
            database=self.config["database"],
            user=self.config["user"],
            password=self.config["password"],
        )

        cursor = conn.cursor()

        for sql in sql_list:
            try:
                cursor.execute(sql)
            except Exception as e:
                print("\n❌ Error ejecutando SQL:")
                print(sql)
                print("Error:", e)

        conn.commit()
        cursor.close()
        conn.close()

        print("\n✅ Base de datos generada con éxito")
