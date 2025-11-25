class DBInspector:
    def __init__(self, conn):
        self.conn = conn

    def table_exists(self, table):
        cur = self.conn.cursor()
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = %s
            );
        """, (table,))
        exists = cur.fetchone()[0]
        cur.close()
        return exists

    def get_columns(self, table):
        cur = self.conn.cursor()
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s;
        """, (table,))
        cols = [row[0] for row in cur.fetchall()]
        cur.close()
        return cols
