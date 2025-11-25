from .type_mapper import TypeMapper

class Migrator:
    def __init__(self, conn):
        self.conn = conn
        self.mapper = TypeMapper()

    def add_missing_columns(self, table, attributes, references):
        cur = self.conn.cursor()

        # columnas existentes
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s
        """, (table,))
        existing_cols = [c[0] for c in cur.fetchall()]

        # foreign keys existentes (para no duplicarlas)
        cur.execute("""
            SELECT constraint_name
            FROM information_schema.table_constraints
            WHERE table_name = %s AND constraint_type = 'FOREIGN KEY'
        """, (table,))
        existing_fks = [c[0] for c in cur.fetchall()]

        # Mapear referencias por campo_origen
        refs_dict = {r["campo_origen"]: r for r in references}

        for attr in attributes:
            col = attr["name"]

            # 1. Agregar columna si falta
            if col not in existing_cols:
                col_type = self.mapper.map(attr)
                print(f"➕ Agregando columna {col} a {table}")
                cur.execute(f'''
                    ALTER TABLE {table}
                    ADD COLUMN {col} {col_type};
                ''')

            # 2. Si es FOREIGN KEY, agregar relación si falta
            if col in refs_dict:
                ref = refs_dict[col]
                tabla_destino = ref["tabla_destino"]
                campo_destino = ref["campo_destino"]

                fk_name = f"fk_{table}_{col}_{tabla_destino}"

                if fk_name not in existing_fks:
                    print(f"🔗 Agregando FK: {col} → {tabla_destino}.{campo_destino}")

                    cur.execute(f'''
                        ALTER TABLE {table}
                        ADD CONSTRAINT {fk_name}
                        FOREIGN KEY ({col})
                        REFERENCES {tabla_destino}({campo_destino})
                        ON DELETE CASCADE;
                    ''')

        self.conn.commit()
        cur.close()

    def remove_deleted_columns(self, table, attributes, references):
        cur = self.conn.cursor()

        # columnas actuales en la BD
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s
        """, (table,))
        existing_cols = [c[0] for c in cur.fetchall()]

        # columnas esperadas según el JSON
        expected_cols = [a["name"] for a in attributes]

        # columnas que sobran (están en BD pero NO en JSON)
        to_delete = [c for c in existing_cols if c not in expected_cols]

        # obtener FKs actuales
        cur.execute("""
            SELECT tc.constraint_name, kcu.column_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
            WHERE tc.table_name = %s AND tc.constraint_type = 'FOREIGN KEY'
        """, (table,))
        fk_info = cur.fetchall()  # constraint_name, column_name

        for col in to_delete:
            print(f"🗑 Eliminando columna sobrante: {col} en {table}")

            # 1. Si la columna tiene FK, eliminarla primero
            fks_on_column = [fk for fk in fk_info if fk[1] == col]

            for fk_name, _ in fks_on_column:
                print(f"   🔗 Eliminando restricción FK: {fk_name}")
                cur.execute(f'''
                    ALTER TABLE {table}
                    DROP CONSTRAINT IF EXISTS {fk_name};
                ''')

            # 2. Luego eliminar la columna
            cur.execute(f'''
                ALTER TABLE {table}
                DROP COLUMN {col};
            ''')

        self.conn.commit()
        cur.close()
