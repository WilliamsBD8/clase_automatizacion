from .type_mapper import TypeMapper
import re

class Migrator:
    def __init__(self, conn):
        self.conn = conn
        self.mapper = TypeMapper()

    # --------------------------------------------
    # Normalizar data_type de PostgreSQL → tipo corto
    # --------------------------------------------
    def normalize_pg_type(self, t):
        t = t.lower()

        if t.startswith("character varying"):
            return "varchar"
        if t.startswith("timestamp"):
            return "timestamp"
        if t.startswith("integer"):
            return "integer"
        if t.startswith("boolean"):
            return "boolean"
        if t.startswith("numeric"):
            return "numeric"
        if t.startswith("double precision"):
            return "double"
        if t.startswith("real"):
            return "real"
        if t.startswith("date"):
            return "date"
        return t  # fallback

    # --------------------------------------------
    # Normalizar tipo esperado desde el JSON
    # --------------------------------------------
    def normalize_expected_type(self, t):
        t = t.lower()

        if "varchar" in t:
            return "varchar"
        if "timestamp" in t:
            return "timestamp"
        if "int" == t or t.startswith("int"):
            return "integer"
        if "bool" in t:
            return "boolean"
        if "numeric" in t or "decimal" in t:
            return "numeric"
        if "date" == t:
            return "date"
        return t

    # --------------------------------------------
    # Obtener columnas actuales
    # --------------------------------------------
    def get_existing_columns(self, table):
        cur = self.conn.cursor()
        cur.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = %s
        """, (table,))
        cols = {row[0]: self.normalize_pg_type(row[1]) for row in cur.fetchall()}
        cur.close()
        return cols

    def foreign_key_exists(self, table, constraint_name):
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT 1 
                FROM information_schema.table_constraints 
                WHERE table_name = %s 
                AND constraint_name = %s 
                AND constraint_type = 'FOREIGN KEY'
            """, (table, constraint_name))
            return cur.fetchone() is not None

    # --------------------------------------------
    # Crear columnas faltantes
    # --------------------------------------------
    def add_missing_columns(self, table, attributes, references):
        cur = self.conn.cursor()

        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s
        """, (table,))
        existing_cols = [c[0] for c in cur.fetchall()]

        refs_dict = {r["campo_origen"]: r for r in references}

        for attr in attributes:
            col = attr["name"]

            if col not in existing_cols:
                col_type = self.mapper.map(attr)
                print(f"➕ Agregando columna {col} a {table}")
                cur.execute(f'''
                    ALTER TABLE {table}
                    ADD COLUMN {col} {col_type};
                ''')

            # Agregar FK si aplica
            if col in refs_dict:
                ref = refs_dict[col]
                fk_name = f"fk_{table}_{col}_{ref['tabla_destino']}"
                if self.foreign_key_exists(table, fk_name):
                    print(f"⚠️ Foreign key {fk_name} ya existe — saltando.")
                    continue

                print(f"🔗 Agregando FK: {col} → {ref['tabla_destino']}.{ref['campo_destino']}")
                cur.execute(f'''
                    ALTER TABLE {table}
                    ADD CONSTRAINT {fk_name}
                    FOREIGN KEY ({col})
                    REFERENCES {ref['tabla_destino']}({ref['campo_destino']})
                    ON DELETE CASCADE;
                ''')

        self.conn.commit()
        cur.close()

    # --------------------------------------------
    # Eliminar columnas sobrantes
    # --------------------------------------------
    def remove_deleted_columns(self, table, attributes, references):
        cur = self.conn.cursor()

        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s
        """, (table,))
        existing_cols = [c[0] for c in cur.fetchall()]

        expected = [a["name"] for a in attributes]

        to_delete = [c for c in existing_cols if c not in expected]

        for col in to_delete:
            print(f"🗑 Eliminando columna sobrante {col} en {table}")

            cur.execute(f'''
                ALTER TABLE {table}
                DROP COLUMN {col} CASCADE;
            ''')

        self.conn.commit()
        cur.close()

    # --------------------------------------------
    # CAST automático seguro
    # --------------------------------------------
    def get_safe_cast(self, old_type, new_type, col):
        base_new = new_type.lower().split("(")[0]

        CAST_MAP = {
            ("varchar", "boolean"): f"({col} = 'true' OR {col} = '1')",
            ("text", "boolean"): f"({col} = 'true' OR {col} = '1')",
            ("integer", "boolean"): f"({col} = 1)",

            ("boolean", "integer"): f"CASE WHEN {col} THEN 1 ELSE 0 END",

            ("varchar", "integer"): f"NULLIF({col}, '')::integer",
            ("text", "integer"): f"NULLIF({col}, '')::integer",

            # 🔥 NUEVO: boolean → string
            # ("boolean", "varchar"): f"''",
            # ("boolean", "text"): f"''",
        }

        print(f"old_type: {old_type} y base_new {base_new}, {CAST_MAP.get((old_type, base_new))}")

        return CAST_MAP.get((old_type, base_new))

    # --------------------------------------------
    # Actualizar tipos de columnas
    # --------------------------------------------
    def update_modified_columns(self, table, attributes):
        existing = self.get_existing_columns(table)
        cur = self.conn.cursor()

        for attr in attributes:
            col = attr["name"]
            expected_raw = self.mapper.map(attr)

            expected_norm = self.normalize_expected_type(expected_raw)

            if col not in existing:
                continue

            current = existing[col]

            # Si ya son iguales: NO cambiar
            if current == expected_norm:
                continue

            print(f"🛠 Actualizando tipo de {table}.{col}: {current} → {expected_norm}")

            safe_cast = self.get_safe_cast(current, expected_norm, col)

            print(f"""Safe cats: {safe_cast}""")
            if safe_cast:
                cur.execute(f"""
                    ALTER TABLE {table}
                    ALTER COLUMN {col} TYPE {expected_raw}
                    USING {safe_cast};
                """)
            else:
                cur.execute(f"""
                    ALTER TABLE {table}
                    ALTER COLUMN {col} TYPE {expected_raw};
                """)
                # raise Exception(
                #     f"No se puede convertir {table}.{col} de {current} → {expected_raw}. "
                #     "Agrega un USING manual."
                # )

        self.conn.commit()
        cur.close()
