from generator.table_builder import TableBuilder
from generator.inspector import DBInspector
from generator.migrator import Migrator

def sync_schema(connection, classes):
    inspector = DBInspector(connection)
    migrator = Migrator(connection)
    builder = TableBuilder()

    for cls in classes:
        table = cls["class"]
        attrs = cls["attributes"]
        references = cls.get("references", [])

        if not inspector.table_exists(table):
            sql = builder.generate_table_sql(cls)
            cur = connection.cursor()
            cur.execute(sql)
            connection.commit()
            cur.close()
            print(f"✔ Tabla creada: {table}")
        else:
            print(f"↻ Tabla existente: {table}")
            print(f"↻ Verificando columnas nuevas")
            migrator.add_missing_columns(table, attrs, references)

            
            print(f"↻ Verificando columnas eliminadas")
            migrator.remove_deleted_columns(table, attrs, references)
