from .type_mapper import TypeMapper

class TableBuilder:
    def __init__(self):
        self.mapper = TypeMapper()

    def generate_table_sql(self, cls):
        table_name = cls["class"]
        attributes = cls["attributes"]
        refs = cls.get("references", [])

        columns = []
        fks = []

        for a in attributes:
            col_name = a["name"]
            col_type = self.mapper.map(a)
            pk = "PRIMARY KEY" if a["primary_key"] == "True" else ""
            autoinc = "GENERATED ALWAYS AS IDENTITY" if a["autoincrement"] == "True" else ""

            col_def = f"{col_name} {col_type} {autoinc} {pk}".strip()
            columns.append(col_def)

        # foreign keys
        for ref in refs:
            fk = (
                f"FOREIGN KEY ({ref['campo_origen']}) "
                f"REFERENCES {ref['tabla_destino']}({ref['campo_destino']}) "
                "ON DELETE CASCADE"
            )
            fks.append(fk)

        all_defs = columns + fks
        columns_sql = ",\n    ".join(all_defs)

        sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {columns_sql}
            );
        """
        return sql
