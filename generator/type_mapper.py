class TypeMapper:
    def map(self, attr):
        t = attr["data_type"]

        if t == "int":
            return "INTEGER"
        if t == "float":
            return "NUMERIC"
        if t == "boolean":
            return "BOOLEAN"
        if t == "timestamp":
            return "TIMESTAMP"

        if t == "string":
            length = attr.get("length", 0)
            return f"VARCHAR({length})" if length > 0 else "TEXT"

        return "TEXT"
