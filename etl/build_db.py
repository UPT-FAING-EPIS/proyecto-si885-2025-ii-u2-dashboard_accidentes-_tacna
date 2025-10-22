import yaml
from connection import get_connection

def create_tables_from_yaml(yaml_file):
    with open(yaml_file, 'r', encoding='utf-8') as file:
        schema = yaml.safe_load(file)

    conn = get_connection()
    cursor = conn.cursor()

    for table_name, table_data in schema['tables'].items():
        columns = table_data['columns']
        column_defs = ', '.join([f"{col} {dtype}" for col, dtype in columns.items()])
        create_stmt = f"IF OBJECT_ID('{table_name}', 'U') IS NULL CREATE TABLE {table_name} ({column_defs});"

        print(f"🛠️ Creando tabla: {table_name}")
        cursor.execute(create_stmt)
        conn.commit()

    cursor.close()
    conn.close()
    print("✅ Todas las tablas fueron creadas correctamente.")

if __name__ == "__main__":
    create_tables_from_yaml("etl/schema.yml")
