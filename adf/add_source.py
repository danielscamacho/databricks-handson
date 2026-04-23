# add_source.py
import sqlite3
conn = sqlite3.connect("ingestion_config.db")
conn.execute("""
    INSERT INTO ingestion_config VALUES (
        4, 'todos_api', 1,
        'https://jsonplaceholder.typicode.com', '/todos',
        'GET', NULL,
        'bronze/todos/incoming',
        'full', NULL, NULL,
        'bronze.todos_raw'
    )
""")
conn.commit()
conn.close()
print("Source added.")