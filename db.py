import sqlite3


class DataBase:
    def __init__(self, db_path=None) -> None:
        if db_path is None:
            db_path = "db.sqlite3"
        self.path = db_path
        self.conn = self.create_connection()

    def create_connection(self):
        conn = sqlite3.connect(self.path)
        print("Connection Established")
        return conn

    def get_connection(self):
        if not self.conn:
            self.conn = self.create_connection()
        return self.conn

    def create_table(self, table_name):
        sql = """
        CREATE TABLE IF NOT EXISTS {table_name}
        (
            NAME VARCHAR(100),
            SKU VARCHAR(100),
            DESCRIPTION TEXT
        )
        """.format(table_name=table_name)
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        self.close_connection(conn)

    def show(self, table_name):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM {table_name} where SKU='lay-raise-best-end' LIMIT 10".format(table_name=table_name))
        for row in cursor.fetchall():
            print(row)
