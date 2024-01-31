from typing import Any
# import pymysql
import sqlite3
# import os
from sqlite3 import Error


class DatabasePublic:
    """
    Kelas yang digunakan untuk melakukan pengoperasian database.
    """

    # def __init__(self) -> None:
    #     self.host = os.getenv("DB_HOST")
    #     self.username = os.getenv("DB_USERNAME")
    #     self.password = os.getenv("DB_PASSWORD")
    #     self.db_name = os.getenv("DB_NAME")
    #     self.db_port = integer(os.getenv("DB_PORT"))

    def connect(self) -> sqlite3.Connection:
        """
        Fungsi untuk melakukan koneksi ke database.

        Returns:
            sqlite3.Connection: Koneksi database MySQL
        """
        connection = sqlite3.connect("database/public_client/crawl.db")
        return connection

    def close_connection(self, connection: sqlite3.Connection) -> None:
        """
        Fungsi untuk menutup koneksi ke database.

        Args:
            connection (sqlite3.Connection): Koneksi database MySQL
        """
        try:
            connection.close()
        except:
            pass

    def check_value_in_table(self, connection: sqlite3.Connection, table_name: str, column_name: str, value: Any):
        """
        Fungsi yang berfungsi untuk mengecek keberadaan suatu nilai di dalam tabel dan kolom.

        Args:
            connection (sqlite3.Connection): Koneksi database MySQL
            table_name (str): Nama tabel
            column_name (str): Nama kolom
            value (Any): Nilai yang ingin dicek

        Returns:
            bool: True jika ada, False jika tidak ada
        """
        # connection.ping()
        db_cursor = connection.cursor()
        db_cursor.execute(
            "SELECT {column}, COUNT(*) FROM {table} WHERE {column} = '{value}' GROUP BY {column}".format(
                table=table_name, column=column_name, value=value
            )
        )
        db_cursor.fetchall()
        row_count = db_cursor.rowcount
        db_cursor.close()
        if row_count < 1:
            return False
        return True

    def count_rows(self, connection: sqlite3.Connection, table_name: str):
        """
        Fungsi untuk menghitung jumlah baris pada tabel.

        Args:
            connection (sqlite3.Connection): Koneksi database MySQL
            table_name (str): Nama tabel

        Returns:
            integer: Jumlah baris dari tabel
        """
        # connection.ping()
        db_cursor = connection.cursor()
        db_cursor.execute("SELECT COUNT(*) FROM {table}".format(table=table_name))
        row_count = db_cursor.fetchone()[0]
        db_cursor.close()
        return row_count

    def exec_query(self, connection: sqlite3.Connection, query: str):
        """
        Fungsi untuk eksekusi query pada database.

        Args:
            connection (sqlite3.Connection): Koneksi database MySQL
            query (str): Kueri MySQL
        """
        # print("HAI")
        # connection.ping()
        try:
            db_cursor = connection.cursor()
            db_cursor.execute(query)
            db_cursor.close()
        except Error as e:
            print(e)

    def truncate_tables(self):
        """
        Fungsi untuk mengosongkan semua table yang ada di database.
        """
        connection = self.connect()

        try:
            self.exec_query(
                connection,
                "DELETE FROM `page_information`",
            )
        except:
            return

        self.close_connection(connection)

    def create_tables(self):
        """
        Fungsi untuk membuat tabel-tabel yang diperlukan di database.
        """
        connection = self.connect()
        # id_page INTEGER PRIMARY KEY AUTOINCREMENT,
        try:
            self.exec_query(
                connection,
                """CREATE TABLE IF NOT EXISTS page_information (
                    crawl_id INTEGER,
                    url TEXT,
                    html5 INTEGER,
                    title TEXT,
                    description TEXT,
                    keywords TEXT,
                    content_text TEXT,
                    hot_url INTEGER,
                    size_bytes BIGINT,
                    model_crawl TEXT,
                    duration_crawl REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )""",
            )
        except:
            return

        """
        Fungsi untuk membuat tabel-tabel yang diperlukan di database.
        """
        connection = self.connect()

        try:
            # self.exec_query(
            #     connection,
            #     """CREATE TABLE crawling (
            #         id_crawling INTEGER PRIMARY KEY AUTOINCREMENT,
            #         start_urls TEXT,
            #         keyword TEXT,
            #         total_page INTEGER,
            #         duration_crawl TEXT,
            #         created_at TIMESTAMP
            #     )""",
            # )
            #time
            # id_page INTEGER PRIMARY KEY AUTOINCREMENT,
            self.exec_query(
                connection,
                """CREATE TABLE page_information (
                    crawl_id INTEGER,
                    url TEXT,
                    html5 INTEGER,
                    title TEXT,
                    description TEXT,
                    keywords TEXT,
                    content_text TEXT,
                    hot_url INTEGER,
                    size_bytes BIGINT,
                    model_crawl TEXT,
                    duration_crawl REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )""",
            )
        except:
            return

        self.close_connection(connection)
        