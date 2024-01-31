from typing import Any
# import pymysql
import sqlite3
# import os
from sqlite3 import Error


class Database:
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
        connection = sqlite3.connect("database/crawl_1.db")
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
                "DELETE FROM `tfidf`",
            )
            self.exec_query(
                connection,
                "DELETE FROM `tfidf_word`",
            )
            self.exec_query(
                connection,
                "DELETE FROM `pagerank_changes`",
            )
            self.exec_query(
                connection,
                "DELETE FROM `pagerank`",
            )
            self.exec_query(
                connection,
                "DELETE FROM `page_linking`",
            )
            self.exec_query(
                connection,
                "DELETE FROM `page_images`",
            )
            self.exec_query(
                connection,
                "DELETE FROM `page_tables`",
            )
            self.exec_query(
                connection,
                "DELETE FROM `page_styles`",
            )
            self.exec_query(
                connection,
                "DELETE FROM `page_scripts`",
            )
            self.exec_query(
                connection,
                "DELETE FROM `page_list`",
            )
            self.exec_query(
                connection,
                "DELETE FROM `page_forms`",
            )
            self.exec_query(
                connection,
                "DELETE FROM `page_information`",
            )
            self.exec_query(
                connection,
                "DELETE FROM `crawling`",
            )
        except:
            return

        self.close_connection(connection)

    def create_tables(self):
        """
        Fungsi untuk membuat tabel-tabel yang diperlukan di database.
        """
        connection = self.connect()

        try:
            self.exec_query(
                connection,
                """CREATE TABLE IF NOT EXISTS crawling (
                    id_crawling INTEGER PRIMARY KEY AUTOINCREMENT,
                    start_urls TEXT,
                    keyword TEXT,
                    total_page INTEGER,
                    duration_crawl TIME,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )""",
            )
            self.exec_query(
                connection,
                """CREATE TABLE IF NOT EXISTS page_information (
                    id_page INTEGER PRIMARY KEY AUTOINCREMENT,
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
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (crawl_id) REFERENCES crawling (id_crawling) ON DELETE CASCADE
                )""",
            )
            self.exec_query(
                connection,
                """CREATE TABLE IF NOT EXISTS page_linking (
                    id_linking INTEGER PRIMARY KEY AUTOINCREMENT,
                    page_id INTEGER,
                    outgoing_link TEXT,
                    FOREIGN KEY (page_id) REFERENCES page_information (id_page) ON DELETE CASCADE
                )""",
            )
            self.exec_query(
                connection,
                """CREATE TABLE IF NOT EXISTS page_images (
                    id_image INTEGER PRIMARY KEY AUTOINCREMENT,
                    page_id INTEGER,
                    image TEXT,
                    FOREIGN KEY (page_id) REFERENCES page_information (id_page) ON DELETE CASCADE
                )""",
            )
            self.exec_query(
                connection,
                """CREATE TABLE IF NOT EXISTS page_tables (
                    id_table INTEGER PRIMARY KEY AUTOINCREMENT,
                    page_id INTEGER,
                    table_str TEXT,
                    FOREIGN KEY (page_id) REFERENCES page_information (id_page) ON DELETE CASCADE
                )""",
            )
            self.exec_query(
                connection,
                """CREATE TABLE IF NOT EXISTS page_styles (
                    id_style INTEGER PRIMARY KEY AUTOINCREMENT,
                    page_id INTEGER,
                    style TEXT,
                    FOREIGN KEY (page_id) REFERENCES page_information (id_page) ON DELETE CASCADE
                )""",
            )
            self.exec_query(
                connection,
                """CREATE TABLE IF NOT EXISTS page_scripts (
                    id_script INTEGER PRIMARY KEY AUTOINCREMENT,
                    page_id INTEGER,
                    script TEXT,
                    FOREIGN KEY (page_id) REFERENCES page_information (id_page) ON DELETE CASCADE
                )""",
            )
            self.exec_query(
                connection,
                """CREATE TABLE IF NOT EXISTS page_list (
                    id_list INTEGER PRIMARY KEY AUTOINCREMENT,
                    page_id INTEGER,
                    list TEXT,
                    FOREIGN KEY (page_id) REFERENCES page_information (id_page) ON DELETE CASCADE
                )""",
            )
            self.exec_query(
                connection,
                """CREATE TABLE IF NOT EXISTS page_forms (
                    id_form INTEGER PRIMARY KEY AUTOINCREMENT,
                    page_id INTEGER,
                    form TEXT,
                    FOREIGN KEY (page_id) REFERENCES page_information (id_page) ON DELETE CASCADE
                )""",
            )
            self.exec_query(
                connection,
                """CREATE TABLE IF NOT EXISTS tfidf (
                    id_tfidf INTEGER PRIMARY KEY AUTOINCREMENT,
                    keyword TEXT,
                    page_id INTEGER,
                    tfidf_total DOUBLE,
                    FOREIGN KEY (page_id) REFERENCES page_information (id_page) ON DELETE CASCADE
                )""",
            )
            self.exec_query(
                connection,
                """CREATE TABLE IF NOT EXISTS tfidf_word (
                    id_word INTEGER PRIMARY KEY AUTOINCREMENT,
                    word TEXT,
                    page_id INTEGER,
                    tfidf_score DOUBLE,
                    FOREIGN KEY (page_id) REFERENCES page_information (id_page) ON DELETE CASCADE
                )""",
            )
            self.exec_query(
                connection,
                """CREATE TABLE IF NOT EXISTS pagerank (
                    id_pagerank INTEGER PRIMARY KEY AUTOINCREMENT,
                    page_id INTEGER,
                    pagerank_score DOUBLE,
                    FOREIGN KEY (page_id) REFERENCES page_information (id_page) ON DELETE CASCADE
                )""",
            )
            self.exec_query(
                connection,
                """CREATE TABLE IF NOT EXISTS pagerank_changes (
                    id_change INTEGER PRIMARY KEY AUTOINCREMENT,
                    page_id INTEGER,
                    iteration INTEGER,
                    pagerank_change DOUBLE,
                    FOREIGN KEY (page_id) REFERENCES page_information (id_page) ON DELETE CASCADE
                )""",
            )

        except:
            return

        """
        Fungsi untuk membuat tabel-tabel yang diperlukan di database.
        """
        connection = self.connect()

        try:
            self.exec_query(
                connection,
                """CREATE TABLE crawling (
                    id_crawling INTEGER PRIMARY KEY AUTOINCREMENT,
                    start_urls TEXT,
                    keyword TEXT,
                    total_page INTEGER,
                    duration_crawl TEXT,
                    created_at TIMESTAMP
                )""",
            )
            #time
            self.exec_query(
                connection,
                """CREATE TABLE page_information (
                    id_page INTEGER PRIMARY KEY AUTOINCREMENT,
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
                    duration_crawl TEXT,
                    created_at TIMESTAMP
                )""",
            )
            self.exec_query(
                connection,
                """CREATE TABLE page_linking (
                    id_linking INTEGER PRIMARY KEY AUTOINCREMENT,
                    page_id INTEGER,
                    outgoing_link TEXT
                )""",
            )
            self.exec_query(
                connection,
                """CREATE TABLE page_images (
                    id_image INTEGER PRIMARY KEY AUTOINCREMENT,
                    page_id INTEGER,
                    image TEXT
                )""",
            )
            self.exec_query(
                connection,
                """CREATE TABLE page_tables (
                    id_table INTEGER PRIMARY KEY AUTOINCREMENT,
                    page_id INTEGER,
                    table_str TEXT
                )""",
            )
            self.exec_query(
                connection,
                """CREATE TABLE page_styles (
                    id_style INTEGER PRIMARY KEY AUTOINCREMENT,
                    page_id INTEGER,
                    style TEXT
                )""",
            )
            self.exec_query(
                connection,
                """CREATE TABLE page_scripts (
                    id_script INTEGER PRIMARY KEY AUTOINCREMENT,
                    page_id INTEGER,
                    script TEXT
                )""",
            )
            self.exec_query(
                connection,
                """CREATE TABLE page_list (
                    id_list INTEGER PRIMARY KEY AUTOINCREMENT,
                    page_id INTEGER,
                    list TEXT
                )""",
            )
            self.exec_query(
                connection,
                """CREATE TABLE page_forms (
                    id_form INTEGER PRIMARY KEY AUTOINCREMENT,
                    page_id INTEGER,
                    form TEXT
                )""",
            )
            self.exec_query(
                connection,
                """CREATE TABLE tfidf (
                    id_tfidf INTEGER PRIMARY KEY AUTOINCREMENT,
                    keyword TEXT,
                    page_id INTEGER,
                    tfidf_total DOUBLE
                )""",
            )
            self.exec_query(
                connection,
                """CREATE TABLE tfidf_word (
                    id_word INTEGER PRIMARY KEY AUTOINCREMENT,
                    word TEXT,
                    page_id INTEGER,
                    tfidf_score DOUBLE
                )""",
            )
            self.exec_query(
                connection,
                """CREATE TABLE pagerank (
                    id_pagerank INTEGER PRIMARY KEY AUTOINCREMENT,
                    page_id INTEGER,
                    pagerank_score DOUBLE
                )""",
            )
            self.exec_query(
                connection,
                """CREATE TABLE pagerank_changes (
                    id_change INTEGER PRIMARY KEY AUTOINCREMENT,
                    page_id INTEGER,
                    iteration INTEGER,
                    pagerank_change DOUBLE
                )""",
            )
            # self.exec_query(
            #     connection,
            #     """ALTER TABLE page_information
            #     ADD CONSTRAINT pageinfo_crawl
            #     FOREIGN KEY (crawl_id)
            #     REFERENCES crawling (id_crawling)
            #     ON DELETE CASCADE""",
            # )
            # self.exec_query(
            #     connection,
            #     """ALTER TABLE page_linking
            #     ADD CONSTRAINT pagelink_pageinfo
            #     FOREIGN KEY (page_id)
            #     REFERENCES page_information (id_page)
            #     ON DELETE CASCADE""",
            # )
            # self.exec_query(
            #     connection,
            #     """ALTER TABLE page_images
            #     ADD CONSTRAINT pageimage_pageinfo
            #     FOREIGN KEY (page_id)
            #     REFERENCES page_information (id_page)
            #     ON DELETE CASCADE""",
            # )
            # self.exec_query(
            #     connection,
            #     """ALTER TABLE page_tables
            #     ADD CONSTRAINT pagetable_pageinfo
            #     FOREIGN KEY (page_id)
            #     REFERENCES page_information (id_page)
            #     ON DELETE CASCADE""",
            # )
            # self.exec_query(
            #     connection,
            #     """ALTER TABLE page_styles
            #     ADD CONSTRAINT pagestyle_pageinfo
            #     FOREIGN KEY (page_id)
            #     REFERENCES page_information (id_page)
            #     ON DELETE CASCADE""",
            # )
            # self.exec_query(
            #     connection,
            #     """ALTER TABLE page_scripts
            #     ADD CONSTRAINT pagescript_pageinfo
            #     FOREIGN KEY (page_id)
            #     REFERENCES page_information (id_page)
            #     ON DELETE CASCADE""",
            # )
            # self.exec_query(
            #     connection,
            #     """ALTER TABLE page_list
            #     ADD CONSTRAINT pagelist_pageinfo
            #     FOREIGN KEY (page_id)
            #     REFERENCES page_information (id_page)
            #     ON DELETE CASCADE""",
            # )
            # self.exec_query(
            #     connection,
            #     """ALTER TABLE page_forms
            #     ADD CONSTRAINT pageform_pageinfo
            #     FOREIGN KEY (page_id)
            #     REFERENCES page_information (id_page)
            #     ON DELETE CASCADE""",
            # )
            # self.exec_query(
            #     connection,
            #     """ALTER TABLE tfidf
            #     ADD CONSTRAINT tfidf_pageinfo
            #     FOREIGN KEY (page_id)
            #     REFERENCES page_information (id_page)
            #     ON DELETE CASCADE""",
            # )
            # self.exec_query(
            #     connection,
            #     """ALTER TABLE tfidf_word
            #     ADD CONSTRAINT tfidfw_pageinfo
            #     FOREIGN KEY (page_id)
            #     REFERENCES page_information (id_page)
            #     ON DELETE CASCADE""",
            # )
            # self.exec_query(
            #     connection,
            #     """ALTER TABLE pagerank
            #     ADD CONSTRAINT pagerank_pageinfo
            #     FOREIGN KEY (page_id)
            #     REFERENCES page_information (id_page)
            #     ON DELETE CASCADE""",
            # )
            # self.exec_query(
            #     connection,
            #     """ALTER TABLE pagerank_changes
            #     ADD CONSTRAINT pagerankc_pageinfo
            #     FOREIGN KEY (page_id)
            #     REFERENCES page_information (id_page)
            #     ON DELETE CASCADE""",
            # )
        except:
            return


        self.close_connection(connection)