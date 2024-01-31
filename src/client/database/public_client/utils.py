import datetime
import sqlite3

class DatabaseUtils:
    def insert_page_information(
            self,
            db_connection: sqlite3.Connection,
            url: str,
            crawl_id: int,
            html5: bool,
            title: str,
            description: str,
            keywords: str,
            content_text: str,
            hot_url: bool,
            size_bytes: int,
            model_crawl: str,
            duration_crawl: str,
        ) -> None:
            """
            Fungsi untuk menyimpan konten seperti teks, judul, deskripsi yang ada di halaman web ke dalam database.

            Args:
                db_connection (sqlite3.Connection): Koneksi database MySQL
                url (str): URL halaman
                crawl_id (int): ID crawling
                html5 (bool): Versi html5, 1 jika ya, 0 jika tidak
                title (str): Judul halaman
                description (str): Deskripsi  halaman
                keywords (str): Keyword halaman
                content_text (str): Konten teks halaman
                hot_url (bool): Hot URL, 1 jika ya, 0 jika tidak
                size_bytes (int): Ukuran halaman dalam bytes
                model_crawl (str): Model crawling yaitu BFS atau MSB
                duration_crawl (str): Durasi crawl untuk satu halaman ini

            Returns:
                int: ID page dari baris yang disimpan
            """
            # db_connection.ping()
            db_cursor = db_connection.cursor()
            query = "INSERT INTO page_information (url, crawl_id, html5, title, description, keywords, content_text, hot_url, size_bytes, model_crawl, duration_crawl) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
            # time_duration = datetime.timedelta(seconds=duration_crawl)
            db_cursor.execute(
                query,
                ( 
                    url,
                    crawl_id,
                    html5,
                    title,
                    description,
                    keywords,
                    content_text,
                    hot_url,
                    size_bytes,
                    model_crawl,
                    duration_crawl,
                ),
            )
            db_connection.commit()
            inserted_id = db_cursor.lastrowid
            db_cursor.close()
            return inserted_id
        