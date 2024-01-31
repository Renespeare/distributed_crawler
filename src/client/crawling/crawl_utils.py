from typing import Any
from urllib.parse import urlparse
import requests
from concurrent.futures import ThreadPoolExecutor
import queue
import sqlite3
from database.database import Database
import datetime


class CrawlUtils:
    """
    Kelas yang berisi fungsi-fungsi utilitas crawler.
    """

    def count_keyword_in_text(self, text: str, keyword: str) -> int:
        """
        Fungsi untuk menghitung keyword yang muncul dalam suatu teks.

        Args:
            text (str): Input teks
            keyword (str): Keyword yang ingin dihitung

        Returns:
            int: Jumlah keyword yang ada di dalam teks
        """
        count_keyword = text.count(keyword)
        return count_keyword

    def is_valid_url(self, url: str) -> bool:
        """
        Fungsi untuk mengecek apakah sebuah URL valid atau tidak.

        Args:
            url (str): URL halaman

        Returns:
            bool: True jika URL valid, False jika tidak
        """
        temp = url.split("#")
        url = temp[0]
        parsed = urlparse(url)
        return parsed.scheme in ["http", "https"] and bool(parsed.netloc)

    def get_page(self, url: str) -> Any:
        """
        Fungsi untuk melakukan permintaan (request) ke URL.

        Args:
            url (str): URL halaman

        Returns:
            Any: Respons dari halaman. None jika error
        """
        try:
            res = requests.get(url, verify=False, timeout=300)
            res.raise_for_status()
            return res
        except Exception as e:
            print(e)
            return

    def running_thread_count(self, futures: list) -> int:
        """
        Fungsi untuk menghitung berapa threads yang sedang berjalan.

        Args:
            futures (list): Kumpulan objek future

        Returns:
            int: Jumlah threads yang sedang berjalan
        """
        r = 0
        for future in futures:
            if future.running():
                r += 1
        print(f"{r} threads running")
        return r

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
        duration_crawl: int,
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
            duration_crawl (int): Durasi crawl untuk satu halaman ini

        Returns:
            int: ID page dari baris yang disimpan
        """
        # db_connection.ping()
        db_cursor = db_connection.cursor()
        query = "INSERT INTO page_information (url, crawl_id, html5, title, description, keywords, content_text, hot_url, size_bytes, model_crawl, duration_crawl) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
        time_duration = datetime.timedelta(seconds=duration_crawl)
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
                str(time_duration),
            ),
        )
        db_connection.commit()
        inserted_id = db_cursor.lastrowid
        db_cursor.close()
        return inserted_id

    def insert_page_form(self, db_connection: sqlite3.Connection, page_id: int, form: str) -> None:
        """
        Fungsi untuk menyimpan form yang ada di halaman web ke dalam database.

        Args:
            db_connection (sqlite3.Connection): Koneksi database MySQL
            page_id (int): ID page dari table page_information
            form (str): Form halaman
        """
        # db_connection.ping()
        db_cursor = db_connection.cursor()
        query = "INSERT INTO `page_forms` (`page_id`, `form`) VALUES (?, ?)"
        db_cursor.execute(query, (page_id, form))
        db_connection.commit()
        db_cursor.close()

    def insert_page_image(self, db_connection: sqlite3.Connection, page_id: int, image: str) -> None:
        """
        Fungsi untuk menyimpan gambar yang ada di halaman web ke dalam database.

        Args:
            db_connection (sqlite3.Connection): Koneksi database MySQL
            page_id (int): ID page dari table page_information
            image (str): Image halaman
        """
        # db_connection.ping()
        db_cursor = db_connection.cursor()
        query = "INSERT INTO `page_images` (`page_id`, `image`) VALUES (?, ?)"
        db_cursor.execute(query, (page_id, image))
        db_connection.commit()
        db_cursor.close()

    def insert_page_linking(self, db_connection: sqlite3.Connection, page_id: int, outgoing_link: str) -> None:
        """
        Fungsi untuk menyimpan url linking yang ada di halaman web ke dalam database.

        Args:
            db_connection (sqlite3.Connection): Koneksi database MySQL
            page_id (int): ID page dari table page_information
            outgoing_link (str): Outgoing link
        """
        # db_connection.ping()
        db_cursor = db_connection.cursor()
        query = "INSERT INTO `page_linking` (`page_id`, `outgoing_link`) VALUES (?, ?)"
        db_cursor.execute(query, (page_id, outgoing_link))
        db_connection.commit()
        db_cursor.close()

    def insert_page_list(self, db_connection: sqlite3.Connection, page_id: int, list: str) -> None:
        """
        Fungsi untuk menyimpan list yang ada di halaman web ke dalam database.

        Args:
            db_connection (sqlite3.Connection): Koneksi database MySQL
            page_id (int): ID page dari table page_information
            list (str): List halaman
        """
        # db_connection.ping()
        db_cursor = db_connection.cursor()
        query = "INSERT INTO `page_list` (`page_id`, `list`) VALUES (?, ?)"
        db_cursor.execute(query, (page_id, list))
        db_connection.commit()
        db_cursor.close()

    def insert_page_script(self, db_connection: sqlite3.Connection, page_id: int, script: str) -> None:
        """
        Fungsi untuk menyimpan script yang ada di halaman web ke dalam database.

        Args:
            db_connection (sqlite3.Connection): Koneksi database MySQL
            page_id (int): ID page dari table page_information
            script (str): Script halaman
        """
        # db_connection.ping()
        db_cursor = db_connection.cursor()
        query = "INSERT INTO `page_scripts` (`page_id`, `script`) VALUES (?, ?)"
        db_cursor.execute(query, (page_id, script))
        
        db_connection.commit()
        db_cursor.close()

    def insert_page_style(self, db_connection: sqlite3.Connection, page_id: int, style: str) -> None:
        """
        Fungsi untuk menyimpan style yang ada di halaman web ke dalam database.

        Args:
            db_connection (sqlite3.Connection): Koneksi database MySQL
            page_id (int): ID page dari table page_information
            style (str): Style halaman
        """
        # db_connection.ping()
        db_cursor = db_connection.cursor()
        query = "INSERT INTO `page_styles` (`page_id`, `style`) VALUES (?, ?)"
        db_cursor.execute(query, (page_id, style))
        db_connection.commit()
        db_cursor.close()

    def insert_page_table(self, db_connection: sqlite3.Connection, page_id: int, table: str) -> None:
        """
        Fungsi untuk menyimpan table yang ada di halaman web ke dalam database.

        Args:
            db_connection (sqlite3.Connection): Koneksi database MySQL
            page_id (int): ID page dari table page_information
            table (str): Table halaman
        """
        # db_connection.ping()
        db_cursor = db_connection.cursor()
        query = "INSERT INTO `page_tables` (`page_id`, `table_str`) VALUES (?, ?)"
        db_cursor.execute(query, (page_id, table))
        db_connection.commit()
        db_cursor.close()

    def set_hot_url(self, db_connection: sqlite3.Connection, id_page: int, hot_link: bool) -> None:
        """
        Fungsi untuk menandakan hot URL pada halaman web ke dalam database.

        Args:
            db_connection (sqlite3.Connection): Koneksi database MySQL
            id_page (int): ID halaman
            hot_link (bool): Hot URL, 1 jika ya, 0 jika tidak
        """
        # db_connection.ping()
        db_cursor = db_connection.cursor()
        query = "UPDATE `page_information` SET `hot_url` = ? WHERE `id_page` = ?"
        db_cursor.execute(query, (hot_link, id_page))
        db_connection.commit()
        db_cursor.close()

    def insert_crawling(
        self, db_connection: sqlite3.Connection, start_urls: str, keyword: str, total_page: int, duration: int
    ) -> int:
        """
        Fungsi untuk menyimpan data crawling yang sudah dilakukan ke dalam database.

        Args:
            db_connection (sqlite3.Connection): Koneksi database MySQL
            start_urls (str): URL awal halaman (dipisahkan dengan koma jika lebih dari satu)
            keyword (str): Keyword yang dipakai
            total_page (int): Jumlah halaman
            duration (int): Total durasi

        Returns:
            int: ID crawling dari baris yang disimpan
        """
        # db_connection.ping()
        db_cursor = db_connection.cursor()
        query = "INSERT INTO `crawling` (`start_urls`, `keyword`, `total_page`, `duration_crawl`) VALUES (?, ?, ?, ?)"
        time_duration = datetime.timedelta(seconds=duration)
        db_cursor.execute(query, (start_urls, keyword, total_page, str(time_duration)))
        inserted_id = db_cursor.lastrowid
        db_connection.commit()
        db_cursor.close()
        return inserted_id

    def update_page_duration_crawl(self, db_connection: sqlite3.Connection, id_page: int, duration_crawl: int) -> None:
        """
        Fungsi untuk menandakan hot URL pada halaman web ke dalam database.

        Args:
            db_connection (sqlite3.Connection): Koneksi database MySQL
            id_page (int): ID halaman
            duration_crawl (int): Durasi crawl untuk satu halaman ini
        """
        # db_connection.ping()
        db_cursor = db_connection.cursor()
        query = "UPDATE `page_information` SET `duration_crawl` = ? WHERE `id_page` = ?"
        time_duration = datetime.timedelta(seconds=duration_crawl)
        db_cursor.execute(query, (str(time_duration), id_page))
        db_connection.commit()
        db_cursor.close()

    def update_crawling(
        self, db_connection: sqlite3.Connection, crawl_id: int, total_page: int, duration_crawl: int
    ) -> None:
        """
        Fungsi untuk memperbarui data crawling ke dalam database.

        Args:
            db_connection (sqlite3.Connection): Koneksi database MySQL
            crawl_id (int): ID crawling
            total_page (int): Jumlah halaman
        """
        # db_connection.ping()
        db_cursor = db_connection.cursor()
        query = "UPDATE `crawling` SET `total_page` = ?, `duration_crawl` = ? WHERE `id_crawling` = ?"
        time_duration = datetime.timedelta(seconds=duration_crawl)
        db_cursor.execute(query, (total_page, str(time_duration), crawl_id))
        db_connection.commit()
        db_cursor.close()

    def get_visited_urls(self, db_connection: sqlite3.Connection) -> list:
        """
        Fungsi untuk mendapatkan kumpulan URL yang sudah pernah di crawl dari database.

        Args:
            db_connection (sqlite3.Connection): Koneksi database MySQL

        Returns:
            list: Kumpulan URL yang pernah dicrawl
        """
        # db_connection.ping()
        db_cursor = db_connection.cursor()
        query = "SELECT url FROM `page_information`"
        db_cursor.execute(query)
        row_arr = []
        for row in db_cursor.fetchall():
            row_arr.append(row[0])
        db_connection.commit()
        db_cursor.close()
        return row_arr

    def get_page_information_by_ids(self, id_pages: list) -> list:
        """
        Fungsi untuk mendapatkan informasi halaman dari id page (API).

        Args:
            id_pages (list): Kumpulan ID page

        Returns:
            Returns:
            list: List berisi dictionary page information yang didapatkan dari fungsi cursor.fetchall(), berisi empty list jika tidak ada datanya
        """
        db = Database()
        db_connection = db.connect()
        db_connection.row_factory = sqlite3.Row
        db_cursor = db_connection.cursor()
        # db_cursor = db_connection.cursor(sqlite3.cursors.DictCursor)
        id_pages_string = ",".join(map(str, id_pages))
        query = "SELECT * FROM `page_information` WHERE id_page IN ({})".format(id_pages_string)
        db_cursor.execute(query)
        rows = db_cursor.fetchall()
        db_connection.commit()
        db_cursor.close()
        db.close_connection(db_connection)
        result = [dict(row) for row in rows]
        return result

    def get_crawled_pages_api(self, start=None, length=None) -> list:
        """
        Fungsi untuk mendapatkan halaman-halaman yang sudah dicrawl (API).

        Args:
            start (int): Indeks awal (optional, untuk pagination)
            length (int): Total data (optional, untuk pagination)

        Returns:
            list: List berisi dictionary page information yang didapatkan dari fungsi cursor.fetchall(), berisi empty list jika tidak ada datanya
        """
        db = Database()
        db_connection = db.connect()
        db_connection.row_factory = sqlite3.Row
        # db_cursor = db_connection.cursor(sqlite3.cursors.DictCursor)
        db_cursor = db_connection.cursor()
        if start is None or length is None:
            db_cursor.execute("SELECT * FROM `page_information`")
        else:
            db_cursor.execute("SELECT * FROM `page_information` LIMIT ?, ?", (start, length))
        rows = db_cursor.fetchall()
        db_connection.commit()
        db_cursor.close()
        db.close_connection(db_connection)
        result = [dict(row) for row in rows]
        return result

    def start_insert_api(self, start_urls: str, keyword: str, duration_crawl: int) -> int:
        """
        Fungsi untuk start crawling dan mendapatkan crawl id (API).

        Args:
            start_urls (str): URL awal halaman (dipisahkan dengan koma jika lebih dari satu)
            keyword (str): Keyword yang dipakai
            duration_crawl (int): Total durasi

        Returns:
            int: ID crawling dari baris yang disimpan
        """
        db = Database()
        db_connection = db.connect()
        id_crawling = self.insert_crawling(db_connection, start_urls, keyword, 0, duration_crawl)
        db.close_connection(db_connection)
        return id_crawling

    def insert_page_api(
        self,
        page_information: dict,
        page_forms: dict,
        page_images: dict,
        page_linking: dict,
        page_list: dict,
        page_scripts: dict,
        page_styles: dict,
        page_tables: dict,
    ) -> None:
        """
        Fungsi untuk menambahkan page yang sudah dicrawl (API).

        Args:
            page_information (dict): Page information
            page_forms (list): Page forms
            page_images (list): Page images
            page_linking (list): Page linking
            page_list (list): Page list
            page_scripts (list): Page scripts
            page_styles (list): Page styles
            page_tables (list): Page tables
        """
        db = Database()
        db_connection = db.connect()
        if db.check_value_in_table(db_connection, "page_information", "url", page_information["url"]):
            db.close_connection(db_connection)
            return

        page_id = self.insert_page_information(
            db_connection,
            page_information["url"],
            page_information["crawl_id"],
            page_information["html5"],
            page_information["title"],
            page_information["description"],
            page_information["keywords"],
            page_information["content_text"],
            page_information["hot_url"],
            page_information["size_bytes"],
            page_information["model_crawl"],
            page_information["duration_crawl"],
        )
        for page_form in page_forms:
            self.insert_page_form(db_connection, page_id, page_form["form"])
        for page_image in page_images:
            self.insert_page_image(db_connection, page_id, page_image["image"])
        for page_linking in page_linking:
            self.insert_page_linking(db_connection, page_id, page_linking["outgoing_link"])
        for page_list_ in page_list:
            self.insert_page_list(db_connection, page_id, page_list_["list"])
        for page_script in page_scripts:
            self.insert_page_script(db_connection, page_id, page_script["script"])
        for page_style in page_styles:
            self.insert_page_style(db_connection, page_id, page_style["style"])
        for page_table in page_tables:
            self.insert_page_table(db_connection, page_id, page_table["table_str"])
        db.close_connection(db_connection)


class CustomThreadPoolExecutor(ThreadPoolExecutor):
    """
    Kelas yang inherit dari ThreadPoolExecutor untuk menambahkan metode custom shutdown.
    Referensi: https://tiewkh.github.io/blog/python-thread-pool-executor/
    """

    def shutdown39(self, wait=True, *, cancel_futures=False):
        with self._shutdown_lock:
            self._shutdown = True
            if cancel_futures:
                # Drain all work items from the queue, and then cancel their
                # associated futures.
                while True:
                    try:
                        work_item = self._work_queue.get_nowait()
                    except queue.Empty:
                        break
                    if work_item is not None:
                        work_item.future.cancel()

            # Send a wake-up to prevent threads calling
            # _work_queue.get(block=True) from permanently blocking.
            self._work_queue.put(None)
        if wait:
            for t in self._threads:
                t.join()