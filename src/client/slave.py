import socket
import json
import zlib

class Slave:
    def __init__(self, current_client=None, client_list=None):
        self.slave_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.queue = []
        
    def append_url(self, urls):
        if (isinstance(urls, list)):
            for url in urls:
                self.queue.append(url)
        else:
            self.queue.append(urls)
        
    def connect_to_client_public(self, address):
        print('connecting to client public')
        self.slave_socket.connect(address)
        
        # Send the unique identifier
        unique_identifier = self.slave_socket
        compressed_identifier = zlib.compress(str(unique_identifier).encode())
        self.slave_socket.send(compressed_identifier + b'\x00\x01\x02\x03\x04\x05')
        return self.slave_socket
    
    def get_slave_socket(self):
        return self.slave_socket
    
    def get_current_queue(self):
        return self.queue

    def remove_url(self, url_crawled):
        if url_crawled in self.queue:
            self.queue.remove(url_crawled)

    def send_outgoing_link(self, outgoing_link, crawled_url=None, duration_crawled_url=None, page_information_data=None):
        max_chunk_size = 65536
        delimiter = b'\x00\x01\x02\x03\x04\x05'

        unique_identifier = self.slave_socket
        compressed_identifier = zlib.compress(str(unique_identifier).encode())

        # Send the unique identifier to the server
        self.slave_socket.send(compressed_identifier + delimiter)
        serialize_queue = ""

        if crawled_url is None:
            if page_information_data is None:
                serialize_queue = {
                    "outgoing_link": outgoing_link,
                }
            else:
                serialize_queue = {
                    "outgoing_link": outgoing_link,
                    "page_information_data": page_information_data
                }
        else:
            if page_information_data is None:
                serialize_queue = {
                    "outgoing_link": outgoing_link,
                    "crawled_url": crawled_url,
                    "duration_crawled_url": duration_crawled_url
                }
            else:
                serialize_queue = {
                    "outgoing_link": outgoing_link,
                    "page_information_data": page_information_data,
                    "crawled_url": crawled_url,
                    "duration_crawled_url": duration_crawled_url
                }

        # Serialize the JSON object
        json_str = json.dumps(serialize_queue)

        # Compress the serialized JSON data
        compressed_data = zlib.compress(json_str.encode())

        # Send the compressed data in smaller chunks
        for i in range(0, len(compressed_data), max_chunk_size):
            chunk = compressed_data[i:i + max_chunk_size]
            if i + max_chunk_size >= len(compressed_data):
                # If it's the last chunk, add a newline delimiter
                self.slave_socket.send(chunk + delimiter)
            else:
                self.slave_socket.send(chunk)
                # zlib zstd

