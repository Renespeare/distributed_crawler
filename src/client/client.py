import socket
import json
import threading
import time
import ipaddress
from dotenv import load_dotenv
import os
from statistics import mean
import zlib
from crawling.crawl import Crawl
import slave
from database.database import Database
from database.public_client.database import DatabasePublic
from database.public_client.utils import DatabaseUtils

# Server configuration
SERVER_ADDRESS = '185.227.134.123' # sesuaikan addressnya
SERVER_PORT = 55555
manager_address = None
private_ip = True
client_public_ready = False
client_public_address = None
start_urls = None


class Client:
    def __init__(self, unique_identifier, socket):
        self.unique_identifier = unique_identifier
        self.socket = socket
        self.queue = []
        self.average_crawling_time = [] # contain list of average 10 url crawling time
        self.health = 100
        self.lock = threading.Lock()
        
    def append_queue(self, urls):
        for url in urls:
            with self.lock:
                self.queue.append(url)
        
    def remove_queue(self, urls):
        with self.lock:
            for url in urls:
                if url in self.queue:
                    self.queue.remove(url)

class LoadBalancer:
    def __init__(self, queue = list):
        self.slave_connections = {}
        self.visited_urls = set()
        self.queue = queue
        self.lock = threading.Lock()
        self.db_public = DatabasePublic()
        self.db_utils = DatabaseUtils()
    
    
    def handle_slave(self, slave_socket):
        data_buffer = b''
        max_chunk_size = 65536
        delimiter = b'\x00\x01\x02\x03\x04\x05'
        db_public_connection = self.db_public.connect()
        try:
            compressed_identifier = slave_socket.recv(max_chunk_size).split(delimiter, 1)[0]
            unique_identifier = zlib.decompress(compressed_identifier).decode()

            # Check if the client is a new slave or an existing one
            if unique_identifier in self.slave_connections:
                print(f"Client with identifier {unique_identifier} is an existing slave.")
            else:
                print(f"Client with identifier {unique_identifier} is a new slave.")
                slave = Client(unique_identifier, slave_socket)
                self.slave_connections[unique_identifier] = slave

            # Send an initial URL to the newly connected slave
            initial_url = self.pop_queue() 
            self.visited_urls.add(initial_url)
            slave.queue.append(initial_url)
            print("init_url", initial_url)
            if initial_url is not None:
                # Serialize and compress the initial URL before sending
                initial_url_data = json.dumps(initial_url).encode()
                compressed_initial_url = zlib.compress(initial_url_data)
                slave_socket.send(compressed_initial_url + delimiter)

        except Exception as e:
            print(f"Error: {str(e)}")

        while True:
            try:
                time.sleep(0.5)
                compressed_identifier = slave_socket.recv(max_chunk_size)
                if not compressed_identifier:
                    break
                compressed_identifier = compressed_identifier.split(delimiter, 1)[0]
                unique_identifier = zlib.decompress(compressed_identifier).decode()
                while True:
                    chunk = slave_socket.recv(max_chunk_size)
                    if not chunk:
                        break

                    data_buffer += chunk
                    while delimiter in data_buffer:
                        chunk, data_buffer = data_buffer.split(delimiter, 1)
                        try:
                            decompressed_data = zlib.decompress(chunk)
                            complete_json = decompressed_data.decode()
                            if str(complete_json) != str(unique_identifier):
                                try:
                                    json_data = json.loads(complete_json)
                                    self.append_queue(json_data["outgoing_link"])
                                    if "crawled_url" in json_data:
                                        slave.remove_queue(json_data["crawled_url"])
                                        average = round(mean(json_data["duration_crawled_url"]), 2)
                                        with self.lock:
                                            slave.average_crawling_time.append(average)
                                            if len(slave.average_crawling_time) > 10:
                                                slave.average_crawling_time.pop(0)
                                    if "page_information_data" in json_data:
                                        page_information_data = json_data["page_information_data"]
                                        with self.lock:
                                            page_id = self.db_utils.insert_page_information(
                                                db_public_connection,
                                                page_information_data["url"],
                                                page_information_data["crawl_id"],
                                                page_information_data["html5"],
                                                page_information_data["title"],
                                                page_information_data["description"],
                                                page_information_data["keywords"],
                                                page_information_data["content_text"],
                                                page_information_data["hot_url"],
                                                page_information_data["size_bytes"],
                                                page_information_data["model_crawl"],
                                                page_information_data["duration_crawl"]
                                            )
                                except json.JSONDecodeError as e:
                                    print(f"Error decoding JSON: {e}")

                        except zlib.error as e:
                            print(f"Error decompressing data: {e}")

                # break

            except Exception as e:
                print(f"Error: {str(e)}")
                break

        self.remove_inactive_slave(slave_socket)
        
    def remove_inactive_slave(self, slave_socket):
        for unique_identifier, slave in self.slave_connections.items():
            if slave.socket == slave_socket:
                with self.lock:
                    del self.slave_connections[unique_identifier]
                    return

        print(f"Socket '{slave_socket}' does not exist in self.slave_connections")
        
    def send_url_chunks_to_slaves(self):
        while True:
            try:
                # To avoid sending to disconnected slaves
                active_slave_connections = self.slave_connections
                for unique_identifier, slave in active_slave_connections.items():
                    if not self.is_socket_connected(slave.socket):
                        with self.lock:
                            del self.slave_connections[unique_identifier]
                    else:
                        print("len queue:", len(slave.queue))
                        print("health:", slave.health)
                        
                        if slave.health == 100:
                            if len(slave.queue) <= 100:
                                # Get 30 URLs to send to the current slave
                                urls_to_send = self.get_next_urls(30)

                                if urls_to_send:
                                    # Serialize the URLs and compress the data
                                    data_to_send = json.dumps(urls_to_send).encode()
                                    compressed_data = zlib.compress(data_to_send)

                                    slave.socket.send(compressed_data + b'\x00\x01\x02\x03\x04\x05')

                                    self.add_visited_urls(urls_to_send)
                                    slave.append_queue(urls_to_send)
                                    self.remove_queue(urls_to_send)
                        else:
                            if len(slave.queue) <= 70:
                                # Get 20 URLs to send to the current slave
                                urls_to_send = self.get_next_urls(20)

                                if urls_to_send:
                                    # Serialize the URLs and compress the data
                                    data_to_send = json.dumps(urls_to_send).encode()
                                    compressed_data = zlib.compress(data_to_send)

                                    slave.socket.send(compressed_data + b'\x00\x01\x02\x03\x04\x05')

                                    self.add_visited_urls(urls_to_send)
                                    slave.append_queue(urls_to_send)
                                    self.remove_queue(urls_to_send)
            except Exception as e:
                print(f"Error: {str(e)}")

            print(f"{len(self.slave_connections)} Slave(s) Connected")
            time.sleep(5)
    
    def manage_slave_health(self):
        while True:
            try:
                for unique_identifier, slave in self.slave_connections.items():
                    if len(slave.average_crawling_time) > 10: 
                        if mean(slave.average_crawling_time) > 10:
                            slave.health = 50
                        else:
                            slave.health = 100
            except Exception as e:
                print(f"Error: {str(e)}")  
            time.sleep(20)
            
    # Function to check if a socket is still connected
    def is_socket_connected(self, sock):
        try:
            # Check if the socket is still open by sending a small amount of data
            sock.send(b'')
            return True
        except (OSError, ConnectionResetError):
            return False
        
    def append_queue(self, urls):
        with self.lock:
            for url in urls:
                if url not in self.visited_urls and url not in self.queue:
                    self.queue.append(url)
                
        
    def add_visited_urls(self, urls):
        for url in urls:
            with self.lock:
                self.visited_urls.add(url)
    
    def pop_queue(self):
        if not self.queue:
            print("List is empty, timeout 1 sec")
            time.sleep(1)
        else:
            with self.lock:
                return self.queue.pop(0)
                   
    def remove_queue(self, urls):
        for url in urls:
            with self.lock:
                self.queue.remove(url)
    
    def get_next_urls(self, n):
        with self.lock:
            return self.queue[:n]

def get_ip_address():
    try:
        # Get the local machine's IP address
        my_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        my_socket.connect(("8.8.8.8", 80))
        ip_address = my_socket.getsockname()[0]
        my_socket.close()
        return ip_address
    except Exception as e:
        print("Error:", e)
        return None

def is_private_ip(ip_address):
    try:
        ip = ipaddress.ip_address(ip_address)

        # Check if it's the loopback address
        if ip == ipaddress.IPv4Address("127.0.0.1"):
            return True

        private_ip_ranges = [
            ipaddress.IPv4Network("10.0.0.0/8"),
            ipaddress.IPv4Network("172.16.0.0/12"),
            ipaddress.IPv4Network("192.168.0.0/16"),
        ]

        # Check if the IP address is within any of the private ranges
        for private_range in private_ip_ranges:
            if ip in private_range:
                return True

        return False
    except ValueError:
        # Handle invalid IP address format
        return False

def handle_tcp(tcp_sock):
    while True:
        try:
            data = tcp_sock.recv(1024)

            if not data:
                break
                
            response_data = json.loads(data.decode('utf-8'))
            if response_data.get("msg") == "MANAGER_READY":
                global manager_address
                manager_info = response_data.get("data", [])[0]
                manager_address = tuple(manager_info.get("address", ()))
                print('manager address:', manager_address)
            elif response_data.get("msg") == "CLIENT_PUBLIC_IP_READY":
                global client_public_ready, client_public_address
                client_public_ready = True
                client_public_address = tuple(response_data.get("client_public_destination", ()))
        except KeyboardInterrupt:
            print("Main thread terminated by user.")
            break
        except Exception as e:
            print(f"Error while receiving data: {e}")
            break
        time.sleep(1)
    # Close the socket
    tcp_sock.close()
    print("TCP socket closed.")

def handle_client_tcp(sock):
    while True:
        try:
            data = sock.recv(1024)
            
            if not data:
                break
            print(f"Received data: {data.decode('utf-8')}")
        except Exception as e:
            print(f"Error while receiving data: {e}")
            break
        time.sleep(1)
    # Close the socket
    sock.close()
    print("TCP socket close.")

if __name__ == "__main__":
    # Get the local IP address
    my_ip = get_ip_address()

    if my_ip:
        print(f"My IP address: {my_ip}")

        # Check if it's private or public
        # Create a JSON object with the IP information
        private_ip = is_private_ip(my_ip)
        ip_info = {
            "type": "client",
            "ip": my_ip,
            "is_private": private_ip
        }

        # Send JSON message
        json_data = json.dumps(ip_info)

        # Create a TCP socket
        tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)

        # Connect to the tracker
        try:
            tcp_sock.connect((SERVER_ADDRESS, SERVER_PORT))
            print("Connected to the tracker")
            threading.Thread(target=handle_tcp, args=(tcp_sock,), daemon=True).start()
            tcp_sock.send(json_data.encode())
        except Exception as e:
            print(f"Connection error: {e}")
            time.sleep(1)
            
    else:
        print("Unable to retrieve IP address.")

    print("Private:", private_ip)

    # Create a another TCP socket
    tcp_client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp_client_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)

    while True:
        if client_public_ready:
            time.sleep(10)
            
            ###################
            slave_data = slave.Slave()
            # slave_socket = slave_data.connect_to_server()
            slave_socket = slave_data.connect_to_client_public(client_public_address)
            
            def receive_url_chunks():
                max_chunk_size = 65536
                delimiter = b'\x00\x01\x02\x03\x04\x05'
                data_buffer = b''

                while True:
                    try:
                        chunk = slave_socket.recv(max_chunk_size)
                        if not chunk:
                            break

                        data_buffer += chunk

                        while delimiter in data_buffer:
                            chunk, data_buffer = data_buffer.split(delimiter, 1)
                            try:
                                # Decompress the chunk before JSON decoding
                                decompressed_data = zlib.decompress(chunk)
                                urls = json.loads(decompressed_data.decode())
                                slave_data.append_url(urls)
                            except zlib.error as e:
                                print(f"Error decompressing data: {e}")
                            except json.JSONDecodeError as e:
                                print(f"Error decoding JSON: {e}")

                    except Exception as e:
                        print(f"Error while receiving data: {e}")
                        break
                    # print(len(slave_data.queue))
            
            listener_recv_url = threading.Thread(target=receive_url_chunks, daemon=True)
            listener_recv_url.start()
        
            def get_slave_socket():
                return slave_socket
            
            def get_current_queue():
                queue = slave_data.get_current_queue
                return queue
            
            # Keep the main thread running indefinitely
            while True:
                try:
                    if len(slave_data.queue) != 0:
                        load_dotenv()
                        db = Database()
                        db.create_tables()

                        status = os.getenv("CRAWLER_STATUS")
                        start_urls = slave_data.get_current_queue()
                        max_threads = os.getenv("CRAWLER_MAX_THREADS")
                        crawler_duration_sec = os.getenv("CRAWLER_DURATION_SECONDS")
                        try:
                            msb_keyword = os.getenv("CRAWLER_KEYWORD")
                        except:
                            msb_keyword = ""

                        if msb_keyword != "":
                            bfs_duration_sec = int(crawler_duration_sec) // 2
                            msb_duration_sec = int(crawler_duration_sec) // 2
                        else:
                            bfs_duration_sec = int(crawler_duration_sec)
                            msb_duration_sec = 0

                        c = Crawl(status, start_urls, max_threads, bfs_duration_sec, msb_duration_sec, msb_keyword)
                        page_count, res = c.run(slave_data)
                        if res == "Exited":
                            break
                    time.sleep(1)  # Sleep to avoid busy-waiting
                except KeyboardInterrupt:
                    # Handle Ctrl+C or other termination signals here
                    break  # Break the loop to allow the program to exit gracefully

            # Optionally, you can add cleanup code before exiting the application
            print("Exiting the application...")
            slave_socket.close()
            break
            ###################
        elif manager_address != None and not private_ip:
            tcp_client_sock.connect(manager_address)
            tcp_client_sock.send(b'Hi from client with public ip')
            
            data = tcp_client_sock.recv(1024)
            response_data = json.loads(data.decode('utf-8'))
            if response_data.get("msg") == "CRAWLER_START_URLS":
                start_urls = response_data.get("start_urls", [])[0].split()
                print('start_urls:', start_urls)
            
            tcp_sock.send(json.dumps({"msg": "CLIENT_CONNECTED_TO_THE_MANAGER"}).encode())
            break
        time.sleep(1)

    if manager_address != None and not private_ip and start_urls != None:
        ###############
        # Create a socket object
        public_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        public_client.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)

        # Bind the socket to a specific address and port
        public_client.bind((tcp_sock.getsockname()[0], tcp_sock.getsockname()[1]))

        db = DatabasePublic()
        db.create_tables()
        # Listen for incoming connections
        public_client.listen()
        print("Manager is listening for incoming connections...")

        load_balancer = LoadBalancer(queue=start_urls)

        # Start a thread for URL distribution
        url_distribution_thread = threading.Thread(target=load_balancer.send_url_chunks_to_slaves)
        url_distribution_thread.daemon = True
        url_distribution_thread.start()

        # Start a thread for Health Check
        url_distribution_thread = threading.Thread(target=load_balancer.manage_slave_health)
        url_distribution_thread.daemon = True
        url_distribution_thread.start()

        # Accept incoming connections and spawn a new thread to handle each client
        while True:
            try:
                slave_socket, slave_address = public_client.accept()
                print(f"Accepted connection from {slave_address}")
                    
                listener_slave_handler = threading.Thread(target=load_balancer.handle_slave, args=(slave_socket,), daemon=True)
                listener_slave_handler.start()   
                time.sleep(1)
            except KeyboardInterrupt:
                # Handle Ctrl+C or other termination signals here
                public_client.close()
                print("Exiting the application...")
                break  # Break the loop to allow the program to exit gracefully
        ###############
        