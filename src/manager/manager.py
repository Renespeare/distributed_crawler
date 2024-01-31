import socket
import json
import threading
import time
import ipaddress
from dotenv import load_dotenv
import os

class Client:
    def __init__(self, address, type=None, is_private=True):
        self.address = address  # Tuple, contains IP address and port.
        self.type = type
        self.is_private = is_private

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

# Maintain a list of connected clients
clients = set()

load_dotenv()
server_addr = os.getenv("TRACKER_ADDRESS")
# Server configuration
SERVER_ADDRESS = server_addr
SERVER_PORT = 55555

# Create a TCP socket for communication with the server
tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
tcp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)

# Connect to the server
try:
    tcp_sock.connect((SERVER_ADDRESS, SERVER_PORT))
except Exception as e:
    print(f"Connection error: {e}")

# Send information to the server
my_ip = None
if tcp_sock.fileno() != -1:
    try:
        # Get the local machine's IP address
        my_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        my_socket.connect(("8.8.8.8", 80))
        my_ip = my_socket.getsockname()[0]
        my_socket.close()

        if my_ip:
            print(f"My IP address: {my_ip}")

            # Check if it's private or public
            # Create a JSON object with the IP information
            ip_info = {
                "type": "manager",
                "ip": my_ip,
                "is_private": is_private_ip(my_ip)
            }

            # Send JSON message
            tcp_sock.send(json.dumps(ip_info).encode())
            print("Information sent to the server.")
        else:
            print("Unable to retrieve IP address.")
    except Exception as e:
        print(f"Error sending information to the server: {e}")

if my_ip:
    # Function for continuous communication over TCP
    def continuous_communication(tcp_sock):
        while True:
            try:
                # Send a request for the list of clients
                tcp_sock.send(json.dumps({"msg": "REQUEST_CLIENTS"}).encode())

                # Receive and process the response
                data = tcp_sock.recv(1024)
                if not data:
                    print("Connection closed by the server.")
                    break

                response_data = json.loads(data.decode('utf-8'))

                if response_data.get("msg") == "CLIENT_LIST":
                    new_clients_info = response_data.get("data", [])
                    for client_info in new_clients_info:
                        address = tuple(client_info.get("address", ()))
                        type = client_info.get("type", None)
                        is_private = client_info.get("is_private", True)

                        # Check if a client with the same address already exists
                        existing_client = next((c for c in clients if c.address == address), None)

                        if existing_client:
                            # Update the existing client information if needed
                            existing_client.type = type
                            existing_client.is_private = is_private
                        else:
                            # Add a new client if it doesn't exist
                            new_client = Client(address, type=type, is_private=is_private)
                            clients.add(new_client)

                            # Check if the new client has a public IP
                            if type == 'client' and not is_private:
                                print('public:', address)
                                print('notify tracker')
                                tcp_sock.send(json.dumps({"msg": "MANAGER_READY", "client_destination": address}).encode())

                    print(f"Received client list: {new_clients_info}")
                    print(f"Clients: {clients}")
                else:
                    print(data.decode('utf-8'))

                time.sleep(10)
            except KeyboardInterrupt:
                print("Main thread terminated by user.")
                break
            except Exception as e:
                print(f"Error while receiving data: {e}")
                break

        # Close the socket
        tcp_sock.close()
        print("TCP socket closed.")

    threading.Thread(target=continuous_communication, args=(tcp_sock,), daemon=True).start()


# TCP, client with public ip can connect.
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
      
# Create a TCP socket
manager_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
manager_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
manager_sock.bind((tcp_sock.getsockname()[0], tcp_sock.getsockname()[1]))
manager_sock.listen() 

# if client_with_public_ip != None:
#   tcp_sock.connect(client_with_public_ip)
load_dotenv()
start_urls = os.getenv("CRAWLER_START_URLS").split()
print(f"TCP Manager listening on {manager_sock.getsockname()[0]}:{manager_sock.getsockname()[1]}")
tcp_client_sock, tcp_client_address = manager_sock.accept()
print(f"Accepted connection from {tcp_client_address}")

tcp_client_sock.send(json.dumps({"msg": "CRAWLER_START_URLS", "start_urls": start_urls}).encode())
threading.Thread(target=handle_client_tcp, args=(tcp_client_sock,), daemon=True).start()

while True:
    try:
        time.sleep(1)
    except KeyboardInterrupt:
        # Handle Ctrl+C or other termination signals here
        break  # Break the loop to allow the program to exit gracefully
