import socket
import threading
import json
import time
from database.database import Database

class Client:
    def __init__(self, client_socket, address):
        self.client_socket = client_socket
        self.address = address
        self.type = None
        self.is_private = True

    def update_info(self, type, is_private):
        self.type = type
        if is_private:
          self.is_private = True
        else:
          self.is_private = False

def parse_json(json_string):
    try:
        parsed_data = json.loads(json_string)
        return parsed_data
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return None

# Server configuration
HOST = '0.0.0.0'
PORT = 55555

# Create a TCP socket
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
server_socket.bind((HOST, PORT))
server_socket.listen()

# Maintain a list of connected clients
clients = set()
log_clients = list()
manager_ready = False
client_public_ready = False
client_public_destination = None

db = Database()
db.create_tables()

def handle_client(client):
    db_connection = db.connect()
    while True:
        data = client.client_socket.recv(1024)
        if not data:
            break  # Connection closed by client
        data = data.decode('utf-8')

        # Handle the data received from the client
        data_json = parse_json(data)
        if data_json:
            if client.type is None:
                client.update_info(data_json['type'], data_json['is_private'])
                db.insert_log_client(db_connection, client.address, data_json['type'], data_json['is_private'])
                print(f"Received from {client.address}: {data_json}")
                global client_public_ready, client_public_destination
                if client_public_ready == True:
                    for client_private in filter(lambda c: c.is_private, clients):
                        client_private.client_socket.send(json.dumps(
                            {
                                "msg": "CLIENT_PUBLIC_IP_READY", "client_public_destination": client_public_destination
                            }).encode())
                
            # If the client is a manager and requests the list of clients
            if client.type == 'manager':
                if data_json.get("msg") == "REQUEST_CLIENTS":
                    # Send the list of connected clients to the manager
                    client_info_list = [
                        {"address": c.address, "type": c.type, "is_private": c.is_private}
                        for c in clients
                    ]
                    print("clients:", client_info_list)
                    response_data = {
                        "msg": "CLIENT_LIST",
                        "data": client_info_list
                    }
                    client.client_socket.send(json.dumps(response_data).encode())
                elif data_json.get("msg") == "MANAGER_READY":
                    global manager_ready
                    manager_ready = True
                    print('manager ready?', manager_ready)
                    client_destination = tuple(data_json.get("client_destination", ()))
                    client_info_list = [
                        {
                            "address": client.address, 
                            "type": client.type,
                            "is_private": client.is_private,
                        }
                    ]
                    
                    response_data = {
                        "msg": "MANAGER_READY",
                        "data": client_info_list
                    }
                    
                    print('notify client')
                    # Search for the client with the target address
                    target_client = next((client for client in clients if client.address == client_destination), None)
                    target_client.client_socket.send(json.dumps(response_data).encode()) 
            elif data_json.get("msg") == "CLIENT_CONNECTED_TO_THE_MANAGER":
                client_public_ready = True
                print("received data:", data_json)
                client_public_destination = client.address
                for client_private in filter(lambda c: c.is_private, clients):
                    client_private.client_socket.send(json.dumps({"msg": "CLIENT_PUBLIC_IP_READY", "client_public_destination": client.address}).encode())
                
        time.sleep(1)
    # Remove the client from the list when the connection is closed
    clients.remove(client)
    client.client_socket.close()
    print(f"Connection closed with {client.address}")

def listen_for_clients():
    while True:
        try:
            client_socket, client_address = server_socket.accept()
            client = Client(client_socket, client_address)
            clients.add(client)
            log_clients.append(client)
            print(f"New connection from {client_address}")

            # Handle the client in a separate thread
            threading.Thread(target=handle_client, args=(client,), daemon=True).start()
        except KeyboardInterrupt:
            print("Main thread terminated by user.")
            break
        except Exception as e:
            print(f"Error while receiving data: {e}")
            break

if __name__ == "__main__":
    print(f"Server listening on {server_socket.getsockname()[0]}:{PORT}")

    # Start a thread to listen for clients
    threading.Thread(target=listen_for_clients).start()
