#include "rpc.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <iostream>
#include <sstream>

// Connecting to RPC server
int rpc_connect() {
  int clientSocket = socket(AF_INET, SOCK_STREAM, 0);
  if (clientSocket == -1) {
    return -1;
  }

  sockaddr_in addr = {};
  addr.sin_family = AF_INET;
  addr.sin_port = 8000;
  inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

  if (connect(clientSocket, (struct sockaddr *)&addr, sizeof(addr)) == -1) {
    return -1;
  }

  return clientSocket;
}

// Stub for SET command
int kv_set(std::string key, std::string value) {
  int clientSocket = rpc_connect();
  if (clientSocket == -1)
    return -1;

  struct RPC_message rpc;
  rpc.procedure = PROCEDURE_SET;
  strncpy(rpc.args.set_args.key, key.c_str(), 63);
  strncpy(rpc.args.set_args.value, value.c_str(), 63);

  send(clientSocket, (char *)&rpc, sizeof(RPC_message), 0);

  // Wait for acknowledgement
  struct RPC_message response;
  recv(clientSocket, (char *)&response, sizeof(RPC_message), 0);

  close(clientSocket);
  return 0;
}

// Stub for GET command
std::string kv_get(std::string key) {
  int clientSocket = rpc_connect();
  if (clientSocket == -1)
    return "";

  struct RPC_message rpc;
  rpc.procedure = PROCEDURE_GET;
  strncpy(rpc.args.get_args.key, key.c_str(), 63);

  send(clientSocket, (char *)&rpc, sizeof(RPC_message), 0);

  struct RPC_message response;
  recv(clientSocket, (char *)&response, sizeof(RPC_message), 0);

  close(clientSocket);
  // Server returns value in set_args.value
  return std::string(response.args.set_args.value);
}

int main() {
  std::cout << "Commands: SET key value | GET key | EXIT" << std::endl;
  std::cout << std::endl;

  std::string line;
  while (std::getline(std::cin, line)) {
    if (line.empty())
      continue;

    std::istringstream iss(line);
    std::string command;
    iss >> command;

    if (command == "SET") {
      std::string key, value;
      iss >> key;
      std::getline(iss >> std::ws, value);
      kv_set(key, value);
    } else if (command == "GET") {
      std::string key;
      iss >> key;
      std::string value = kv_get(key);
      std::cout << key << ":" << value << std::endl;
    } else if (command == "EXIT") {
      break;
    } else {
      std::cout << "Unknown command: " << command << std::endl;
      continue;
    }
  }

  return 0;
}
