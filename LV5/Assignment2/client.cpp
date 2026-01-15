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

  uint8_t buffer[132]; // 4 + 64 + 64
  uint32_t procedure = PROCEDURE_SET;

  // Marshal Procedure
  memcpy(buffer, &procedure, sizeof(uint32_t));

  // Marshal Key (Fixed 64 bytes)
  char keyBuf[KEY_SIZE] = {0};
  strncpy(keyBuf, key.c_str(), KEY_SIZE - 1);
  memcpy(buffer + sizeof(uint32_t), keyBuf, KEY_SIZE);

  // Marshal Value (Fixed 64 bytes)
  char valBuf[VALUE_SIZE] = {0};
  strncpy(valBuf, value.c_str(), VALUE_SIZE - 1);
  memcpy(buffer + sizeof(uint32_t) + KEY_SIZE, valBuf, VALUE_SIZE);

  send(clientSocket, buffer, sizeof(buffer), 0);

  // Wait for Ack
  recv(clientSocket, buffer, sizeof(buffer), 0);

  close(clientSocket);
  return 0;
}

// Stub for GET command
std::string kv_get(std::string key) {
  int clientSocket = rpc_connect();
  if (clientSocket == -1)
    return "";

  uint8_t buffer[68]; // 4 + 64
  uint32_t procedure = PROCEDURE_GET;

  // Marshal Procedure
  memcpy(buffer, &procedure, sizeof(uint32_t));

  // Marshal Key
  char keyBuf[KEY_SIZE] = {0};
  strncpy(keyBuf, key.c_str(), KEY_SIZE - 1);
  memcpy(buffer + sizeof(uint32_t), keyBuf, KEY_SIZE);

  send(clientSocket, buffer, sizeof(buffer), 0);

  // Receive Value
  char responseBuf[VALUE_SIZE] = {0};
  recv(clientSocket, responseBuf, VALUE_SIZE, 0);

  close(clientSocket);
  return std::string(responseBuf);
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
