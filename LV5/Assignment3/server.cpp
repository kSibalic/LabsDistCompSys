#include "rpc.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdint>
#include <iostream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

std::unordered_map<std::string, std::string> kvStore;

// Handle a single client connection
void handleClient(int clientSocket) {
  uint32_t procedure;
  // Read Procedure ID (4 bytes)
  if (recv(clientSocket, (char *)&procedure, sizeof(uint32_t), 0) <= 0) {
    return; // Client disconnected
  }

  if (procedure == PROCEDURE_SET) {
    // Read Key Length
    uint32_t keyLen = 0;
    recv(clientSocket, &keyLen, sizeof(uint32_t), 0);

    // Read Key
    std::vector<char> keyBuf(keyLen + 1);
    recv(clientSocket, keyBuf.data(), keyLen, 0);
    keyBuf[keyLen] = '\0';
    std::string key = keyBuf.data();

    // Read Value Length
    uint32_t valLen = 0;
    recv(clientSocket, &valLen, sizeof(uint32_t), 0);

    // Read Value
    std::vector<char> valBuf(valLen + 1);
    recv(clientSocket, valBuf.data(), valLen, 0);
    valBuf[valLen] = '\0';
    std::string value = valBuf.data();

    std::cout << "SET " << key << " = " << value << std::endl;
    kvStore[key] = value;

    // Send Ack
    char ack = 1;
    send(clientSocket, &ack, 1, 0);

  } else if (procedure == PROCEDURE_GET) {
    // Read Key Length
    uint32_t keyLen = 0;
    recv(clientSocket, &keyLen, sizeof(uint32_t), 0);

    // Read Key
    std::vector<char> keyBuf(keyLen + 1);
    recv(clientSocket, keyBuf.data(), keyLen, 0);
    keyBuf[keyLen] = '\0';
    std::string key = keyBuf.data();

    std::cout << "GET " << key << std::endl;
    std::string resultValue = "";
    if (kvStore.count(key)) {
      resultValue = kvStore[key];
    }

    // Send Value Length
    uint32_t valLen = resultValue.length();
    send(clientSocket, &valLen, sizeof(uint32_t), 0);

    // Send Value
    if (valLen > 0) {
      send(clientSocket, resultValue.c_str(), valLen, 0);
    }
  } else {
    std::cout << "Received unknown procedure selection: " << procedure
              << std::endl;
  }
}

int main() {
  int clientSocket = socket(AF_INET, SOCK_STREAM, 0);

  sockaddr_in clientAddr = {};
  clientAddr.sin_family = AF_INET;
  clientAddr.sin_port = 8000;
  inet_pton(AF_INET, "127.0.0.1", &clientAddr.sin_addr);

  int option_value = 1;
  setsockopt(clientSocket, SOL_SOCKET, SO_REUSEADDR, &option_value,
             sizeof(int));

  if (bind(clientSocket, (struct sockaddr *)&clientAddr, sizeof(clientAddr)) ==
      -1) {
    perror("Failed to bind client socket");
    return 1;
  }

  listen(clientSocket, 10);
  std::cout << "RPC server listening for clients on 8000" << std::endl;

  // Accept client connections
  while (true) {
    struct sockaddr_in addr;
    socklen_t addrLen = sizeof(addr);

    int connSocket = accept(clientSocket, (struct sockaddr *)&addr, &addrLen);
    if (connSocket == -1) {
      perror("Failed to accept client");
      continue;
    }

    // Handle each client in a separate thread
    std::thread clientThread(handleClient, connSocket);
    clientThread.detach();
  }

  return 0;
}
