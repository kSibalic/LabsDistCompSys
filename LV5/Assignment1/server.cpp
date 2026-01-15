#include "rpc.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <iostream>
#include <map>
#include <string>
#include <thread>
#include <unordered_map>

std::unordered_map<std::string, std::string> kvStore;

// Handle a single client connection
void handleClient(int clientSocket) {
  RPC_message request;
  if (recv(clientSocket, (char *)&request, sizeof(request), 0) <= 0) {
    return; // Client disconnected
  }

  if (request.procedure == PROCEDURE_SET) {
    std::string key = request.args.set_args.key;
    std::string value = request.args.set_args.value;
    std::cout << "SET " << key << " = " << value << std::endl;
    kvStore[key] = value;
    send(clientSocket, (char *)&request, sizeof(RPC_message), 0);
  } else if (request.procedure == PROCEDURE_GET) {
    std::string key = request.args.get_args.key;
    std::cout << "GET " << key << std::endl;
    std::string value = "";
    if (kvStore.count(key)) {
      value = kvStore[key];
    }
    memset(request.args.set_args.value, 0, sizeof(request.args.set_args.value));
    strncpy(request.args.set_args.value, value.c_str(), 63);
    send(clientSocket, (char *)&request, sizeof(RPC_message), 0);
  } else {
    std::cout << "Received unknown procedure selection from client: "
              << request.procedure << std::endl;
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
