#include "kv_store.h"
#include <algorithm>
#include <arpa/inet.h>
#include <chrono>
#include <csignal>
#include <iomanip>
#include <iostream>
#include <netinet/in.h>
#include <numeric>
#include <sstream>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

int main() {
  signal(SIGPIPE, SIG_IGN);
  int clientSocket = socket(AF_INET, SOCK_STREAM, 0);
  sockaddr_in addr = {};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(8000);
  inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

  if (connect(clientSocket, (struct sockaddr *)&addr, sizeof(addr)) == -1) {
    perror("[CLIENT] Failed to connect");
    return 1;
  }

  std::cout << "=== BONUS CLIENT ===" << std::endl;
  std::string line;
  while (std::getline(std::cin, line)) {
    if (line.empty())
      continue;
    std::istringstream iss(line);
    std::string command;
    iss >> command;

    Message msg;
    if (command == "SET") {
      std::string key, value;
      iss >> key >> value;
      msg.cmd = CMD_SET;
      strncpy(msg.key, key.c_str(), MAX_KEY_SIZE - 1);
      strncpy(msg.value, value.c_str(), MAX_VALUE_SIZE - 1);
    } else if (command == "GET") {
      std::string key;
      iss >> key;
      msg.cmd = CMD_GET;
      strncpy(msg.key, key.c_str(), MAX_KEY_SIZE - 1);
    } else {
      continue;
    }

    auto start = std::chrono::high_resolution_clock::now();
    send(clientSocket, (char *)&msg, sizeof(Message), 0);

    Message response;
    if (recv(clientSocket, (char *)&response, sizeof(Message), 0) <= 0)
      break;

    auto end = std::chrono::high_resolution_clock::now();
    std::cout << response.response << " ("
              << std::chrono::duration<double, std::milli>(end - start).count()
              << "ms)" << std::endl;
  }
  return 0;
}
