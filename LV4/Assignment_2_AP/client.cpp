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

// Statistics tracking
std::vector<double> responseTimes;

double calculateAverage(const std::vector<double> &times) {
  if (times.empty())
    return 0;
  return std::accumulate(times.begin(), times.end(), 0.0) / times.size();
}

double calculateMin(const std::vector<double> &times) {
  if (times.empty())
    return 0;
  return *std::min_element(times.begin(), times.end());
}

double calculateMax(const std::vector<double> &times) {
  if (times.empty())
    return 0;
  return *std::max_element(times.begin(), times.end());
}

void printStatistics() {
  if (responseTimes.empty()) {
    std::cout << "\nNo requests recorded." << std::endl;
    return;
  }

  std::cout << "\n========================================" << std::endl;
  std::cout << "       AP SYSTEM STATISTICS            " << std::endl;
  std::cout << "========================================" << std::endl;
  std::cout << std::fixed << std::setprecision(3);
  std::cout << "Total requests: " << responseTimes.size() << std::endl;
  std::cout << "Average response time: " << calculateAverage(responseTimes)
            << " ms" << std::endl;
  std::cout << "Min response time: " << calculateMin(responseTimes) << " ms"
            << std::endl;
  std::cout << "Max response time: " << calculateMax(responseTimes) << " ms"
            << std::endl;
  std::cout << "========================================" << std::endl;
}

int main() {
  // macOS: Ignore SIGPIPE
  signal(SIGPIPE, SIG_IGN);

  int clientSocket = socket(AF_INET, SOCK_STREAM, 0);
  if (clientSocket == -1) {
    perror("Failed to create socket");
    return 1;
  }

  sockaddr_in addr = {};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(8000);
  inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

  if (connect(clientSocket, (struct sockaddr *)&addr, sizeof(addr)) == -1) {
    perror("Failed to connect to leader");
    std::cerr << "Make sure the AP leader process is running!" << std::endl;
    return 1;
  }

  std::cout << "========================================" << std::endl;
  std::cout << "   AP SYSTEM CLIENT                    " << std::endl;
  std::cout << "========================================" << std::endl;
  std::cout << "Connected to AP key-value store" << std::endl;
  std::cout
      << "Commands: SET key value | GET key | DELETE key | LIST | STATS | EXIT"
      << std::endl;
  std::cout << "Response times are measured automatically" << std::endl;
  std::cout << "========================================" << std::endl;
  std::cout << std::endl;

  std::string line;
  while (std::getline(std::cin, line)) {
    if (line.empty())
      continue;

    std::istringstream iss(line);
    std::string command;
    iss >> command;

    // Stats command (local)
    if (command == "STATS") {
      printStatistics();
      continue;
    }

    if (command == "EXIT") {
      printStatistics();
      break;
    }

    Message msg;

    if (command == "SET") {
      std::string key, value;
      iss >> key;
      std::getline(iss >> std::ws, value);

      msg.cmd = CMD_SET;
      strncpy(msg.key, key.c_str(), MAX_KEY_SIZE - 1);
      strncpy(msg.value, value.c_str(), MAX_VALUE_SIZE - 1);

    } else if (command == "GET") {
      std::string key;
      iss >> key;
      msg.cmd = CMD_GET;
      strncpy(msg.key, key.c_str(), MAX_KEY_SIZE - 1);

    } else if (command == "DELETE") {
      std::string key;
      iss >> key;
      msg.cmd = CMD_DELETE;
      strncpy(msg.key, key.c_str(), MAX_KEY_SIZE - 1);

    } else if (command == "LIST") {
      msg.cmd = CMD_LIST;

    } else {
      std::cout << "Unknown command: " << command << std::endl;
      continue;
    }

    // Measure response time
    auto startTime = std::chrono::high_resolution_clock::now();

    if (send(clientSocket, (char *)&msg, sizeof(Message), 0) == -1) {
      perror("Failed to send message");
      break;
    }

    Message response;
    if (recv(clientSocket, (char *)&response, sizeof(Message), 0) <= 0) {
      std::cout << "Connection lost" << std::endl;
      break;
    }

    auto endTime = std::chrono::high_resolution_clock::now();
    double responseTime =
        std::chrono::duration<double, std::milli>(endTime - startTime).count();
    responseTimes.push_back(responseTime);

    if (response.status == 0) {
      std::cout << "[OK] " << response.response << " (time: " << std::fixed
                << std::setprecision(3) << responseTime << " ms)" << std::endl;
    } else {
      std::cout << "[ERROR] " << response.response << " (time: " << std::fixed
                << std::setprecision(3) << responseTime << " ms)" << std::endl;
    }
  }

  close(clientSocket);
  return 0;
}
