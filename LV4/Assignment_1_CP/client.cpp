#include "kv_store.h"
#include <algorithm>
#include <arpa/inet.h>
#include <chrono>
#include <csignal>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <netinet/in.h>
#include <numeric>
#include <sstream>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

// Statistics tracking
struct ResponseStats {
  std::vector<double> responseTimes;
  int successCount = 0;
  int failureCount = 0;

  void addResponse(double timeMs, bool success) {
    responseTimes.push_back(timeMs);
    if (success)
      successCount++;
    else
      failureCount++;
  }

  double getAverage() const {
    if (responseTimes.empty())
      return 0;
    return std::accumulate(responseTimes.begin(), responseTimes.end(), 0.0) /
           responseTimes.size();
  }

  double getMin() const {
    if (responseTimes.empty())
      return 0;
    return *std::min_element(responseTimes.begin(), responseTimes.end());
  }

  double getMax() const {
    if (responseTimes.empty())
      return 0;
    return *std::max_element(responseTimes.begin(), responseTimes.end());
  }

  void printStats() const {
    std::cout << "\n========== Response Time Statistics =========="
              << std::endl;
    std::cout << "Total requests: " << responseTimes.size() << std::endl;
    std::cout << "Successful: " << successCount << std::endl;
    std::cout << "Failed: " << failureCount << std::endl;
    std::cout << std::fixed << std::setprecision(3);
    std::cout << "Average response time: " << getAverage() << " ms"
              << std::endl;
    std::cout << "Min response time: " << getMin() << " ms" << std::endl;
    std::cout << "Max response time: " << getMax() << " ms" << std::endl;
    std::cout << "==============================================" << std::endl;
  }

  void saveToFile(const std::string &filename) const {
    std::ofstream file(filename);
    file << "request_num,response_time_ms,success" << std::endl;
    for (size_t i = 0; i < responseTimes.size(); i++) {
      file << i + 1 << "," << responseTimes[i] << ","
           << (i < (size_t)successCount ? "1" : "0") << std::endl;
    }
    file.close();
    std::cout << "Statistics saved to " << filename << std::endl;
  }
};

ResponseStats stats;

// Run automated benchmark
void runBenchmark(int clientSocket, int numOperations) {
  std::cout << "\n========== Running Benchmark ==========" << std::endl;
  std::cout << "Operations: " << numOperations << std::endl;
  std::cout << std::endl;

  for (int i = 0; i < numOperations; i++) {
    Message msg;
    msg.cmd = CMD_SET;
    snprintf(msg.key, MAX_KEY_SIZE, "benchmark_key_%d", i);
    snprintf(msg.value, MAX_VALUE_SIZE, "benchmark_value_%d", i);

    auto start = std::chrono::high_resolution_clock::now();

    // Send message
    if (send(clientSocket, (char *)&msg, sizeof(Message), 0) == -1) {
      std::cout << "Failed to send message " << i << std::endl;
      continue;
    }

    // Receive response
    Message response;
    if (recv(clientSocket, (char *)&response, sizeof(Message), 0) <= 0) {
      std::cout << "Failed to receive response " << i << std::endl;
      break;
    }

    auto end = std::chrono::high_resolution_clock::now();
    double elapsed =
        std::chrono::duration<double, std::milli>(end - start).count();

    bool success = (response.status == 0);
    stats.addResponse(elapsed, success);

    std::cout << "Operation " << (i + 1) << "/" << numOperations << ": "
              << elapsed << " ms " << (success ? "[OK]" : "[FAILED]")
              << std::endl;
  }

  stats.printStats();
}

int main(int argc, char *argv[]) {
  // macOS: Ignore SIGPIPE to prevent crash on write to closed socket
  signal(SIGPIPE, SIG_IGN);

  std::cout << "========================================" << std::endl;
  std::cout << "   CP System Client" << std::endl;
  std::cout << "========================================" << std::endl;
  std::cout << std::endl;

  // Connect to leader
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
    std::cerr << "Make sure the leader process is running!" << std::endl;
    return 1;
  }

  std::cout << "Connected to CP key-value store leader" << std::endl;
  std::cout << std::endl;
  std::cout << "Commands:" << std::endl;
  std::cout << "  SET key value    - Set a key-value pair" << std::endl;
  std::cout << "  GET key          - Get value for a key" << std::endl;
  std::cout << "  DELETE key       - Delete a key" << std::endl;
  std::cout << "  LIST             - List all key-value pairs" << std::endl;
  std::cout << "  BENCHMARK n      - Run n SET operations and measure times"
            << std::endl;
  std::cout << "  STATS            - Show response time statistics"
            << std::endl;
  std::cout << "  EXIT             - Exit client" << std::endl;
  std::cout << std::endl;

  std::string line;
  while (std::getline(std::cin, line)) {
    if (line.empty())
      continue;

    std::istringstream iss(line);
    std::string command;
    iss >> command;

    // Handle local commands
    if (command == "STATS") {
      stats.printStats();
      continue;
    } else if (command == "SAVE") {
      std::string filename;
      iss >> filename;
      if (filename.empty())
        filename = "cp_stats.csv";
      stats.saveToFile(filename);
      continue;
    } else if (command == "BENCHMARK") {
      int n = 10;
      iss >> n;
      runBenchmark(clientSocket, n);
      continue;
    } else if (command == "EXIT") {
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
    auto start = std::chrono::high_resolution_clock::now();

    // Send message to leader
    if (send(clientSocket, (char *)&msg, sizeof(Message), 0) == -1) {
      perror("Failed to send message");
      break;
    }

    // Receive response
    Message response;
    if (recv(clientSocket, (char *)&response, sizeof(Message), 0) <= 0) {
      std::cout << "Connection lost" << std::endl;
      break;
    }

    auto end = std::chrono::high_resolution_clock::now();
    double elapsed =
        std::chrono::duration<double, std::milli>(end - start).count();

    bool success = (response.status == 0);

    // Only track write operations for CP comparison
    if (command == "SET" || command == "DELETE") {
      stats.addResponse(elapsed, success);
    }

    if (response.status == 0) {
      std::cout << "[OK] " << response.response
                << " (response time: " << std::fixed << std::setprecision(3)
                << elapsed << " ms)" << std::endl;
    } else {
      std::cout << "[ERROR] " << response.response
                << " (response time: " << std::fixed << std::setprecision(3)
                << elapsed << " ms)" << std::endl;
    }
  }

  // Print final stats
  if (!stats.responseTimes.empty()) {
    stats.printStats();
  }

  close(clientSocket);
  return 0;
}
