#include "kv_store.h"
#include <algorithm>
#include <arpa/inet.h>
#include <atomic>
#include <condition_variable>
#include <csignal>
#include <fcntl.h>
#include <iostream>
#include <map>
#include <mutex>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

KeyValueStore store;
std::vector<int> followerSockets;
std::mutex socketsMutex;
std::atomic<uint64_t> sequenceCounter{0};

// Track pending ACKs for each sequence number
struct PendingOperation {
  int expectedAcks;
  int receivedAcks;
  std::mutex mtx;
  std::condition_variable cv;
  bool completed;
  bool success;
};

std::map<uint64_t, PendingOperation *> pendingOps;
std::mutex pendingOpsMutex;

// Thread to receive ACKs from a specific follower
void receiveAcks(int followerSocket, int followerId) {
  Message ackMsg;

  while (true) {
    int bytesReceived =
        recv(followerSocket, (char *)&ackMsg, sizeof(Message), 0);

    if (bytesReceived <= 0) {
      std::cout << "[LEADER] Follower " << followerId << " disconnected"
                << std::endl;

      // STRICT CP: Do NOT remove from follower list.
      // If we remove it, the next write succeeds with N-1 nodes (AP-like
      // behavior). By keeping it, the next write will try to send to N nodes,
      // fail to reach this one, and thus fail the write (Strong Consistency).

      /*
      // Remove from follower list
      {
        std::lock_guard<std::mutex> lock(socketsMutex);
        auto it = std::find(followerSockets.begin(), followerSockets.end(),
                            followerSocket);
        if (it != followerSockets.end()) {
          followerSockets.erase(it);
        }
      }
      */
      break;
    }

    if (ackMsg.cmd == CMD_ACK) {
      uint64_t seq = ackMsg.sequence;

      std::lock_guard<std::mutex> lock(pendingOpsMutex);
      auto it = pendingOps.find(seq);
      if (it != pendingOps.end()) {
        PendingOperation *op = it->second;
        std::lock_guard<std::mutex> opLock(op->mtx);
        op->receivedAcks++;

        std::cout << "[LEADER] Received ACK for seq " << seq
                  << " from follower " << ackMsg.followerId << " ("
                  << op->receivedAcks << "/" << op->expectedAcks << ")"
                  << std::endl;

        // Check if we have enough ACKs
        if (REQUIRE_ALL_ACKS) {
          if (op->receivedAcks >= op->expectedAcks) {
            op->completed = true;
            op->success = true;
            op->cv.notify_all();
          }
        } else {
          // Quorum-based: majority is enough
          int majority = (op->expectedAcks / 2) + 1;
          if (op->receivedAcks >= majority) {
            op->completed = true;
            op->success = true;
            op->cv.notify_all();
          }
        }
      }
    }
  }

  close(followerSocket);
}

// Broadcast replication log to all followers and wait for ACKs
bool broadcastAndWaitForAcks(const Message &msg) {
  std::vector<int> currentFollowers;

  {
    std::lock_guard<std::mutex> lock(socketsMutex);
    currentFollowers = followerSockets;
  }

  int numFollowers = currentFollowers.size();

  // If no followers, operation succeeds immediately
  if (numFollowers == 0) {
    std::cout
        << "[LEADER] No followers connected, proceeding without replication"
        << std::endl;
    return true;
  }

  // Create pending operation tracker
  PendingOperation *op = new PendingOperation();
  op->expectedAcks = numFollowers;
  op->receivedAcks = 0;
  op->completed = false;
  op->success = false;

  {
    std::lock_guard<std::mutex> lock(pendingOpsMutex);
    pendingOps[msg.sequence] = op;
  }

  // Send to all followers
  std::cout << "[LEADER] Broadcasting seq " << msg.sequence << " to "
            << numFollowers << " followers" << std::endl;

  for (int followerSocket : currentFollowers) {
    if (send(followerSocket, (const char *)&msg, sizeof(Message), 0) == -1) {
      std::cout << "[LEADER] Failed to send to follower " << followerSocket
                << std::endl;
      // Do not continue or exit, just print error.
      // The pendingOp will time out because this follower won't ACK.
    }
  }

  // Wait for ACKs with timeout
  bool success = false;
  {
    std::unique_lock<std::mutex> lock(op->mtx);
    success = op->cv.wait_for(lock, std::chrono::milliseconds(ACK_TIMEOUT_MS),
                              [op]() { return op->completed; });

    if (!success) {
      std::cout << "[LEADER] TIMEOUT waiting for ACKs on seq " << msg.sequence
                << " (received " << op->receivedAcks << "/" << op->expectedAcks
                << ")" << std::endl;
    }
  }

  // Cleanup
  {
    std::lock_guard<std::mutex> lock(pendingOpsMutex);
    pendingOps.erase(msg.sequence);
  }

  bool result = op->success;
  delete op;

  return result;
}

// Handle a single client connection
void handleClient(int clientSocket) {
  Message msg;

  while (true) {
    // Receive message from client
    if (recv(clientSocket, (char *)&msg, sizeof(Message), 0) <= 0) {
      break; // Client disconnected
    }

    std::string key(msg.key);
    std::string value(msg.value);

    if (msg.cmd == CMD_SET) {
      // Assign sequence number
      msg.sequence = ++sequenceCounter;

      std::cout << "[LEADER] Processing SET " << key << " = " << value
                << " (seq: " << msg.sequence << ")" << std::endl;

      // In CP system: first replicate, then commit locally
      // This ensures all nodes have the data before confirming

      bool replicationSuccess = broadcastAndWaitForAcks(msg);

      if (replicationSuccess) {
        // All followers acknowledged, now commit locally
        store.set(key, value);
        msg.status = 0;
        snprintf(msg.response, MAX_VALUE_SIZE,
                 "SET %s = %s (replicated to all nodes)", key.c_str(),
                 value.c_str());
      } else {
        // CP guarantee: if we can't replicate to all, we fail the operation
        msg.status = -1;
        snprintf(msg.response, MAX_VALUE_SIZE,
                 "FAILED: Could not replicate to all followers (CP violation "
                 "prevented)");
      }

    } else if (msg.cmd == CMD_GET) {
      // Read from local store (reads don't need replication)
      std::string result;
      if (store.get(key, result)) {
        msg.status = 0;
        snprintf(msg.response, MAX_VALUE_SIZE, "%s", result.c_str());
      } else {
        msg.status = -1;
        snprintf(msg.response, MAX_VALUE_SIZE, "Key not found");
      }

    } else if (msg.cmd == CMD_DELETE) {
      // Assign sequence number
      msg.sequence = ++sequenceCounter;

      std::cout << "[LEADER] Processing DELETE " << key
                << " (seq: " << msg.sequence << ")" << std::endl;

      // In CP system: first replicate, then commit locally
      bool replicationSuccess = broadcastAndWaitForAcks(msg);

      if (replicationSuccess) {
        bool deleted = store.deleteKey(key);
        msg.status = deleted ? 0 : -1;
        snprintf(msg.response, MAX_VALUE_SIZE, "%s",
                 deleted ? "Key deleted (replicated to all nodes)"
                         : "Key not found");
      } else {
        msg.status = -1;
        snprintf(msg.response, MAX_VALUE_SIZE,
                 "FAILED: Could not replicate to all followers (CP violation "
                 "prevented)");
      }

    } else if (msg.cmd == CMD_LIST) {
      const std::unordered_map<std::string, std::string> data =
          store.getAllData();
      std::string listStr = "Keys: ";
      for (auto const &[k, v] : data) {
        listStr += k + "=" + v + "; ";
        std::cout << k << ":" << v << std::endl;
      }
      msg.status = 0;
      snprintf(msg.response, MAX_VALUE_SIZE, "%s", listStr.c_str());
    }

    // Send response back to client
    if (send(clientSocket, (char *)&msg, sizeof(Message), 0) == -1) {
      perror("Failed to send response");
      break;
    }
  }

  close(clientSocket);
}

// Accept follower registrations
void acceptFollowers(int registrationSocket) {
  struct sockaddr_in addr;
  socklen_t addrLen = sizeof(addr);
  int followerIdCounter = 0;

  while (true) {
    int followerSocket =
        accept(registrationSocket, (struct sockaddr *)&addr, &addrLen);
    if (followerSocket == -1) {
      perror("Failed to accept follower");
      continue;
    }

    int followerId = ++followerIdCounter;

    {
      std::lock_guard<std::mutex> lock(socketsMutex);
      followerSockets.push_back(followerSocket);
    }

    std::cout << "[LEADER] New follower " << followerId
              << " connected (total: " << followerSockets.size() << ")"
              << std::endl;

    // Start thread to receive ACKs from this follower
    std::thread ackThread(receiveAcks, followerSocket, followerId);
    ackThread.detach();
  }
}

int main() {
  // macOS: Ignore SIGPIPE
  signal(SIGPIPE, SIG_IGN);

  std::cout << "========================================" << std::endl;
  std::cout << "   CP System Leader (Strong Consistency)" << std::endl;
  std::cout << "========================================" << std::endl;
  std::cout << "ACK Timeout: " << ACK_TIMEOUT_MS << "ms" << std::endl;
  std::cout << "Mode: "
            << (REQUIRE_ALL_ACKS ? "ALL followers must ACK"
                                 : "Majority (quorum) ACK")
            << std::endl;
  std::cout << std::endl;

  // Create registration socket for followers
  int regSocket = socket(AF_INET, SOCK_STREAM, 0);

  int option_value = 1;
  setsockopt(regSocket, SOL_SOCKET, SO_REUSEADDR, &option_value, sizeof(int));

  sockaddr_in regAddr = {};
  regAddr.sin_family = AF_INET;
  regAddr.sin_port = htons(8080);
  inet_pton(AF_INET, "127.0.0.1", &regAddr.sin_addr);

  if (bind(regSocket, (struct sockaddr *)&regAddr, sizeof(regAddr)) == -1) {
    perror("Failed to bind registration socket");
    return 1;
  }

  listen(regSocket, 10);
  std::cout << "[LEADER] Follower registration on port 8080" << std::endl;

  // Start thread to accept follower registrations
  std::thread followerAcceptThread(acceptFollowers, regSocket);
  followerAcceptThread.detach();

  // Create client socket
  int clientSocket = socket(AF_INET, SOCK_STREAM, 0);

  setsockopt(clientSocket, SOL_SOCKET, SO_REUSEADDR, &option_value,
             sizeof(int));

  sockaddr_in clientAddr = {};
  clientAddr.sin_family = AF_INET;
  clientAddr.sin_port = htons(8000);
  inet_pton(AF_INET, "127.0.0.1", &clientAddr.sin_addr);

  if (bind(clientSocket, (struct sockaddr *)&clientAddr, sizeof(clientAddr)) ==
      -1) {
    perror("Failed to bind client socket");
    return 1;
  }

  listen(clientSocket, 10);
  std::cout << "[LEADER] Client connections on port 8000" << std::endl;
  std::cout << std::endl;

  // Main loop: accept client connections
  while (true) {
    struct sockaddr_in addr;
    socklen_t addrLen = sizeof(addr);

    int connSocket = accept(clientSocket, (struct sockaddr *)&addr, &addrLen);
    if (connSocket == -1) {
      perror("Failed to accept client");
      continue;
    }

    std::cout << "[LEADER] New client connected" << std::endl;

    // Handle each client in a separate thread
    std::thread clientThread(handleClient, connSocket);
    clientThread.detach();
  }

  close(clientSocket);
  close(regSocket);
  return 0;
}
