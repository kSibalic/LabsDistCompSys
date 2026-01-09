#include "kv_store.h"
#include <algorithm>
#include <arpa/inet.h>
#include <atomic>
#include <csignal>
#include <errno.h>
#include <fcntl.h>
#include <iostream>
#include <mutex>
#include <netinet/in.h>
#include <queue>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

KeyValueStore store;
std::vector<int> followerSockets;
std::mutex socketsMutex;

// Operation log for eventual consistency - stores all write operations
std::vector<Message> operationLog;
std::mutex logMutex;
std::atomic<int> logSequence{0};

// Broadcast to all followers asynchronously (fire-and-forget)
void broadcastToFollowersAsync(const Message &msg) {
  std::lock_guard<std::mutex> lock(socketsMutex);

  std::vector<int> deadFollowers;

  for (int followerSocket : followerSockets) {
    // macOS Fix: Removed MSG_NOSIGNAL
    int result = send(followerSocket, (const char *)&msg, sizeof(Message), 0);
    if (result == -1) {
      std::cout << "[LEADER-AP] Follower " << followerSocket << " unreachable"
                << std::endl;
      deadFollowers.push_back(followerSocket);
    }
  }

  // Remove dead followers (they'll resync when they reconnect)
  for (int dead : deadFollowers) {
    followerSockets.erase(
        std::remove(followerSockets.begin(), followerSockets.end(), dead),
        followerSockets.end());
    close(dead);
  }
}

// Handle client - AP system responds immediately without waiting for followers
void handleClient(int clientSocket) {
  Message msg;

  while (true) {
    if (recv(clientSocket, (char *)&msg, sizeof(Message), 0) <= 0) {
      break;
    }

    std::string key(msg.key);
    std::string value(msg.value);

    if (msg.cmd == CMD_SET) {
      // AP: Write locally and respond immediately
      store.set(key, value);
      msg.status = 0;
      msg.sequence = ++logSequence;
      snprintf(msg.response, MAX_VALUE_SIZE, "SET %s = %s (seq: %d)",
               key.c_str(), value.c_str(), msg.sequence);

      // Store in operation log for eventual consistency
      {
        std::lock_guard<std::mutex> lock(logMutex);
        operationLog.push_back(msg);
      }

      // Fire-and-forget broadcast to followers (async, non-blocking)
      std::thread([msg]() { broadcastToFollowersAsync(msg); }).detach();

    } else if (msg.cmd == CMD_GET) {
      // AP: Read from local store immediately
      std::string result;
      if (store.get(key, result)) {
        msg.status = 0;
        snprintf(msg.response, MAX_VALUE_SIZE, "%s", result.c_str());
      } else {
        msg.status = -1;
        snprintf(msg.response, MAX_VALUE_SIZE, "Key not found");
      }

    } else if (msg.cmd == CMD_DELETE) {
      // AP: Delete locally and respond immediately
      bool deleted = store.deleteKey(key);
      msg.status = deleted ? 0 : -1;
      msg.sequence = ++logSequence;
      snprintf(msg.response, MAX_VALUE_SIZE, "%s (seq: %d)",
               deleted ? "Key deleted" : "Key not found", msg.sequence);

      // Store in operation log
      {
        std::lock_guard<std::mutex> lock(logMutex);
        operationLog.push_back(msg);
      }

      // Fire-and-forget broadcast
      std::thread([msg]() { broadcastToFollowersAsync(msg); }).detach();

    } else if (msg.cmd == CMD_LIST) {
      const auto &data = store.getAllData();
      std::cout << "[LEADER-AP] Current data:" << std::endl;
      for (const auto &[k, v] : data) {
        std::cout << "  " << k << ": " << v << std::endl;
      }
      msg.status = 0;
      snprintf(msg.response, MAX_VALUE_SIZE, "Listed %zu keys", data.size());

      std::thread([msg]() { broadcastToFollowersAsync(msg); }).detach();

    } else if (msg.cmd == CMD_SYNC) {
      // Follower requesting sync - send all operations from given sequence
      int fromSeq = msg.sequence;
      std::cout << "[LEADER-AP] Sync request from seq " << fromSeq << std::endl;

      std::lock_guard<std::mutex> lock(logMutex);
      int syncCount = 0;
      for (const auto &op : operationLog) {
        if (op.sequence > fromSeq) {
          send(clientSocket, (char *)&op, sizeof(Message), 0);
          syncCount++;
        }
      }

      // Send sync complete message
      Message syncDone;
      syncDone.cmd = CMD_ACK;
      syncDone.sequence = logSequence;
      snprintf(syncDone.response, MAX_VALUE_SIZE, "Synced %d operations",
               syncCount);
      msg = syncDone;
    }

    // Send response to client immediately (AP: no waiting)
    if (send(clientSocket, (char *)&msg, sizeof(Message), 0) == -1) {
      perror("Failed to send response");
      break;
    }
  }

  close(clientSocket);
}

// Handle follower connection and sync
void handleFollower(int followerSocket) {
  // First, sync the follower with current state
  Message syncMsg;
  if (recv(followerSocket, (char *)&syncMsg, sizeof(Message), 0) > 0) {
    if (syncMsg.cmd == CMD_SYNC) {
      int fromSeq = syncMsg.sequence;
      std::cout << "[LEADER-AP] New follower syncing from seq " << fromSeq
                << std::endl;

      // Send all operations since their last known sequence
      std::lock_guard<std::mutex> lock(logMutex);
      for (const auto &op : operationLog) {
        if (op.sequence > fromSeq) {
          send(followerSocket, (char *)&op, sizeof(Message), 0);
          usleep(1000); // Small delay to prevent overflow
        }
      }

      // Send current sequence number
      Message ack;
      ack.cmd = CMD_ACK;
      ack.sequence = logSequence;
      send(followerSocket, (char *)&ack, sizeof(Message), 0);
    }
  }

  // Add to active followers
  {
    std::lock_guard<std::mutex> lock(socketsMutex);
    followerSockets.push_back(followerSocket);
  }
  std::cout << "[LEADER-AP] Follower registered and synced" << std::endl;
}

void acceptFollowers(int registrationSocket) {
  struct sockaddr_in addr;
  socklen_t addrLen = sizeof(addr);

  while (true) {
    int followerSocket =
        accept(registrationSocket, (struct sockaddr *)&addr, &addrLen);
    if (followerSocket == -1) {
      perror("Failed to accept follower");
      continue;
    }

    // Handle follower sync in separate thread
    std::thread(handleFollower, followerSocket).detach();
  }
}

int main() {
  // macOS: Ignore SIGPIPE
  signal(SIGPIPE, SIG_IGN);

  std::cout << "========================================" << std::endl;
  std::cout << "   AP SYSTEM - Availability Priority   " << std::endl;
  std::cout << "========================================" << std::endl;
  std::cout << "- Responds immediately (no ACK wait)" << std::endl;
  std::cout << "- Eventual consistency via operation log" << std::endl;
  std::cout << "- Followers sync on reconnect" << std::endl;
  std::cout << "========================================" << std::endl;

  // Create registration socket for followers
  int regSocket = socket(AF_INET, SOCK_STREAM, 0);

  int opt = 1;
  setsockopt(regSocket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

  sockaddr_in regAddr = {};
  regAddr.sin_family = AF_INET;
  regAddr.sin_port = htons(8080);
  inet_pton(AF_INET, "127.0.0.1", &regAddr.sin_addr);

  if (bind(regSocket, (struct sockaddr *)&regAddr, sizeof(regAddr)) == -1) {
    perror("Failed to bind registration socket");
    return 1;
  }

  listen(regSocket, 10);
  std::cout << "[LEADER-AP] Follower registration on port 8080" << std::endl;

  std::thread followerAcceptThread(acceptFollowers, regSocket);
  followerAcceptThread.detach();

  // Create client socket
  int clientSocket = socket(AF_INET, SOCK_STREAM, 0);

  setsockopt(clientSocket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

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
  std::cout << "[LEADER-AP] Client connections on port 8000" << std::endl;

  while (true) {
    struct sockaddr_in addr;
    socklen_t addrLen = sizeof(addr);

    int connSocket = accept(clientSocket, (struct sockaddr *)&addr, &addrLen);
    if (connSocket == -1) {
      perror("Failed to accept client");
      continue;
    }

    std::thread clientThread(handleClient, connSocket);
    clientThread.detach();
  }

  close(clientSocket);
  close(regSocket);
  return 0;
}
