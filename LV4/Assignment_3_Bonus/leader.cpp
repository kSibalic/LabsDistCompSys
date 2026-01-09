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

std::vector<Message> operationLog;
std::mutex logMutex;
std::atomic<int> logSequence{0};

void broadcastToFollowersAsync(const Message &msg) {
  std::lock_guard<std::mutex> lock(socketsMutex);
  std::vector<int> deadFollowers;
  for (int followerSocket : followerSockets) {
    int result = send(followerSocket, (const char *)&msg, sizeof(Message), 0);
    if (result == -1)
      deadFollowers.push_back(followerSocket);
  }
  for (int dead : deadFollowers) {
    followerSockets.erase(
        std::remove(followerSockets.begin(), followerSockets.end(), dead),
        followerSockets.end());
    close(dead);
  }
}

void handleClient(int clientSocket) {
  Message msg;
  while (true) {
    if (recv(clientSocket, (char *)&msg, sizeof(Message), 0) <= 0)
      break;

    std::string key(msg.key);
    std::string value(msg.value);

    // Generate Header Timestamp if 0 (local write)
    if (msg.timestamp == 0) {
      msg.timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::system_clock::now().time_since_epoch())
                          .count();
    }

    if (msg.cmd == CMD_SET) {
      store.set(key, value, msg.timestamp);
      msg.status = 0;
      msg.sequence = ++logSequence;
      snprintf(msg.response, MAX_VALUE_SIZE, "SET %s = %s (seq: %d, ts: %llu)",
               key.c_str(), value.c_str(), msg.sequence, msg.timestamp);
      {
        std::lock_guard<std::mutex> lock(logMutex);
        operationLog.push_back(msg);
      }
      std::thread([msg]() { broadcastToFollowersAsync(msg); }).detach();

    } else if (msg.cmd == CMD_GET) {
      std::string result;
      if (store.get(key, result)) {
        msg.status = 0;
        snprintf(msg.response, MAX_VALUE_SIZE, "%s", result.c_str());
      } else {
        msg.status = -1;
        snprintf(msg.response, MAX_VALUE_SIZE, "Key not found");
      }

    } else if (msg.cmd == CMD_DELETE) {
      store.deleteKey(key, msg.timestamp);
      msg.status = 0;
      msg.sequence = ++logSequence;
      snprintf(msg.response, MAX_VALUE_SIZE, "Deleted %s (seq: %d, ts: %llu)",
               key.c_str(), msg.sequence, msg.timestamp);
      {
        std::lock_guard<std::mutex> lock(logMutex);
        operationLog.push_back(msg);
      }
      std::thread([msg]() { broadcastToFollowersAsync(msg); }).detach();

    } else if (msg.cmd == CMD_SYNC) {
      int fromSeq = msg.sequence;
      std::cout << "[LEADER-BONUS] Sync request from seq " << fromSeq
                << std::endl;
      std::lock_guard<std::mutex> lock(logMutex);
      for (const auto &op : operationLog) {
        if (op.sequence > fromSeq) {
          send(clientSocket, (char *)&op, sizeof(Message), 0);
        }
      }
      Message syncDone;
      syncDone.cmd = CMD_ACK;
      syncDone.sequence = logSequence;
      msg = syncDone;
    }
    send(clientSocket, (char *)&msg, sizeof(Message), 0);
  }
  close(clientSocket);
}

void handleFollower(int followerSocket) {
  Message syncMsg;
  if (recv(followerSocket, (char *)&syncMsg, sizeof(Message), 0) > 0) {
    if (syncMsg.cmd == CMD_SYNC) {
      int fromSeq = syncMsg.sequence;
      std::cout << "[LEADER-BONUS] New follower syncing from seq " << fromSeq
                << std::endl;
      std::lock_guard<std::mutex> lock(logMutex);
      for (const auto &op : operationLog) {
        if (op.sequence > fromSeq) {
          send(followerSocket, (char *)&op, sizeof(Message), 0);
          usleep(1000);
        }
      }
      Message ack;
      ack.cmd = CMD_ACK;
      ack.sequence = logSequence;
      send(followerSocket, (char *)&ack, sizeof(Message), 0);
    }
  }
  {
    std::lock_guard<std::mutex> lock(socketsMutex);
    followerSockets.push_back(followerSocket);
  }
}

void acceptFollowers(int registrationSocket) {
  struct sockaddr_in addr;
  socklen_t addrLen = sizeof(addr);
  while (true) {
    int followerSocket =
        accept(registrationSocket, (struct sockaddr *)&addr, &addrLen);
    if (followerSocket >= 0)
      std::thread(handleFollower, followerSocket).detach();
  }
}

int main() {
  signal(SIGPIPE, SIG_IGN);
  std::cout << "=== BONUS: AP SYSTEM + CONFLICT RESOLUTION ===" << std::endl;

  int regSocket = socket(AF_INET, SOCK_STREAM, 0);
  int opt = 1;
  setsockopt(regSocket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
  sockaddr_in regAddr = {};
  regAddr.sin_family = AF_INET;
  regAddr.sin_port = htons(8080);
  inet_pton(AF_INET, "127.0.0.1", &regAddr.sin_addr);
  bind(regSocket, (struct sockaddr *)&regAddr, sizeof(regAddr));
  listen(regSocket, 10);

  std::thread(acceptFollowers, regSocket).detach();

  int clientSocket = socket(AF_INET, SOCK_STREAM, 0);
  setsockopt(clientSocket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
  sockaddr_in clientAddr = {};
  clientAddr.sin_family = AF_INET;
  clientAddr.sin_port = htons(8000);
  inet_pton(AF_INET, "127.0.0.1", &clientAddr.sin_addr);
  bind(clientSocket, (struct sockaddr *)&clientAddr, sizeof(clientAddr));
  listen(clientSocket, 10);

  std::cout << "[LEADER-BONUS] Listening on 8000 (Client) and 8080 (Follower)"
            << std::endl;

  while (true) {
    struct sockaddr_in addr;
    socklen_t addrLen = sizeof(addr);
    int connSocket = accept(clientSocket, (struct sockaddr *)&addr, &addrLen);
    if (connSocket >= 0)
      std::thread(handleClient, connSocket).detach();
  }
  return 0;
}
