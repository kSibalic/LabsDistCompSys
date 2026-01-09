#include "kv_store.h"
#include <algorithm>
#include <arpa/inet.h>
#include <atomic>
#include <csignal>
#include <fstream>
#include <iostream>
#include <netinet/in.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

KeyValueStore store;
int followerId = 0;
std::atomic<int> lastSequence{0};

void requestSync(int leaderSocket) {
  Message syncReq;
  syncReq.cmd = CMD_SYNC;
  syncReq.sequence = lastSequence;
  send(leaderSocket, (char *)&syncReq, sizeof(Message), 0);

  Message msg;
  while (true) {
    if (recv(leaderSocket, (char *)&msg, sizeof(Message), 0) <= 0)
      break;
    if (msg.cmd == CMD_ACK) {
      lastSequence = msg.sequence;
      std::cout << "[FOLLOWER] Sync complete. Seq: " << lastSequence
                << std::endl;
      break;
    }
    std::string key(msg.key);
    std::string value(msg.value);
    if (msg.cmd == CMD_SET) {
      // Use timestamp for conflict resolution
      if (store.set(key, value, msg.timestamp)) {
        std::cout << "[FOLLOWER] Synced SET " << key << " = " << value
                  << " (ts: " << msg.timestamp << ")" << std::endl;
      }
    }
    if (msg.sequence > lastSequence)
      lastSequence = msg.sequence;
  }
}

void listenForUpdates(int leaderSocket) {
  Message msg;
  std::cout << "[FOLLOWER] Listening for updates..." << std::endl;
  while (true) {
    if (recv(leaderSocket, (char *)&msg, sizeof(Message), 0) <= 0) {
      std::cout << "[FOLLOWER] Connection lost" << std::endl;
      break;
    }
    std::string key(msg.key);
    std::string value(msg.value);
    if (msg.cmd == CMD_SET) {
      if (store.set(key, value, msg.timestamp)) {
        std::cout << "[FOLLOWER] Applied SET " << key << " = " << value
                  << " (ts: " << msg.timestamp << ")" << std::endl;
      }
      lastSequence = msg.sequence;
    }
  }
  close(leaderSocket);
}

int main(int argc, char *argv[]) {
  signal(SIGPIPE, SIG_IGN);
  if (argc > 1)
    followerId = atoi(argv[1]);

  std::cout << "=== BONUS FOLLOWER " << followerId
            << " (LWW Conflict Resolution) ===" << std::endl;

  while (true) {
    int regSocket = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(8080);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    if (connect(regSocket, (struct sockaddr *)&addr, sizeof(addr)) == 0) {
      std::cout << "[FOLLOWER] Connected to leader" << std::endl;
      requestSync(regSocket);
      listenForUpdates(regSocket);
    } else {
      std::cout << "[FOLLOWER] Waiting for leader..." << std::endl;
      close(regSocket);
      sleep(2);
    }
  }
  return 0;
}
