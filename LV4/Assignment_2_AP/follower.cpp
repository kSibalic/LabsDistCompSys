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

// Save sequence to file for persistence across restarts
void saveSequence() {
  std::ofstream file("follower_" + std::to_string(followerId) + "_seq.txt");
  file << lastSequence.load();
}

// Load last known sequence from file
void loadSequence() {
  std::ifstream file("follower_" + std::to_string(followerId) + "_seq.txt");
  int seq = 0;
  if (file >> seq) {
    lastSequence = seq;
  }
}

// Request sync from leader to get missed operations (eventual consistency)
void requestSync(int leaderSocket) {
  Message syncReq;
  syncReq.cmd = CMD_SYNC;
  syncReq.sequence = lastSequence;

  std::cout << "[FOLLOWER-AP " << followerId << "] Requesting sync from seq "
            << lastSequence << std::endl;

  if (send(leaderSocket, (char *)&syncReq, sizeof(Message), 0) == -1) {
    perror("Failed to send sync request");
    return;
  }

  // Receive all missed operations
  Message msg;
  while (true) {
    int bytes = recv(leaderSocket, (char *)&msg, sizeof(Message), 0);
    if (bytes <= 0)
      break;

    if (msg.cmd == CMD_ACK) {
      // Sync complete
      lastSequence = msg.sequence;
      saveSequence();
      std::cout << "[FOLLOWER-AP " << followerId
                << "] Sync complete, now at seq " << lastSequence << std::endl;
      break;
    }

    // Apply missed operation
    std::string key(msg.key);
    std::string value(msg.value);

    if (msg.cmd == CMD_SET) {
      store.set(key, value);
      std::cout << "[FOLLOWER-AP " << followerId << "] Synced SET " << key
                << " = " << value << " (seq: " << msg.sequence << ")"
                << std::endl;
    } else if (msg.cmd == CMD_DELETE) {
      store.deleteKey(key);
      std::cout << "[FOLLOWER-AP " << followerId << "] Synced DELETE " << key
                << " (seq: " << msg.sequence << ")" << std::endl;
    }

    if (msg.sequence > lastSequence) {
      lastSequence = msg.sequence;
    }
  }
}

// Listen for replication updates from leader
void listenForUpdates(int leaderSocket) {
  Message msg;

  std::cout << "[FOLLOWER-AP " << followerId << "] Connected and listening"
            << std::endl;

  while (true) {
    int bytesReceived = recv(leaderSocket, (char *)&msg, sizeof(Message), 0);

    if (bytesReceived <= 0) {
      std::cout << "[FOLLOWER-AP " << followerId
                << "] Lost connection to leader" << std::endl;
      break;
    }

    std::string key(msg.key);
    std::string value(msg.value);

    if (msg.cmd == CMD_SET) {
      store.set(key, value);
      lastSequence = msg.sequence;
      saveSequence();
      std::cout << "[FOLLOWER-AP " << followerId << "] Applied SET " << key
                << " = " << value << " (seq: " << msg.sequence << ")"
                << std::endl;

    } else if (msg.cmd == CMD_DELETE) {
      store.deleteKey(key);
      lastSequence = msg.sequence;
      saveSequence();
      std::cout << "[FOLLOWER-AP " << followerId << "] Applied DELETE " << key
                << " (seq: " << msg.sequence << ")" << std::endl;

    } else if (msg.cmd == CMD_LIST) {
      const auto &data = store.getAllData();
      std::cout << "[FOLLOWER-AP " << followerId
                << "] Current data:" << std::endl;
      for (const auto &[k, v] : data) {
        std::cout << "  " << k << ": " << v << std::endl;
      }
    }
  }

  close(leaderSocket);
}

int connectToLeader() {
  int regSocket = socket(AF_INET, SOCK_STREAM, 0);
  if (regSocket == -1) {
    perror("Failed to create socket");
    return -1;
  }

  sockaddr_in addr = {};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(8080);
  inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

  if (connect(regSocket, (struct sockaddr *)&addr, sizeof(addr)) == 0) {
    return regSocket;
  }

  close(regSocket);
  return -1;
}

int main(int argc, char *argv[]) {
  // macOS: Ignore SIGPIPE
  signal(SIGPIPE, SIG_IGN);

  if (argc > 1) {
    followerId = atoi(argv[1]);
  }

  std::cout << "========================================" << std::endl;
  std::cout << "  AP FOLLOWER " << followerId << " - Eventual Consistency"
            << std::endl;
  std::cout << "========================================" << std::endl;

  // Load last known sequence (for reconnection sync)
  loadSequence();
  std::cout << "[FOLLOWER-AP " << followerId
            << "] Last known seq: " << lastSequence << std::endl;

  // Retry connection with exponential backoff
  while (true) {
    int leaderSocket = -1;
    int retryDelay = 1;

    // Try to connect to leader
    for (int i = 0; i < 10; i++) {
      leaderSocket = connectToLeader();
      if (leaderSocket >= 0)
        break;

      std::cout << "[FOLLOWER-AP " << followerId
                << "] Waiting for leader... (retry in " << retryDelay << "s)"
                << std::endl;
      sleep(retryDelay);
      retryDelay = std::min(retryDelay * 2, 30);
    }

    if (leaderSocket < 0) {
      std::cout << "[FOLLOWER-AP " << followerId
                << "] Could not connect to leader" << std::endl;
      continue;
    }

    // Request sync for missed operations (eventual consistency)
    requestSync(leaderSocket);

    // Listen for new updates
    listenForUpdates(leaderSocket);

    // Connection lost - will retry
    std::cout << "[FOLLOWER-AP " << followerId
              << "] Will attempt reconnection..." << std::endl;
    sleep(2);
  }

  return 0;
}
