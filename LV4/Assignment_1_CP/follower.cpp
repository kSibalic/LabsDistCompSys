#include "kv_store.h"
#include <arpa/inet.h>
#include <csignal>
#include <iostream>
#include <netinet/in.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

KeyValueStore store;
int followerId = 0;

// Send ACK back to leader
void sendAck(int leaderSocket, uint64_t sequence) {
  Message ack;
  ack.cmd = CMD_ACK;
  ack.sequence = sequence;
  ack.followerId = followerId;
  ack.status = 0;

  if (send(leaderSocket, (char *)&ack, sizeof(Message), 0) == -1) {
    perror("Failed to send ACK");
  } else {
    std::cout << "[FOLLOWER " << followerId << "] Sent ACK for seq " << sequence
              << std::endl;
  }
}

// Listen for replication updates from leader
void listenForUpdates(int leaderSocket) {
  Message msg;

  std::cout << "[FOLLOWER " << followerId
            << "] Connected to leader, waiting for updates..." << std::endl;
  std::cout << std::endl;

  while (true) {
    // Receive replication message from leader
    int bytesReceived = recv(leaderSocket, (char *)&msg, sizeof(Message), 0);

    if (bytesReceived <= 0) {
      std::cout << "[FOLLOWER " << followerId << "] Lost connection to leader"
                << std::endl;
      break;
    }

    // Apply the operation locally
    std::string key(msg.key);
    std::string value(msg.value);

    if (msg.cmd == CMD_SET) {
      store.set(key, value);
      std::cout << "[FOLLOWER " << followerId << "] Applied SET " << key
                << " = " << value << " (seq: " << msg.sequence << ")"
                << std::endl;

      // Send ACK back to leader
      sendAck(leaderSocket, msg.sequence);

    } else if (msg.cmd == CMD_DELETE) {
      store.deleteKey(key);
      std::cout << "[FOLLOWER " << followerId << "] Applied DELETE " << key
                << " (seq: " << msg.sequence << ")" << std::endl;

      // Send ACK back to leader
      sendAck(leaderSocket, msg.sequence);

    } else if (msg.cmd == CMD_LIST) {
      std::cout << "[FOLLOWER " << followerId << "] Current data:" << std::endl;
      const std::unordered_map<std::string, std::string> data =
          store.getAllData();
      for (auto const &[k, v] : data) {
        std::cout << "  " << k << " = " << v << std::endl;
      }
    }
  }

  close(leaderSocket);
}

int main(int argc, char *argv[]) {
  // macOS: Ignore SIGPIPE
  signal(SIGPIPE, SIG_IGN);

  if (argc > 1) {
    followerId = atoi(argv[1]);
  }

  std::cout << "========================================" << std::endl;
  std::cout << "   CP System Follower " << followerId << std::endl;
  std::cout << "========================================" << std::endl;
  std::cout << std::endl;

  // Connect to leader's registration socket
  int regSocket = socket(AF_INET, SOCK_STREAM, 0);
  if (regSocket == -1) {
    perror("Failed to create socket");
    return 1;
  }

  sockaddr_in addr = {};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(8080);
  inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

  // Retry connection (leader might not be running yet)
  bool connected = false;
  for (int i = 0; i < 10; i++) {
    if (connect(regSocket, (struct sockaddr *)&addr, sizeof(addr)) == 0) {
      connected = true;
      break;
    }
    std::cout << "[FOLLOWER " << followerId << "] Waiting for leader (attempt "
              << (i + 1) << "/10)..." << std::endl;
    sleep(1);
  }

  if (!connected) {
    std::cout << "[FOLLOWER " << followerId << "] Could not connect to leader"
              << std::endl;
    return 1;
  }

  // Start listening for updates
  listenForUpdates(regSocket);

  return 0;
}
