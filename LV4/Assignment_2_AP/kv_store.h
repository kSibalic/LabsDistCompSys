#ifndef KV_STORE_H
#define KV_STORE_H

#include <cstring>
#include <iostream>
#include <string>
#include <unordered_map>

// Maximum sizes for protocol messages
#define MAX_KEY_SIZE 256
#define MAX_VALUE_SIZE 4096
#define MAX_SOCKET_PATH 256

// Command types in the replication protocol
enum CommandType {
  CMD_SET = 1,    // Set key-value pair
  CMD_GET = 2,    // Get value for key
  CMD_DELETE = 3, // Delete key
  CMD_ACK = 4,    // Acknowledgment
  CMD_SYNC = 5,   // Sync request from follower
  CMD_LIST = 6,   // List keys and values on nodes
};

// Message structure sent over sockets
// This is the protocol for communication
struct Message {
  CommandType cmd;
  char key[MAX_KEY_SIZE];
  char value[MAX_VALUE_SIZE];
  char response[MAX_VALUE_SIZE];
  int status;   // 0 = success, -1 = error
  int sequence; // Sequence number for eventual consistency

  Message() : cmd(CMD_SET), status(0), sequence(0) {
    memset(key, 0, MAX_KEY_SIZE);
    memset(value, 0, MAX_VALUE_SIZE);
    memset(response, 0, MAX_VALUE_SIZE);
  }
};

// Simple in-memory key-value store
class KeyValueStore {
private:
  std::unordered_map<std::string, std::string> data;

public:
  // Set a key-value pair
  void set(const std::string &key, const std::string &value) {
    data[key] = value;
  }

  // Get value for a key
  bool get(const std::string &key, std::string &value) const {
    auto it = data.find(key);
    if (it != data.end()) {
      value = it->second;
      return true;
    }
    return false;
  }

  // Delete a key
  bool deleteKey(const std::string &key) { return data.erase(key) > 0; }

  // Get all data (for synchronization)
  const std::unordered_map<std::string, std::string> &getAllData() const {
    return data;
  }
};

#endif // KV_STORE_H
