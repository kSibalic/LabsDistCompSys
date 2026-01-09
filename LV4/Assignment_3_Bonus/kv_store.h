#ifndef KV_STORE_H
#define KV_STORE_H

#include <chrono>
#include <cstring>
#include <iostream>
#include <string>
#include <unordered_map>

// Maximum sizes
#define MAX_KEY_SIZE 256
#define MAX_VALUE_SIZE 4096
#define MAX_SOCKET_PATH 256

enum CommandType {
  CMD_SET = 1,
  CMD_GET = 2,
  CMD_DELETE = 3,
  CMD_ACK = 4,
  CMD_SYNC = 5,
  CMD_LIST = 6,
};

// Message with Timestamp for Conflict Resolution
struct Message {
  CommandType cmd;
  char key[MAX_KEY_SIZE];
  char value[MAX_VALUE_SIZE];
  char response[MAX_VALUE_SIZE];
  int status;
  int sequence;
  uint64_t timestamp; // Time in milliseconds

  Message() : cmd(CMD_SET), status(0), sequence(0), timestamp(0) {
    memset(key, 0, MAX_KEY_SIZE);
    memset(value, 0, MAX_VALUE_SIZE);
    memset(response, 0, MAX_VALUE_SIZE);
  }
};

// Key-Value Store with Conflict Resolution (LWW)
class KeyValueStore {
private:
  struct ValueEntry {
    std::string value;
    uint64_t timestamp;
  };
  std::unordered_map<std::string, ValueEntry> data;

public:
  // Set with Conflict Resolution
  // Returns true if updated, false if rejected (older timestamp)
  bool set(const std::string &key, const std::string &value, uint64_t ts) {
    auto it = data.find(key);
    if (it != data.end()) {
      // Conflict detected: Last Write Wins (LWW)
      if (ts < it->second.timestamp) {
        // Incoming is older, ignore
        std::cout << "[STORE] Conflict resolved: Ignoring SET " << key
                  << " (Existing TS: " << it->second.timestamp
                  << " > New TS: " << ts << ")" << std::endl;
        return false;
      }
    }
    data[key] = {value, ts};
    return true;
  }

  // Overload for local operations (generates timestamp)
  void set(const std::string &key, const std::string &value) {
    uint64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count();
    set(key, value, now);
  }

  bool get(const std::string &key, std::string &value) const {
    auto it = data.find(key);
    if (it != data.end()) {
      value = it->second.value;
      return true;
    }
    return false;
  }

  bool deleteKey(const std::string &key, uint64_t ts) {
    auto it = data.find(key);
    if (it != data.end()) {
      if (ts < it->second.timestamp) {
        return false;
      }
      data.erase(key);
      return true;
    }
    return false; // Already gone, effectively success
  }

  const std::unordered_map<std::string, ValueEntry> &getAllData() const {
    return data;
  }
};

#endif // KV_STORE_H
