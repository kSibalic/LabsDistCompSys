#include <iostream>
#include <string>
#include <thread>
#include <vector>
#include <map>
#include <set>
#include <mutex>
#include <algorithm>
#include <cstring>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

constexpr int PORT = 8080;
constexpr int BUFFER_SIZE = 4096;

std::mutex topics_mutex;
std::map<std::string, std::vector<int>> topics;
std::map<int, std::set<std::string>> client_topics;

void remove_client_from_topic_nolock(const std::string &topic, int client_fd) {
    auto it = topics.find(topic);
    if (it == topics.end()) return;
    auto &vec = it->second;
    vec.erase(std::remove(vec.begin(), vec.end(), client_fd), vec.end());
}

void cleanup_client(int client_fd) {
    std::lock_guard<std::mutex> lock(topics_mutex);
    auto it = client_topics.find(client_fd);
    if (it != client_topics.end()) {
        for (const auto &topic : it->second) {
            remove_client_from_topic_nolock(topic, client_fd);
        }
        client_topics.erase(it);
    }

    for (auto & [topic, vec] : topics) {
        vec.erase(std::remove(vec.begin(), vec.end(), client_fd), vec.end());
    }
}

void send_line_to_client(int client_fd, const std::string &line) {
    std::string out = line;
    if (!out.empty() && out.back() != '\n') out.push_back('\n');

    ssize_t sent = send(client_fd, out.c_str(), (size_t)out.size(), 0);
    if (sent <= 0) { }
}

void handle_client(int client_fd) {
    char buf[BUFFER_SIZE];
    std::string backlog;

    while (true) {
        ssize_t r = read(client_fd, buf, sizeof(buf));
        if (r > 0) {
            backlog.append(buf, buf + r);

            size_t pos;
            while ((pos = backlog.find('\n')) != std::string::npos) {
                std::string line = backlog.substr(0, pos);
                if (!line.empty() && line.back() == '\r') line.pop_back();
                backlog.erase(0, pos + 1);

                if (line.empty()) continue;

                std::string cmd;
                {
                    size_t p = line.find(' ');
                    if (p == std::string::npos) cmd = line;
                    else cmd = line.substr(0, p);
                }

                if (cmd == "SUBSCRIBE") {
                    size_t p = line.find(' ');
                    if (p == std::string::npos) {
                        send_line_to_client(client_fd, "ERROR: SUBSCRIBE requires a topic");
                        continue;
                    }
                    std::string topic = line.substr(p + 1);
                    if (topic.empty()) {
                        send_line_to_client(client_fd, "ERROR: empty topic");
                        continue;
                    }

                    {
                        std::lock_guard<std::mutex> lock(topics_mutex);
                        auto &vec = topics[topic];
                        if (std::find(vec.begin(), vec.end(), client_fd) == vec.end())
                            vec.push_back(client_fd);
                        client_topics[client_fd].insert(topic);
                    }

                    send_line_to_client(client_fd, std::string("Subscribed to ") + topic);
                    std::cout << "Client " << client_fd << " subscribed to '" << topic << "'\n";
                }
                else if (cmd == "UNSUBSCRIBE") {
                    size_t p = line.find(' ');
                    if (p == std::string::npos) {
                        send_line_to_client(client_fd, "ERROR: UNSUBSCRIBE requires a topic");
                        continue;
                    }
                    std::string topic = line.substr(p + 1);
                    if (topic.empty()) {
                        send_line_to_client(client_fd, "ERROR: empty topic");
                        continue;
                    }

                    {
                        std::lock_guard<std::mutex> lock(topics_mutex);
                        remove_client_from_topic_nolock(topic, client_fd);
                        auto it = client_topics.find(client_fd);
                        if (it != client_topics.end()) it->second.erase(topic);
                    }

                    send_line_to_client(client_fd, std::string("Unsubscribed from ") + topic);
                    std::cout << "Client " << client_fd << " unsubscribed from '" << topic << "'\n";
                }
                else if (cmd == "PUBLISH") {
                    size_t p = line.find(' ');
                    if (p == std::string::npos) {
                        send_line_to_client(client_fd, "ERROR: PUBLISH requires topic and message");
                        continue;
                    }
                    size_t q = line.find(' ', p + 1);
                    if (q == std::string::npos) {
                        send_line_to_client(client_fd, "ERROR: PUBLISH requires topic and message");
                        continue;
                    }
                    std::string topic = line.substr(p + 1, q - (p + 1));
                    std::string message = line.substr(q + 1);
                    if (topic.empty()) {
                        send_line_to_client(client_fd, "ERROR: empty topic");
                        continue;
                    }

                    std::string payload = std::string("[") + topic + "] " + message;

                    std::vector<int> dead_clients;
                    {
                        std::lock_guard<std::mutex> lock(topics_mutex);
                        auto it = topics.find(topic);
                        if (it == topics.end() || it->second.empty()) {
                            send_line_to_client(client_fd, std::string("Published to '") + topic + "' (no subscribers)");
                            std::cout << "Client " << client_fd << " published to '" << topic << "' but no subscribers\n";
                            continue;
                        }

                        auto &subs = it->second;
                        for (int subfd : subs) {
                            if (subfd == client_fd) {
                            }

                            ssize_t sent = send(subfd, (payload + "\n").c_str(), payload.size() + 1, 0);
                            if (sent <= 0) {
                                dead_clients.push_back(subfd);
                            }
                        }

                        for (int dead : dead_clients) {
                            subs.erase(std::remove(subs.begin(), subs.end(), dead), subs.end());
                            auto itc = client_topics.find(dead);
                            if (itc != client_topics.end()) itc->second.erase(topic);
                        }
                    }

                    send_line_to_client(client_fd, std::string("Published to '") + topic + "'");
                    std::cout << "Client " << client_fd << " published to '" << topic << "': " << message << "\n";
                }
                else if (cmd == "LIST") {
                    if (line == "LIST TOPICS") {
                        std::lock_guard<std::mutex> lock(topics_mutex);
                        if (topics.empty()) {
                            send_line_to_client(client_fd, "No topics available");
                        } else {
                            send_line_to_client(client_fd, "Active topics:");
                            for (const auto &kv : topics) {
                                send_line_to_client(client_fd, "- " + kv.first);
                            }
                        }
                    } else {
                        send_line_to_client(client_fd, "ERROR: unknown LIST command");
                    }
                }
                else {
                    send_line_to_client(client_fd, "ERROR: unknown command");
                }
            }
        }
        else if (r == 0) {
            std::cout << "Client " << client_fd << " disconnected (EOF)\n";
            break;
        }
        else {
            perror("read");
            break;
        }
    }

    cleanup_client(client_fd);
    close(client_fd);
}

int main() {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        perror("socket");
        return 1;
    }

    int opt = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("setsockopt");
        close(server_fd);
        return 1;
    }

    sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(PORT);

    if (bind(server_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        perror("bind");
        close(server_fd);
        return 1;
    }

    if (listen(server_fd, 10) < 0) {
        perror("listen");
        close(server_fd);
        return 1;
    }

    std::cout << "PubSub server listening on port " << PORT << "\n";

    std::vector<std::thread> workers;
    while (true) {
        int client_fd = accept(server_fd, nullptr, nullptr);
        if (client_fd < 0) {
            perror("accept");
            continue;
        }

        std::cout << "New client connected: fd=" << client_fd << "\n";
        workers.emplace_back(std::thread(handle_client, client_fd));
        workers.back().detach();
    }

    close(server_fd);
    return 0;
}
