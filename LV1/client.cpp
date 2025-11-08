#include <iostream>
#include <string>
#include <thread>
#include <cstring>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>

constexpr int PORT = 8080;
constexpr int BUFFER_SIZE = 4096;

void listener_thread_fn(int sockfd) {
    char buf[BUFFER_SIZE];
    std::string backlog;
    while (true) {
        ssize_t r = read(sockfd, buf, sizeof(buf));
        if (r > 0) {
            backlog.append(buf, buf + r);
            size_t pos;
            while ((pos = backlog.find('\n')) != std::string::npos) {
                std::string line = backlog.substr(0, pos);
                if (!line.empty() && line.back() == '\r') line.pop_back();
                backlog.erase(0, pos + 1);
                std::cout << "\n[SERVER] " << line << "\n> " << std::flush;
            }
        } else if (r == 0) {
            std::cout << "\nServer closed the connection.\n";
            break;
        } else {
            perror("read");
            break;
        }
    }
}

int main() {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("socket");
        return 1;
    }

    sockaddr_in serv{};
    serv.sin_family = AF_INET;
    serv.sin_port = htons(PORT);
    if (inet_pton(AF_INET, "127.0.0.1", &serv.sin_addr) <= 0) {
        perror("inet_pton");
        close(sockfd);
        return 1;
    }

    if (connect(sockfd, reinterpret_cast<sockaddr*>(&serv), sizeof(serv)) < 0) {
        perror("connect");
        close(sockfd);
        return 1;
    }

    std::cout << "Connected to server at 127.0.0.1:" << PORT << "\n";
    std::thread listener(listener_thread_fn, sockfd);
    listener.detach();

    while (true) {
        std::cout << "> " << std::flush;
        std::string line;
        if (!std::getline(std::cin, line)) break;

        if (line == "exit" || line == "quit") {
            std::cout << "Closing client.\n";
            break;
        }

        std::string out = line;
        if (out.empty()) continue;
        if (out.back() != '\n') out.push_back('\n');

        ssize_t s = send(sockfd, out.c_str(), out.size(), 0);
        if (s <= 0) {
            perror("send");
            break;
        }
    }

    close(sockfd);
    return 0;
}
