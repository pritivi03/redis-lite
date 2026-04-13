#include <arpa/inet.h>
#include <charconv>
#include <cctype>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <netdb.h>
#include <string>
#include <sys/socket.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace {

bool read_crlf_line(const std::string &s, size_t &pos, std::string &line) {
  size_t start = pos;
  while (pos < s.size() && s[pos] != '\r') {
    ++pos;
  }
  if (pos + 1 >= s.size() || s[pos] != '\r' || s[pos + 1] != '\n') {
    return false;
  }
  line.assign(s.data() + start, pos - start);
  pos += 2;
  return true;
}

bool parse_int64(const std::string &line, long long &out) {
  const char *begin = line.data();
  const char *end = line.data() + line.size();
  auto result = std::from_chars(begin, end, out);
  if (result.ec != std::errc{} || result.ptr != end) {
    return false;
  }
  return true;
}

bool parse_bulk_string(const std::string &s, size_t &pos, std::string &out) {
  if (pos >= s.size() || s[pos] != '$') {
    return false;
  }
  ++pos;
  std::string len_line;
  if (!read_crlf_line(s, pos, len_line)) {
    return false;
  }
  long long len = 0;
  if (!parse_int64(len_line, len)) {
    return false;
  }
  if (len < -1) {
    return false;
  }
  if (len == -1) {
    out.clear();
    return true;
  }
  if (static_cast<size_t>(len) > s.size() - pos) {
    return false;
  }
  out.assign(s.data() + pos, static_cast<size_t>(len));
  pos += static_cast<size_t>(len);
  if (pos + 1 >= s.size() || s[pos] != '\r' || s[pos + 1] != '\n') {
    return false;
  }
  pos += 2;
  return true;
}

// RESP arrays sent by redis-cli for commands are *-prefixed arrays of bulk strings.
bool parse_array_of_bulk_strings(const std::string &s, size_t &pos,
                                 std::vector<std::string> &args) {
  if (pos >= s.size() || s[pos] != '*') {
    return false;
  }
  ++pos;
  std::string count_line;
  if (!read_crlf_line(s, pos, count_line)) {
    return false;
  }
  long long count = 0;
  if (!parse_int64(count_line, count) || count < 0) {
    return false;
  }
  args.clear();
  args.reserve(static_cast<size_t>(count));
  for (long long i = 0; i < count; ++i) {
    std::string elem;
    if (!parse_bulk_string(s, pos, elem)) {
      return false;
    }
    args.push_back(std::move(elem));
  }
  return true;
}

void send_all(int fd, const std::string &data) {
  const char *p = data.data();
  size_t left = data.size();
  while (left > 0) {
    ssize_t n = send(fd, p, left, 0);
    if (n <= 0) {
      return;
    }
    p += static_cast<size_t>(n);
    left -= static_cast<size_t>(n);
  }
}

std::string bulk_reply(const std::string &payload) {
  return std::string("$") + std::to_string(payload.size()) + "\r\n" + payload +
         "\r\n";
}

bool iequals(const std::string &a, const std::string &b) {
  if (a.size() != b.size()) {
    return false;
  }
  for (size_t i = 0; i < a.size(); ++i) {
    if (std::tolower(static_cast<unsigned char>(a[i])) !=
        std::tolower(static_cast<unsigned char>(b[i]))) {
      return false;
    }
  }
  return true;
}

void dispatch_command(int client_fd, const std::vector<std::string> &args) {
  if (args.empty()) {
    send_all(client_fd, "-ERR empty command\r\n");
    return;
  }

  if (iequals(args[0], "ping")) {
    if (args.size() == 1) {
      send_all(client_fd, "+PONG\r\n");
    } else if (args.size() == 2) {
      send_all(client_fd, bulk_reply(args[1]));
    } else {
      send_all(client_fd, "-ERR wrong number of arguments for 'ping' command\r\n");
    }
    return;
  }

  if (iequals(args[0], "echo")) {
    if (args.size() != 2) {
      send_all(client_fd,
               "-ERR wrong number of arguments for 'echo' command\r\n");
      return;
    }
    send_all(client_fd, bulk_reply(args[1]));
    return;
  }

  send_all(client_fd, "-ERR unknown command '" + args[0] + "'\r\n");
}

} // namespace

void handleClient(int client_fd) {
  const size_t BUFFER_SIZE = 4096;
  char buffer[BUFFER_SIZE];
  std::string recv_buf;
  recv_buf.reserve(4096);

  while (true) {
    ssize_t bytes_read = read(client_fd, buffer, BUFFER_SIZE);

    if (bytes_read == -1) {
      perror("Error reading from file descriptor");
      break;
    }
    if (bytes_read == 0) {
      break;
    }

    recv_buf.append(buffer, static_cast<size_t>(bytes_read));

    size_t consumed = 0;
    while (consumed < recv_buf.size()) {
      if (recv_buf[consumed] != '*') {
        send_all(client_fd, "-ERR protocol error\r\n");
        recv_buf.clear();
        consumed = 0;
        break;
      }

      size_t try_pos = consumed;
      std::vector<std::string> args;
      if (!parse_array_of_bulk_strings(recv_buf, try_pos, args)) {
        break;
      }

      consumed = try_pos;
      dispatch_command(client_fd, args);
    }

    recv_buf.erase(0, consumed);
  }

  close(client_fd);
}

int main([[maybe_unused]] int argc, [[maybe_unused]] char **argv) {
  std::cout << std::unitbuf;
  std::cerr << std::unitbuf;

  int server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0) {
    std::cerr << "Failed to create server socket\n";
    return 1;
  }

  int reuse = 1;
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) <
      0) {
    std::cerr << "setsockopt failed\n";
    return 1;
  }

  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(6379);

  if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) !=
      0) {
    std::cerr << "Failed to bind to port 6379\n";
    return 1;
  }

  int connection_backlog = 5;
  if (listen(server_fd, connection_backlog) != 0) {
    std::cerr << "listen failed\n";
    return 1;
  }

  struct sockaddr_in client_addr;
  int client_addr_len = sizeof(client_addr);
  std::cout << "Waiting for clients to connect...\n";

  while (true) {
    int client_fd = accept(server_fd, (struct sockaddr *)&client_addr,
                           (socklen_t *)&client_addr_len);
    if (client_fd < 0) {
      std::cerr << "Failed to accept client connection\n";
      return 1;
    }

    std::cout << "Client connected\n";
    std::thread client_thread(handleClient, client_fd);
    client_thread.detach();
  }
  close(server_fd);

  return 0;
}
