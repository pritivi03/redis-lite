#include <algorithm>
#include <arpa/inet.h>
#include <cctype>
#include <charconv>
#include <chrono>
#include <climits>
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <iostream>
#include <memory>
#include <mutex>
#include <netdb.h>
#include <optional>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <vector>

namespace {

struct Entry {
  std::string value;
  std::optional<int64_t> expiry_at_ms; // absolute time, nullopt = no expiry
};

// Waiter registered by a BLPOP call. Protected by kv_store_mutex.
struct BlpopWaiter {
  std::string matched_key;
  std::string matched_value;
  bool done = false;
  std::condition_variable cv;
};

std::unordered_map<std::string, Entry> kv_store;
std::unordered_map<std::string, std::vector<std::string>> list_store;
// Per-key FIFO queue of clients blocked in BLPOP. Protected by kv_store_mutex.
std::unordered_map<std::string, std::deque<std::shared_ptr<BlpopWaiter>>>
    blpop_waiters;
std::mutex kv_store_mutex;

int64_t now_ms() {
  using namespace std::chrono;
  return duration_cast<milliseconds>(system_clock::now().time_since_epoch())
      .count();
}

bool safe_add_int64(int64_t a, int64_t b, int64_t &out) {
  if ((b > 0 && a > LLONG_MAX - b) || (b < 0 && a < LLONG_MIN - b)) {
    return false;
  }
  out = a + b;
  return true;
}

bool parse_positive_int64(const std::string &s, int64_t &out) {
  const char *begin = s.data();
  const char *end = s.data() + s.size();
  long long parsed = 0;
  auto result = std::from_chars(begin, end, parsed);
  if (result.ec != std::errc{} || result.ptr != end || parsed <= 0) {
    return false;
  }
  out = static_cast<int64_t>(parsed);
  return true;
}

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

// Signed integer for command args (e.g. LRANGE start/stop: 0, negative
// indices).
bool parse_signed_int64(const std::string &s, int64_t &out) {
  long long parsed = 0;
  if (!parse_int64(s, parsed)) {
    return false;
  }
  out = static_cast<int64_t>(parsed);
  return true;
}

// Redis LRANGE index rules: negative offsets from end, then clamp, then empty
// if start > end.
bool lrange_normalized_range(int64_t raw_start, int64_t raw_end, size_t len,
                             size_t &out_start, size_t &out_end_inclusive) {
  if (len == 0) {
    return false;
  }

  const int64_t n = static_cast<int64_t>(len);
  int64_t start = raw_start;
  int64_t end = raw_end;

  if (start < 0) {
    start += n;
  }
  if (end < 0) {
    end += n;
  }
  if (start < 0) {
    start = 0;
  }
  if (end >= n) {
    end = n - 1;
  }
  if (start > end) {
    return false;
  }

  out_start = static_cast<size_t>(start);
  out_end_inclusive = static_cast<size_t>(end);
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

// RESP arrays sent by redis-cli for commands are *-prefixed arrays of bulk
// strings.
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

std::string encode_bulk_string(const std::optional<std::string_view> payload) {
  if (!payload.has_value()) {
    return "$-1\r\n";
  }

  return std::string("$") + std::to_string(payload->size()) + "\r\n" +
         std::string(*payload) + "\r\n";
}

std::string encode_integer(int64_t value) {
  return ":" + std::to_string(value) + "\r\n";
}

std::string encode_array(const std::vector<std::string_view> &elements) {
  if (elements.empty()) {
    return "*0\r\n";
  }
  std::string out = "*" + std::to_string(elements.size()) + "\r\n";
  for (const auto &element : elements) {
    out += encode_bulk_string(element);
  }
  return out;
}

// Must be called with kv_store_mutex held. Serves as many BLPOP waiters on
// `key` as there are elements in the list, in FIFO order.
void notify_blpop_waiters(const std::string &key) {
  auto wait_it = blpop_waiters.find(key);
  if (wait_it == blpop_waiters.end())
    return;

  auto list_it = list_store.find(key);
  if (list_it == list_store.end() || list_it->second.empty())
    return;

  auto &q = wait_it->second;
  auto &lst = list_it->second;

  while (!q.empty() && !lst.empty()) {
    auto waiter = q.front();
    q.pop_front();
    if (waiter->done)
      continue; // already served by another key or timed out

    waiter->matched_key = key;
    waiter->matched_value = std::move(lst.front());
    lst.erase(lst.begin());
    waiter->done = true;
    waiter->cv.notify_one();
  }

  if (q.empty())
    blpop_waiters.erase(key);
  if (lst.empty())
    list_store.erase(key);
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
      send_all(client_fd, encode_bulk_string(args[1]));
    } else {
      send_all(client_fd,
               "-ERR wrong number of arguments for 'ping' command\r\n");
    }
    return;
  }

  if (iequals(args[0], "set")) {
    if (args.size() != 3 && args.size() != 5) {
      send_all(client_fd,
               "-ERR wrong number of arguments for 'set' command\r\n");
      return;
    }

    std::optional<int64_t> expiry_at_ms = std::nullopt;
    if (args.size() == 5) {
      int64_t ttl = 0;
      if (!parse_positive_int64(args[4], ttl)) {
        send_all(client_fd, "-ERR invalid expire time in 'set' command\r\n");
        return;
      }

      int64_t ttl_ms = 0;
      if (iequals(args[3], "EX")) {
        if (ttl > LLONG_MAX / 1000) {
          send_all(client_fd, "-ERR invalid expire time in 'set' command\r\n");
          return;
        }
        ttl_ms = ttl * 1000;
      } else if (iequals(args[3], "PX")) {
        ttl_ms = ttl;
      } else {
        send_all(client_fd, "-ERR wrong time_variant for 'set' command\r\n");
        return;
      }

      int64_t absolute_expiry = 0;
      if (!safe_add_int64(now_ms(), ttl_ms, absolute_expiry)) {
        send_all(client_fd, "-ERR invalid expire time in 'set' command\r\n");
        return;
      }
      expiry_at_ms = absolute_expiry;
    }

    {
      std::lock_guard<std::mutex> lock(kv_store_mutex);
      std::string key = args[1];
      std::string value = args[2];
      kv_store[key] = Entry{value, expiry_at_ms};
    }

    send_all(client_fd, "+OK\r\n");
    return;
  }

  if (iequals(args[0], "get")) {
    if (args.size() != 2) {
      send_all(client_fd,
               "-ERR wrong number of arguments for 'get' command\r\n");
      return;
    }

    std::optional<std::string_view> val = std::nullopt;
    {
      std::lock_guard<std::mutex> lock(kv_store_mutex);
      std::string key = args[1];
      auto it = kv_store.find(key);
      if (it != kv_store.end()) {
        Entry &entry = it->second;
        if (entry.expiry_at_ms.has_value()) {
          if (now_ms() >= entry.expiry_at_ms.value()) {
            // This entry has expired - remove it - don't set val
            kv_store.erase(key);
          } else {
            // Otherwise this entry is still valid
            val = entry.value;
          }
        } else {
          val = entry.value;
        }
      }
    }

    send_all(client_fd, encode_bulk_string(val));
    return;
  }

  if (iequals(args[0], "echo")) {
    if (args.size() != 2) {
      send_all(client_fd,
               "-ERR wrong number of arguments for 'echo' command\r\n");
      return;
    }
    send_all(client_fd, encode_bulk_string(args[1]));
    return;
  }

  if (iequals(args[0], "rpush")) {
    if (args.size() < 3) {
      send_all(client_fd,
               "-ERR wrong number of arguments for 'rpush' command\r\n");
      return;
    }

    int64_t list_size = 0;
    {
      std::lock_guard<std::mutex> lock(kv_store_mutex);
      auto &lst = list_store[args[1]];
      for (size_t i = 2; i < args.size(); ++i) {
        lst.push_back(args[i]);
      }
      list_size = static_cast<int64_t>(lst.size());
      notify_blpop_waiters(args[1]);
    }

    send_all(client_fd, encode_integer(list_size));
    return;
  }

  if (iequals(args[0], "lpush")) {
    if (args.size() < 3) {
      send_all(client_fd,
               "-ERR wrong number of arguments for 'lpush' command\r\n");
      return;
    }

    int64_t list_size = 0;
    {
      std::lock_guard<std::mutex> lock(kv_store_mutex);
      auto &lst = list_store[args[1]];
      for (size_t i = 2; i < args.size(); ++i) {
        lst.insert(lst.begin(), args[i]);
      }
      list_size = static_cast<int64_t>(lst.size());
      notify_blpop_waiters(args[1]);
    }

    send_all(client_fd, encode_integer(list_size));
    return;
  }

  if (iequals(args[0], "llen")) {
    if (args.size() != 2) {
      send_all(client_fd,
               "-ERR wrong number of arguments for 'llen' command\r\n");
      return;
    }

    std::string key = args[1];

    int64_t list_size = 0;
    if (list_store.find(key) != list_store.end()) {
      list_size = list_store[key].size();
    }

    send_all(client_fd, encode_integer(list_size));
    return;
  }

  if (iequals(args[0], "lrange")) {
    if (args.size() != 4) {
      send_all(client_fd,
               "-ERR wrong number of arguments for 'lrange' command\r\n");
      return;
    }

    int64_t raw_start = 0;
    int64_t raw_end = 0;
    if (!parse_signed_int64(args[2], raw_start) ||
        !parse_signed_int64(args[3], raw_end)) {
      send_all(client_fd, "-ERR value is not an integer or out of range\r\n");
      return;
    }

    std::vector<std::string_view> sublist;
    {
      std::lock_guard<std::mutex> lock(kv_store_mutex);
      auto list_it = list_store.find(args[1]);
      if (list_it == list_store.end()) {
        send_all(client_fd, encode_array({}));
        return;
      }

      const std::vector<std::string> &list = list_it->second;
      size_t start_idx = 0;
      size_t end_idx = 0;
      if (!lrange_normalized_range(raw_start, raw_end, list.size(), start_idx,
                                   end_idx)) {
        send_all(client_fd, encode_array({}));
        return;
      }

      sublist.reserve(end_idx - start_idx + 1);
      for (size_t i = start_idx; i <= end_idx; ++i) {
        sublist.push_back(list[i]);
      }
    }

    send_all(client_fd, encode_array(sublist));
    return;
  }

  if (iequals(args[0], "lpop")) {
    if (args.size() != 2 && args.size() != 3) {
      send_all(client_fd,
               "-ERR wrong number of arguments for 'lpop' command\r\n");
      return;
    }

    if (args.size() == 2) {
      std::optional<std::string> popped;
      {
        std::lock_guard<std::mutex> lock(kv_store_mutex);
        auto it = list_store.find(args[1]);
        if (it != list_store.end() && !it->second.empty()) {
          popped = std::move(it->second.front());
          it->second.erase(it->second.begin());
          if (it->second.empty()) {
            list_store.erase(it);
          }
        }
      }

      if (!popped.has_value()) {
        send_all(client_fd, encode_bulk_string(std::nullopt));
      } else {
        send_all(client_fd,
                 encode_bulk_string(std::optional<std::string_view>(*popped)));
      }
      return;
    }

    int64_t count = 0;
    if (!parse_signed_int64(args[2], count)) {
      send_all(client_fd, "-ERR value is not an integer or out of range\r\n");
      return;
    }
    if (count < 0) {
      send_all(client_fd, "-ERR value is out of range\r\n");
      return;
    }

    // Own popped strings here — do not store string_views into lst while
    // erasing; erase destroys the std::string objects the views would point at.
    std::vector<std::string> removed;
    {
      std::lock_guard<std::mutex> lock(kv_store_mutex);
      auto it = list_store.find(args[1]);
      if (it != list_store.end() && !it->second.empty()) {
        auto &lst = it->second;
        const size_t take = std::min(static_cast<size_t>(count), lst.size());
        removed.reserve(take);
        for (size_t i = 0; i < take; ++i) {
          removed.push_back(std::move(lst.front()));
          lst.erase(lst.begin());
        }
        if (lst.empty()) {
          list_store.erase(it);
        }
      }
    }

    std::vector<std::string_view> views;
    views.reserve(removed.size());
    for (const auto &s : removed) {
      views.push_back(s);
    }
    send_all(client_fd, encode_array(views));
    return;
  }

  if (iequals(args[0], "blpop")) {
    // Syntax: BLPOP key [key ...] timeout
    if (args.size() < 3) {
      send_all(client_fd,
               "-ERR wrong number of arguments for 'blpop' command\r\n");
      return;
    }

    // Last argument is the timeout in whole seconds (0 = block indefinitely).
    int64_t timeout_secs = 0;
    {
      long long parsed = 0;
      if (!parse_int64(args.back(), parsed) || parsed < 0) {
        send_all(client_fd,
                 "-ERR timeout is not an integer or out of range\r\n");
        return;
      }
      timeout_secs = static_cast<int64_t>(parsed);
    }

    const std::vector<std::string> keys(args.begin() + 1, args.end() - 1);

    std::unique_lock<std::mutex> lock(kv_store_mutex);

    // If any key already has elements, serve immediately (first key wins).
    for (const auto &key : keys) {
      auto it = list_store.find(key);
      if (it != list_store.end() && !it->second.empty()) {
        std::string val = std::move(it->second.front());
        it->second.erase(it->second.begin());
        if (it->second.empty())
          list_store.erase(it);
        lock.unlock();

        std::string resp = "*2\r\n";
        resp += encode_bulk_string(std::string_view(key));
        resp += encode_bulk_string(std::string_view(val));
        send_all(client_fd, resp);
        return;
      }
    }

    // No elements yet — register as a waiter on every requested key.
    auto waiter = std::make_shared<BlpopWaiter>();
    for (const auto &key : keys) {
      blpop_waiters[key].push_back(waiter);
    }

    // Block until served or timed out.
    bool timed_out = false;
    if (timeout_secs == 0) {
      waiter->cv.wait(lock, [&] { return waiter->done; });
    } else {
      auto deadline =
          std::chrono::steady_clock::now() +
          std::chrono::seconds(timeout_secs);
      bool served =
          waiter->cv.wait_until(lock, deadline, [&] { return waiter->done; });
      if (!served)
        timed_out = true;
    }

    // Clean up: remove this waiter from all per-key queues (handles timeout
    // and the case where we were registered on multiple keys but only one
    // fired).
    for (const auto &key : keys) {
      auto wait_it = blpop_waiters.find(key);
      if (wait_it == blpop_waiters.end())
        continue;
      auto &q = wait_it->second;
      q.erase(std::remove(q.begin(), q.end(), waiter), q.end());
      if (q.empty())
        blpop_waiters.erase(key);
    }

    lock.unlock();

    if (timed_out) {
      send_all(client_fd, "*-1\r\n");
    } else {
      std::string resp = "*2\r\n";
      resp += encode_bulk_string(std::string_view(waiter->matched_key));
      resp += encode_bulk_string(std::string_view(waiter->matched_value));
      send_all(client_fd, resp);
    }
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
