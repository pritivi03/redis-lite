#include <algorithm>
#include <arpa/inet.h>
#include <cctype>
#include <charconv>
#include <chrono>
#include <climits>
#include <condition_variable>
#include <deque>
#include <iostream>
#include <memory>
#include <mutex>
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

// ── Types ──────────────────────────────────────────────────────────────────

struct KvEntry {
  std::string value;
  std::optional<int64_t> expiry_ms; // absolute epoch ms; nullopt = no expiry
};

// Registered by a BLPOP call that found no immediate element. All fields are
// accessed under Store::mu. The shared_ptr lets the push path reach a waiter
// without the pointer going stale after timeout cleanup.
struct BlpopWaiter {
  std::string matched_key;
  std::string matched_value;
  bool done = false;
  std::condition_variable cv;
};

// ── Store ──────────────────────────────────────────────────────────────────
//
// Single mutex covering all mutable state. The thread-per-connection model
// and typical command latency make finer-grained locking unnecessary here.

struct Store {
  std::unordered_map<std::string, KvEntry> kv;
  std::unordered_map<std::string, std::deque<std::string>> lists;
  std::unordered_map<std::string, std::deque<std::shared_ptr<BlpopWaiter>>>
      blpop_waiters;
  std::mutex mu;

  // Requires mutex held. Drains as many blocked BLPOP clients as there are
  // elements on key, serving them in FIFO arrival order.
  void drain_blpop_waiters(const std::string &key) {
    auto wq_it = blpop_waiters.find(key);
    if (wq_it == blpop_waiters.end())
      return;

    auto lst_it = lists.find(key);
    if (lst_it == lists.end() || lst_it->second.empty())
      return;

    auto &wq = wq_it->second;
    auto &lst = lst_it->second;

    while (!wq.empty() && !lst.empty()) {
      auto w = wq.front();
      wq.pop_front();
      if (w->done)
        continue; // timed out or already served by another key

      w->matched_key = key;
      w->matched_value = std::move(lst.front());
      lst.pop_front();
      w->done = true;
      w->cv.notify_one();
    }

    if (wq.empty())
      blpop_waiters.erase(key);
    if (lst.empty())
      lists.erase(key);
  }
};

Store g_store;

// ── Utilities ──────────────────────────────────────────────────────────────

int64_t now_ms() {
  using namespace std::chrono;
  return duration_cast<milliseconds>(system_clock::now().time_since_epoch())
      .count();
}

bool safe_add_int64(int64_t a, int64_t b, int64_t &out) {
  if ((b > 0 && a > LLONG_MAX - b) || (b < 0 && a < LLONG_MIN - b))
    return false;
  out = a + b;
  return true;
}

// ── Parsers ────────────────────────────────────────────────────────────────

bool parse_ll(std::string_view s, long long &out) {
  auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), out);
  return ec == std::errc{} && ptr == s.data() + s.size();
}

bool parse_positive_int64(std::string_view s, int64_t &out) {
  long long v = 0;
  if (!parse_ll(s, v) || v <= 0)
    return false;
  out = v;
  return true;
}

bool parse_signed_int64(std::string_view s, int64_t &out) {
  long long v = 0;
  if (!parse_ll(s, v))
    return false;
  out = v;
  return true;
}

bool parse_nonneg_double(std::string_view s, double &out) {
  if (s.empty())
    return false;
  try {
    size_t idx = 0;
    double v = std::stod(std::string(s), &idx);
    if (idx != s.size() || v < 0.0)
      return false;
    out = v;
    return true;
  } catch (...) {
    return false;
  }
}

// ── RESP parsing ───────────────────────────────────────────────────────────

bool read_crlf_line(std::string_view buf, size_t &pos, std::string_view &line) {
  const size_t start = pos;
  while (pos < buf.size() && buf[pos] != '\r')
    ++pos;
  if (pos + 1 >= buf.size() || buf[pos + 1] != '\n')
    return false;
  line = buf.substr(start, pos - start);
  pos += 2;
  return true;
}

bool parse_bulk_string(std::string_view buf, size_t &pos, std::string &out) {
  if (pos >= buf.size() || buf[pos] != '$')
    return false;
  ++pos;

  std::string_view len_sv;
  if (!read_crlf_line(buf, pos, len_sv))
    return false;

  long long len = 0;
  if (!parse_ll(len_sv, len) || len < -1)
    return false;
  if (len == -1) {
    out.clear();
    return true;
  }

  const size_t ulen = static_cast<size_t>(len);
  if (ulen > buf.size() - pos)
    return false;

  out.assign(buf.data() + pos, ulen);
  pos += ulen;

  if (pos + 1 >= buf.size() || buf[pos] != '\r' || buf[pos + 1] != '\n')
    return false;
  pos += 2;
  return true;
}

// Parses one complete RESP array-of-bulk-strings command from buf at pos.
// Returns false (without advancing pos) if the buffer holds only a partial
// command.
bool parse_command(std::string_view buf, size_t &pos,
                   std::vector<std::string> &args) {
  if (pos >= buf.size() || buf[pos] != '*')
    return false;
  size_t cur = pos + 1;

  std::string_view count_sv;
  if (!read_crlf_line(buf, cur, count_sv))
    return false;

  long long count = 0;
  if (!parse_ll(count_sv, count) || count < 0)
    return false;

  args.clear();
  args.reserve(static_cast<size_t>(count));
  for (long long i = 0; i < count; ++i) {
    std::string elem;
    if (!parse_bulk_string(buf, cur, elem))
      return false;
    args.push_back(std::move(elem));
  }
  pos = cur;
  return true;
}

// ── RESP encoding ──────────────────────────────────────────────────────────

[[nodiscard]] std::string encode_bulk(std::string_view sv) {
  auto len_s = std::to_string(sv.size());
  std::string out;
  out.reserve(1 + len_s.size() + 2 + sv.size() + 2);
  out += '$';
  out += len_s;
  out += "\r\n";
  out += sv;
  out += "\r\n";
  return out;
}

[[nodiscard]] std::string encode_null_bulk() { return "$-1\r\n"; }
[[nodiscard]] std::string encode_null_array() { return "*-1\r\n"; }
[[nodiscard]] std::string encode_integer(int64_t v) {
  return ':' + std::to_string(v) + "\r\n";
}

[[nodiscard]] std::string encode_array(const std::vector<std::string> &elems) {
  if (elems.empty())
    return "*0\r\n";
  std::string out = '*' + std::to_string(elems.size()) + "\r\n";
  for (const auto &e : elems)
    out += encode_bulk(e);
  return out;
}

// ── I/O helpers ────────────────────────────────────────────────────────────

void send_all(int fd, std::string_view data) {
  const char *p = data.data();
  size_t left = data.size();
  while (left > 0) {
    ssize_t n = send(fd, p, left, 0);
    if (n <= 0)
      return;
    p += static_cast<size_t>(n);
    left -= static_cast<size_t>(n);
  }
}

bool iequals(std::string_view a, std::string_view b) {
  if (a.size() != b.size())
    return false;
  for (size_t i = 0; i < a.size(); ++i)
    if (std::tolower(static_cast<unsigned char>(a[i])) !=
        std::tolower(static_cast<unsigned char>(b[i])))
      return false;
  return true;
}

// ── Index helpers ──────────────────────────────────────────────────────────

// Resolves Redis-style negative indices and clamps to [0, len). Returns false
// if the resulting range is empty.
bool normalize_range(int64_t raw_start, int64_t raw_end, size_t len,
                     size_t &out_start, size_t &out_end) {
  if (len == 0)
    return false;
  const int64_t n = static_cast<int64_t>(len);
  int64_t s = raw_start < 0 ? raw_start + n : raw_start;
  int64_t e = raw_end < 0 ? raw_end + n : raw_end;
  if (s < 0)
    s = 0;
  if (e >= n)
    e = n - 1;
  if (s > e)
    return false;
  out_start = static_cast<size_t>(s);
  out_end = static_cast<size_t>(e);
  return true;
}

// ── Command handlers ───────────────────────────────────────────────────────

void cmd_ping(int fd, const std::vector<std::string> &args) {
  if (args.size() == 1)
    send_all(fd, "+PONG\r\n");
  else if (args.size() == 2)
    send_all(fd, encode_bulk(args[1]));
  else
    send_all(fd, "-ERR wrong number of arguments for 'ping' command\r\n");
}

void cmd_set(int fd, const std::vector<std::string> &args) {
  if (args.size() != 3 && args.size() != 5) {
    send_all(fd, "-ERR wrong number of arguments for 'set' command\r\n");
    return;
  }

  std::optional<int64_t> expiry_ms;
  if (args.size() == 5) {
    int64_t ttl = 0;
    if (!parse_positive_int64(args[4], ttl)) {
      send_all(fd, "-ERR invalid expire time in 'set' command\r\n");
      return;
    }
    int64_t ttl_ms = 0;
    if (iequals(args[3], "EX")) {
      if (ttl > LLONG_MAX / 1000) {
        send_all(fd, "-ERR invalid expire time in 'set' command\r\n");
        return;
      }
      ttl_ms = ttl * 1000;
    } else if (iequals(args[3], "PX")) {
      ttl_ms = ttl;
    } else {
      send_all(fd, "-ERR syntax error\r\n");
      return;
    }
    int64_t abs_expiry = 0;
    if (!safe_add_int64(now_ms(), ttl_ms, abs_expiry)) {
      send_all(fd, "-ERR invalid expire time in 'set' command\r\n");
      return;
    }
    expiry_ms = abs_expiry;
  }

  {
    std::lock_guard lock(g_store.mu);
    g_store.kv[args[1]] = KvEntry{args[2], expiry_ms};
  }
  send_all(fd, "+OK\r\n");
}

void cmd_get(int fd, const std::vector<std::string> &args) {
  if (args.size() != 2) {
    send_all(fd, "-ERR wrong number of arguments for 'get' command\r\n");
    return;
  }

  std::optional<std::string> val;
  {
    std::lock_guard lock(g_store.mu);
    auto it = g_store.kv.find(args[1]);
    if (it != g_store.kv.end()) {
      auto &entry = it->second;
      if (entry.expiry_ms && now_ms() >= *entry.expiry_ms) {
        g_store.kv.erase(it);
      } else {
        val = entry.value; // copy before releasing lock
      }
    }
  }
  send_all(fd, val ? encode_bulk(*val) : encode_null_bulk());
}

void cmd_echo(int fd, const std::vector<std::string> &args) {
  if (args.size() != 2) {
    send_all(fd, "-ERR wrong number of arguments for 'echo' command\r\n");
    return;
  }
  send_all(fd, encode_bulk(args[1]));
}

void cmd_rpush(int fd, const std::vector<std::string> &args) {
  if (args.size() < 3) {
    send_all(fd, "-ERR wrong number of arguments for 'rpush' command\r\n");
    return;
  }
  int64_t list_size = 0;
  {
    std::lock_guard lock(g_store.mu);
    auto &lst = g_store.lists[args[1]];
    for (size_t i = 2; i < args.size(); ++i)
      lst.push_back(args[i]);
    list_size = static_cast<int64_t>(lst.size());
    g_store.drain_blpop_waiters(args[1]);
  }
  send_all(fd, encode_integer(list_size));
}

void cmd_lpush(int fd, const std::vector<std::string> &args) {
  if (args.size() < 3) {
    send_all(fd, "-ERR wrong number of arguments for 'lpush' command\r\n");
    return;
  }
  int64_t list_size = 0;
  {
    std::lock_guard lock(g_store.mu);
    auto &lst = g_store.lists[args[1]];
    for (size_t i = 2; i < args.size(); ++i)
      lst.push_front(args[i]);
    list_size = static_cast<int64_t>(lst.size());
    g_store.drain_blpop_waiters(args[1]);
  }
  send_all(fd, encode_integer(list_size));
}

void cmd_llen(int fd, const std::vector<std::string> &args) {
  if (args.size() != 2) {
    send_all(fd, "-ERR wrong number of arguments for 'llen' command\r\n");
    return;
  }
  int64_t size = 0;
  {
    std::lock_guard lock(g_store.mu);
    auto it = g_store.lists.find(args[1]);
    if (it != g_store.lists.end())
      size = static_cast<int64_t>(it->second.size());
  }
  send_all(fd, encode_integer(size));
}

void cmd_lrange(int fd, const std::vector<std::string> &args) {
  if (args.size() != 4) {
    send_all(fd, "-ERR wrong number of arguments for 'lrange' command\r\n");
    return;
  }
  int64_t raw_start = 0, raw_end = 0;
  if (!parse_signed_int64(args[2], raw_start) ||
      !parse_signed_int64(args[3], raw_end)) {
    send_all(fd, "-ERR value is not an integer or out of range\r\n");
    return;
  }

  std::vector<std::string> result;
  {
    std::lock_guard lock(g_store.mu);
    auto it = g_store.lists.find(args[1]);
    if (it != g_store.lists.end()) {
      const auto &lst = it->second;
      size_t s = 0, e = 0;
      if (normalize_range(raw_start, raw_end, lst.size(), s, e))
        result.assign(lst.begin() + static_cast<ptrdiff_t>(s),
                      lst.begin() + static_cast<ptrdiff_t>(e) + 1);
    }
  }
  send_all(fd, encode_array(result));
}

void cmd_lpop(int fd, const std::vector<std::string> &args) {
  if (args.size() != 2 && args.size() != 3) {
    send_all(fd, "-ERR wrong number of arguments for 'lpop' command\r\n");
    return;
  }

  if (args.size() == 2) {
    std::optional<std::string> val;
    {
      std::lock_guard lock(g_store.mu);
      auto it = g_store.lists.find(args[1]);
      if (it != g_store.lists.end() && !it->second.empty()) {
        val = std::move(it->second.front());
        it->second.pop_front();
        if (it->second.empty())
          g_store.lists.erase(it);
      }
    }
    send_all(fd, val ? encode_bulk(*val) : encode_null_bulk());
    return;
  }

  int64_t count = 0;
  if (!parse_signed_int64(args[2], count) || count < 0) {
    send_all(fd, "-ERR value is not an integer or out of range\r\n");
    return;
  }

  std::vector<std::string> removed;
  {
    std::lock_guard lock(g_store.mu);
    auto it = g_store.lists.find(args[1]);
    if (it != g_store.lists.end() && !it->second.empty()) {
      auto &lst = it->second;
      const size_t take = std::min(static_cast<size_t>(count), lst.size());
      removed.reserve(take);
      for (size_t i = 0; i < take; ++i) {
        removed.push_back(std::move(lst.front()));
        lst.pop_front();
      }
      if (lst.empty())
        g_store.lists.erase(it);
    }
  }
  send_all(fd, encode_array(removed));
}

void cmd_blpop(int fd, const std::vector<std::string> &args) {
  // Syntax: BLPOP key [key ...] timeout
  if (args.size() < 3) {
    send_all(fd, "-ERR wrong number of arguments for 'blpop' command\r\n");
    return;
  }

  double timeout_secs = 0.0;
  if (!parse_nonneg_double(args.back(), timeout_secs)) {
    send_all(fd, "-ERR timeout is not a float or out of range\r\n");
    return;
  }

  const std::vector<std::string> keys(args.begin() + 1, args.end() - 1);

  std::unique_lock lock(g_store.mu);

  // Serve immediately if any key already has elements (first key wins).
  for (const auto &key : keys) {
    auto it = g_store.lists.find(key);
    if (it != g_store.lists.end() && !it->second.empty()) {
      std::string val = std::move(it->second.front());
      it->second.pop_front();
      if (it->second.empty())
        g_store.lists.erase(it);
      lock.unlock();

      send_all(fd, "*2\r\n" + encode_bulk(key) + encode_bulk(val));
      return;
    }
  }

  // Register as a waiter on every requested key.
  auto waiter = std::make_shared<BlpopWaiter>();
  for (const auto &key : keys)
    g_store.blpop_waiters[key].push_back(waiter);

  bool timed_out = false;
  if (timeout_secs == 0.0) {
    waiter->cv.wait(lock, [&] { return waiter->done; });
  } else {
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::duration<double>(timeout_secs);
    if (!waiter->cv.wait_until(lock, deadline, [&] { return waiter->done; }))
      timed_out = true;
  }

  // Remove this waiter from any queues it is still in (timeout or multi-key
  // registration where only one key fired).
  for (const auto &key : keys) {
    auto wq_it = g_store.blpop_waiters.find(key);
    if (wq_it == g_store.blpop_waiters.end())
      continue;
    auto &q = wq_it->second;
    q.erase(std::remove(q.begin(), q.end(), waiter), q.end());
    if (q.empty())
      g_store.blpop_waiters.erase(key);
  }

  lock.unlock();

  if (timed_out) {
    send_all(fd, encode_null_array());
  } else {
    send_all(fd, "*2\r\n" + encode_bulk(waiter->matched_key) +
                     encode_bulk(waiter->matched_value));
  }
}

// ── Dispatch ───────────────────────────────────────────────────────────────

void dispatch_command(int fd, const std::vector<std::string> &args) {
  if (args.empty()) {
    send_all(fd, "-ERR empty command\r\n");
    return;
  }

  const auto &cmd = args[0];
  if (iequals(cmd, "ping"))
    cmd_ping(fd, args);
  else if (iequals(cmd, "set"))
    cmd_set(fd, args);
  else if (iequals(cmd, "get"))
    cmd_get(fd, args);
  else if (iequals(cmd, "echo"))
    cmd_echo(fd, args);
  else if (iequals(cmd, "rpush"))
    cmd_rpush(fd, args);
  else if (iequals(cmd, "lpush"))
    cmd_lpush(fd, args);
  else if (iequals(cmd, "llen"))
    cmd_llen(fd, args);
  else if (iequals(cmd, "lrange"))
    cmd_lrange(fd, args);
  else if (iequals(cmd, "lpop"))
    cmd_lpop(fd, args);
  else if (iequals(cmd, "blpop"))
    cmd_blpop(fd, args);
  else
    send_all(fd, "-ERR unknown command '" + std::string(cmd) + "'\r\n");
}

} // namespace

// ── Per-connection thread ──────────────────────────────────────────────────

void handle_client(int fd) {
  constexpr size_t kBufSize = 4096;
  char raw[kBufSize];
  std::string buf;
  buf.reserve(kBufSize);

  while (true) {
    ssize_t n = read(fd, raw, kBufSize);
    if (n <= 0)
      break;
    buf.append(raw, static_cast<size_t>(n));

    size_t consumed = 0;
    while (consumed < buf.size()) {
      if (buf[consumed] != '*') {
        send_all(fd, "-ERR protocol error\r\n");
        buf.clear();
        break;
      }
      size_t pos = consumed;
      std::vector<std::string> args;
      if (!parse_command(buf, pos, args))
        break;
      consumed = pos;
      dispatch_command(fd, args);
    }
    if (consumed > 0)
      buf.erase(0, consumed);
  }
  close(fd);
}

// ── Entry point ────────────────────────────────────────────────────────────

int main() {
  std::cout << std::unitbuf;
  std::cerr << std::unitbuf;

  const int server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0) {
    std::cerr << "Failed to create server socket\n";
    return 1;
  }

  const int reuse = 1;
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) <
      0) {
    std::cerr << "setsockopt failed\n";
    return 1;
  }

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = INADDR_ANY;
  addr.sin_port = htons(6379);

  if (bind(server_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) != 0) {
    std::cerr << "Failed to bind to port 6379\n";
    return 1;
  }
  if (listen(server_fd, /*backlog=*/128) != 0) {
    std::cerr << "listen failed\n";
    return 1;
  }

  std::cout << "Waiting for clients to connect...\n";
  while (true) {
    sockaddr_in client_addr{};
    socklen_t client_len = sizeof(client_addr);
    const int client_fd = accept(
        server_fd, reinterpret_cast<sockaddr *>(&client_addr), &client_len);
    if (client_fd < 0) {
      std::cerr << "accept failed\n";
      continue; // don't abort the server on a transient accept error
    }
    std::thread(handle_client, client_fd).detach();
  }
}
