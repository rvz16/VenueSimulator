#pragma once

// Header-only transport and conservative logical clock for the CMF venue
// protocol. Requires standalone Asio (ASIO_STANDALONE).

#include <asio.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <cmath>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <deque>
#include <exception>
#include <functional>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

namespace cmf_driver {

using SimTime = std::uint64_t;

struct Level {
    double price = 0.0;
    std::uint64_t qty = 0;
};

enum class Side { Buy, Sell };

enum class RejectReason : std::uint32_t {
    NoSuchOrder = 1,
    AlreadyTerminal = 2,
    PriceOffTickGrid = 3,
    InvalidParameters = 4,
    UnknownInstrument = 5,
    InternalError = 9,
};

struct Options {
    std::string host = "127.0.0.1";
    std::uint16_t feed_port = 0;
    std::uint16_t order_port = 0;
    std::vector<std::string> datasets;
};

struct NewOrder {
    std::uint64_t client_id = 0;
    SimTime ts_seen = 0;
    SimTime effective_ts = 0;
    std::string class_code;
    std::string sec_code;
    Side side = Side::Buy;
    double price = 0.0;
    std::uint64_t qty = 0;
    std::uint8_t tif = 0;
};

struct Cancel {
    std::uint64_t client_id = 0;
    SimTime ts_seen = 0;
    SimTime effective_ts = 0;
    std::string class_code;
    std::string sec_code;
    std::uint64_t order_id = 0;
};

class ProtocolError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

namespace detail {

inline constexpr SimTime kReservedTime = std::numeric_limits<SimTime>::max();

inline std::vector<std::string_view> Split(std::string_view line) {
    std::vector<std::string_view> fields;
    std::size_t begin = 0;
    for (;;) {
        const std::size_t end = line.find('|', begin);
        fields.emplace_back(line.substr(begin, end == std::string_view::npos ? end : end - begin));
        if (end == std::string_view::npos) break;
        begin = end + 1;
    }
    return fields;
}

inline std::uint64_t ParseUnsigned(std::string_view s, const char* field) {
    if (s.empty()) throw ProtocolError(std::string("empty ") + field);
    std::uint64_t value = 0;
    const auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), value);
    if (ec != std::errc{} || ptr != s.data() + s.size())
        throw ProtocolError(std::string("invalid ") + field);
    return value;
}

inline double ParsePrice(std::string_view s) {
    if (s.empty()) throw ProtocolError("empty price");
    std::string copy(s);
    char* end = nullptr;
    const double value = std::strtod(copy.c_str(), &end);
    if (end != copy.c_str() + copy.size() || !std::isfinite(value))
        throw ProtocolError("invalid price");
    return value;
}

inline std::string PriceText(double price) {
    if (!std::isfinite(price)) throw ProtocolError("cannot encode a non-finite price");
    std::array<char, 128> buffer{};
    const auto [end, ec] = std::to_chars(buffer.data(), buffer.data() + buffer.size(),
                                         price, std::chars_format::fixed, 9);
    if (ec != std::errc{}) throw ProtocolError("price formatting failed");
    std::string result(buffer.data(), end);
    while (result.find('.') != std::string::npos && result.back() == '0') result.pop_back();
    if (!result.empty() && result.back() == '.') result.pop_back();
    return result;
}

inline SimTime CheckedTimeAdd(SimTime a, SimTime b) {
    if (a == 0 || a == kReservedTime || b > kReservedTime - 1 - a)
        throw ProtocolError("logical timestamp overflow or reserved timestamp");
    return a + b;
}

inline std::uint64_t PortValue(std::string_view s, const char* name) {
    const auto value = ParseUnsigned(s, name);
    if (value == 0 || value > std::numeric_limits<std::uint16_t>::max())
        throw ProtocolError(std::string("invalid ") + name);
    return value;
}

inline std::string ReadLine(asio::ip::tcp::socket& socket, asio::streambuf& buffer) {
    asio::read_until(socket, buffer, '\n');
    std::istream input(&buffer);
    std::string line;
    std::getline(input, line);
    if (!line.empty() && line.back() == '\r') line.pop_back();
    return line;
}

} // namespace detail

inline Options ParseOptions(int argc, char** argv) {
    Options options;
    bool host_seen = false;
    bool feed_seen = false;
    bool order_seen = false;
    for (int i = 1; i < argc; ++i) {
        const std::string_view arg(argv[i]);
        constexpr std::string_view host_prefix = "--host=";
        constexpr std::string_view feed_prefix = "--feed-port=";
        constexpr std::string_view order_prefix = "--order-port=";
        if (arg.starts_with(host_prefix)) {
            options.host = std::string(arg.substr(host_prefix.size()));
            host_seen = true;
        } else if (arg.starts_with(feed_prefix)) {
            options.feed_port = static_cast<std::uint16_t>(detail::PortValue(arg.substr(feed_prefix.size()), "feed port"));
            feed_seen = true;
        } else if (arg.starts_with(order_prefix)) {
            options.order_port = static_cast<std::uint16_t>(detail::PortValue(arg.substr(order_prefix.size()), "order port"));
            order_seen = true;
        } else if (arg.starts_with("--")) {
            // Unknown engine flags are forwarded for forward compatibility.
            continue;
        } else {
            options.datasets.emplace_back(argv[i]);
        }
    }
    if (host_seen && options.host.empty()) throw ProtocolError("host cannot be empty");
    if (feed_seen != order_seen) throw ProtocolError("both channel ports must be provided together");
    // Defaults make the driver convenient to launch manually. Production
    // launchers should always supply all three connection arguments.
    if (feed_seen && (!host_seen || options.host.empty())) throw ProtocolError("missing --host");
    return options;
}

class Session {
public:
    std::function<void(const NewOrder&)> on_new_order;
    std::function<void(const Cancel&)> on_cancel;

    explicit Session(const Options& options)
        : io_(), order_acceptor_(io_), feed_acceptor_(io_),
          order_socket_(io_), feed_socket_(io_) {
        if (options.feed_port == 0 || options.order_port == 0)
            throw ProtocolError("feed and order ports must be non-zero");
        asio::error_code ec;
        const auto address = asio::ip::make_address(options.host, ec);
        if (ec) throw ProtocolError("invalid listen address: " + options.host);
        OpenAcceptor(order_acceptor_, address, options.order_port);
        OpenAcceptor(feed_acceptor_, address, options.feed_port);
    }

    Session(const Session&) = delete;
    Session& operator=(const Session&) = delete;

    ~Session() { Finish(); }

    SimTime Start() {
        if (started_) throw ProtocolError("Session::Start called more than once");
        // The engine connects order first, but accept each independently so
        // small launch-time scheduling differences cannot deadlock startup.
        order_acceptor_.accept(order_socket_);
        feed_acceptor_.accept(feed_socket_);
        const auto hello = detail::Split(detail::ReadLine(order_socket_, order_buffer_));
        if (hello.size() != 2 || hello[0] != "HELLO")
            throw ProtocolError("expected HELLO on order channel");
        lookahead_ = detail::ParseUnsigned(hello[1], "lookahead");
        if (lookahead_ <= 1 || lookahead_ == detail::kReservedTime)
            throw ProtocolError("lookahead must be greater than one nanosecond");
        started_ = true;
        reader_ = std::thread([this] { ReadOrders(); });
        return lookahead_;
    }

    void Ready(SimTime first_ts) {
        RequireStarted();
        ValidateTime(first_ts);
        if (ready_) throw ProtocolError("Session::Ready called more than once");
        order_horizon_ = first_ts;
        SendOrder("K|" + std::to_string(first_ts));
        SendOrder("H|" + std::to_string(first_ts));
        // The first snapshot is at first_ts; the feed cannot promise it until
        // after that snapshot is sent. ts-1 is the greatest safe initial bound.
        if (first_ts > 1) {
            SendFeed("H|" + std::to_string(first_ts - 1));
            feed_horizon_ = first_ts - 1;
        }
        venue_time_ = first_ts;
        {
            std::lock_guard lock(inbound_mutex_);
            if (engine_time_ < first_ts) engine_time_ = first_ts;
        }
        ready_ = true;
    }

    void AdvanceTo(SimTime ts) {
        RequireReady();
        ValidateTime(ts);
        if (ts < venue_time_) throw ProtocolError("venue timestamp moved backwards");

        // Announce everything before the next event. This closes the feed
        // channel at a point that is already published and lets the engine
        // advance through sparse gaps without inventing market data.
        if (ts > 1) AnnounceFeedHorizon(ts - 1);

        for (;;) {
            CheckReaderError();
            SimTime engine_now = 0;
            {
                std::lock_guard lock(inbound_mutex_);
                engine_now = engine_time_;
            }
            const SimTime safe_limit = detail::CheckedTimeAdd(engine_now, lookahead_) - 1;
            if (ts <= safe_limit) break;
            AnnounceOrderHorizon(safe_limit);
            std::unique_lock lock(inbound_mutex_);
            inbound_cv_.wait(lock, [this, safe_limit] {
                return engine_finished_ || reader_error_ || engine_time_ >= safe_limit;
            });
            if (engine_finished_) break; // unique_lock still owns inbound_mutex_
        }

        std::deque<Inbound> due;
        {
            std::lock_guard lock(inbound_mutex_);
            auto it = inbound_.begin();
            while (it != inbound_.end()) {
                const SimTime effective = std::visit([](const auto& value) { return value.effective_ts; }, *it);
                if (effective <= ts) {
                    due.push_back(std::move(*it));
                    it = inbound_.erase(it);
                } else {
                    ++it;
                }
            }
        }
        for (const auto& message : due) {
            if (const auto* order = std::get_if<NewOrder>(&message)) {
                if (on_new_order) on_new_order(*order);
                else Reject(order->client_id, order->effective_ts, RejectReason::InternalError);
            } else {
                const auto& cancel = std::get<Cancel>(message);
                if (on_cancel) on_cancel(cancel);
                else CancelRejected(cancel.client_id, cancel.effective_ts, cancel.order_id,
                                    RejectReason::NoSuchOrder);
            }
        }
        venue_time_ = ts;
        if (EngineFinished()) return;
    }

    bool EngineFinished() const noexcept {
        std::lock_guard lock(inbound_mutex_);
        return engine_finished_;
    }

    void PublishSnapshot(std::uint64_t seq, SimTime ts, std::string_view class_code,
                         std::string_view sec_code, const std::vector<Level>& bids,
                         const std::vector<Level>& asks) {
        RequireReady();
        ValidateTime(ts);
        if (ts != venue_time_) throw ProtocolError("snapshot timestamp must follow AdvanceTo");
        if (seq == 0) throw ProtocolError("snapshot sequence must be non-zero");
        if (class_code.size() > 7 || sec_code.size() > 31 ||
            class_code.find('|') != std::string_view::npos || sec_code.find('|') != std::string_view::npos)
            throw ProtocolError("instrument code is too long or contains a separator");
        if (bids.size() > 64 || asks.size() > 64) throw ProtocolError("snapshot exceeds 64 levels per side");
        ValidateLevels(bids, true);
        ValidateLevels(asks, false);
        if (last_seq_ == std::numeric_limits<std::uint64_t>::max() || seq != last_seq_ + 1)
            throw ProtocolError("snapshot sequence must be consecutive");
        if (ts <= feed_horizon_) throw ProtocolError("snapshot violates an announced feed horizon");
        if (last_feed_ts_ != 0 && ts < last_feed_ts_)
            throw ProtocolError("snapshot timestamp moved backwards");
        std::string line = "S|" + std::to_string(seq) + "|" + std::to_string(ts) + "|" +
                           std::string(class_code) + "|" + std::string(sec_code) + "|" +
                           std::to_string(bids.size()) + "|" + std::to_string(asks.size());
        for (const auto& level : bids) AppendLevel(line, level);
        for (const auto& level : asks) AppendLevel(line, level);
        SendFeed(line);
        last_seq_ = seq;
        last_feed_ts_ = ts;
    }

    void Accept(std::uint64_t client_id, SimTime ts, std::uint64_t order_id, std::uint64_t leaves) {
        ValidateReply(client_id, ts, order_id);
        if (!order_ids_.insert(order_id).second)
            throw ProtocolError("order id was already issued in this run");
        SendOrder("A|" + std::to_string(client_id) + "|" + std::to_string(ts) + "|" +
                  std::to_string(order_id) + "|" + std::to_string(leaves));
    }

    void Reject(std::uint64_t client_id, SimTime ts, RejectReason reason) {
        ValidateClientTime(client_id, ts);
        SendOrder("R|" + std::to_string(client_id) + "|" + std::to_string(ts) + "|" +
                  std::to_string(static_cast<std::uint32_t>(reason)));
    }

    void Fill(std::uint64_t client_id, SimTime ts, std::uint64_t order_id,
              std::uint64_t trade_id, double price, std::uint64_t qty, std::uint64_t leaves) {
        ValidateReply(client_id, ts, order_id);
        if (trade_id == 0 || qty == 0 || !std::isfinite(price) || price <= 0.0)
            throw ProtocolError("invalid fill fields");
        if (!trade_ids_.insert(trade_id).second)
            throw ProtocolError("trade id was already issued in this run");
        SendOrder("F|" + std::to_string(client_id) + "|" + std::to_string(ts) + "|" +
                  std::to_string(order_id) + "|" + std::to_string(trade_id) + "|" +
                  detail::PriceText(price) + "|" + std::to_string(qty) + "|" +
                  std::to_string(leaves));
    }

    void Cancelled(std::uint64_t client_id, SimTime ts, std::uint64_t order_id) {
        ValidateReply(client_id, ts, order_id);
        SendOrder("X|" + std::to_string(client_id) + "|" + std::to_string(ts) + "|" +
                  std::to_string(order_id));
    }

    void CancelRejected(std::uint64_t client_id, SimTime ts, std::uint64_t order_id,
                        RejectReason reason) {
        ValidateClientTime(client_id, ts);
        SendOrder("Y|" + std::to_string(client_id) + "|" + std::to_string(ts) + "|" +
                  std::to_string(order_id) + "|" +
                  std::to_string(static_cast<std::uint32_t>(reason)));
    }

    void Finish() noexcept {
        if (finished_.exchange(true)) return;
        if (started_) {
            try { SendOrder("E"); } catch (...) {}
            try { SendFeed("E"); } catch (...) {}
        }
        asio::error_code ignored;
        order_socket_.shutdown(asio::ip::tcp::socket::shutdown_both, ignored);
        feed_socket_.shutdown(asio::ip::tcp::socket::shutdown_both, ignored);
        if (reader_.joinable()) reader_.join();
        order_acceptor_.close(ignored);
        feed_acceptor_.close(ignored);
    }

private:
    using Inbound = std::variant<NewOrder, Cancel>;

    void OpenAcceptor(asio::ip::tcp::acceptor& acceptor, const asio::ip::address& address,
                      std::uint16_t port) {
        const asio::ip::tcp::endpoint endpoint(address, port);
        acceptor.open(endpoint.protocol());
        acceptor.set_option(asio::socket_base::reuse_address(true));
        acceptor.bind(endpoint);
        acceptor.listen();
    }

    void RequireStarted() const {
        if (!started_) throw ProtocolError("Session::Start must be called first");
    }
    void RequireReady() const {
        RequireStarted();
        if (!ready_) throw ProtocolError("Session::Ready must be called first");
    }
    static void ValidateTime(SimTime ts) {
        if (ts == 0 || ts == detail::kReservedTime) throw ProtocolError("reserved logical timestamp");
    }
    void ValidateClientTime(std::uint64_t client_id, SimTime ts) {
        if (client_id == 0) throw ProtocolError("client id zero is reserved");
        ValidateTime(ts);
        if (ts <= order_horizon_.load())
            throw ProtocolError("order reply violates an announced order-channel horizon");
        if (ts < last_order_reply_ts_) throw ProtocolError("outbound order timestamp moved backwards");
        last_order_reply_ts_ = ts;
    }
    void ValidateReply(std::uint64_t client_id, SimTime ts, std::uint64_t order_id) {
        ValidateClientTime(client_id, ts);
        if (order_id == 0) throw ProtocolError("order id zero is reserved");
    }
    static void ValidateLevels(const std::vector<Level>& levels, bool bids) {
        double previous = bids ? std::numeric_limits<double>::infinity()
                               : -std::numeric_limits<double>::infinity();
        for (const auto& level : levels) {
            if (!std::isfinite(level.price) || level.price <= 0.0 || level.qty == 0)
                throw ProtocolError("invalid snapshot level");
            if (bids ? level.price >= previous : level.price <= previous)
                throw ProtocolError("snapshot levels are not best-first");
            previous = level.price;
        }
    }
    static void AppendLevel(std::string& line, const Level& level) {
        line += "|" + detail::PriceText(level.price) + ":" + std::to_string(level.qty);
    }
    void SendOrder(const std::string& line) {
        std::lock_guard lock(order_send_mutex_);
        asio::write(order_socket_, asio::buffer(line + "\n"));
    }
    void SendFeed(const std::string& line) {
        std::lock_guard lock(feed_send_mutex_);
        asio::write(feed_socket_, asio::buffer(line + "\n"));
    }
    void AnnounceFeedHorizon(SimTime ts) {
        if (ts > feed_horizon_) {
            SendFeed("H|" + std::to_string(ts));
            feed_horizon_ = ts;
        }
    }
    void AnnounceOrderHorizon(SimTime ts) {
        if (ts > order_horizon_) {
            order_horizon_ = ts;
            SendOrder("H|" + std::to_string(ts));
        }
    }
    void CheckReaderError() const {
        std::lock_guard lock(inbound_mutex_);
        if (reader_error_) std::rethrow_exception(reader_error_);
    }

    void ReadOrders() noexcept {
        try {
            for (;;) {
                const std::string line = detail::ReadLine(order_socket_, order_buffer_);
                const auto fields = detail::Split(line);
                if (fields.empty()) throw ProtocolError("empty order-channel message");
                if (fields[0] == "TIME") {
                    if (fields.size() != 2) throw ProtocolError("malformed TIME");
                    const SimTime now = detail::ParseUnsigned(fields[1], "TIME timestamp");
                    ValidateTime(now);
                    {
                        std::lock_guard lock(inbound_mutex_);
                        if (now < engine_time_) throw ProtocolError("engine time moved backwards");
                        engine_time_ = now;
                    }
                    if (now < last_inbound_ts_)
                        throw ProtocolError("engine TIME moved behind a previously sent order");
                    last_inbound_ts_ = now;
                    inbound_cv_.notify_all();
                } else if (fields[0] == "BYE") {
                    if (fields.size() != 1) throw ProtocolError("malformed BYE");
                    {
                        std::lock_guard lock(inbound_mutex_);
                        engine_finished_ = true;
                    }
                    inbound_cv_.notify_all();
                    return;
                } else if (fields[0] == "N") {
                    if (fields.size() != 9) throw ProtocolError("malformed new order");
                    NewOrder order;
                    order.client_id = detail::ParseUnsigned(fields[1], "client id");
                    order.ts_seen = detail::ParseUnsigned(fields[2], "order timestamp");
                    order.class_code = fields[3];
                    order.sec_code = fields[4];
                    ValidateTime(order.ts_seen);
                    if (order.ts_seen < last_inbound_ts_)
                        throw ProtocolError("inbound order timestamp moved backwards");
                    if (order.class_code.size() > 7 || order.sec_code.size() > 31)
                        throw ProtocolError("inbound instrument code exceeds protocol limit");
                    if (fields[5] == "B") order.side = Side::Buy;
                    else if (fields[5] == "S") order.side = Side::Sell;
                    else throw ProtocolError("invalid order side");
                    order.price = detail::ParsePrice(fields[6]);
                    order.qty = detail::ParseUnsigned(fields[7], "order quantity");
                    const auto tif = detail::ParseUnsigned(fields[8], "time in force");
                    if (order.client_id == 0 || tif > 2) throw ProtocolError("invalid new order fields");
                    order.tif = static_cast<std::uint8_t>(tif);
                    order.effective_ts = detail::CheckedTimeAdd(order.ts_seen, lookahead_);
                    if (order.effective_ts <= order_horizon_.load())
                        throw ProtocolError("inbound order violates an announced order-channel horizon");
                    last_inbound_ts_ = order.ts_seen;
                    std::lock_guard lock(inbound_mutex_);
                    inbound_.emplace_back(std::move(order));
                    inbound_cv_.notify_all();
                } else if (fields[0] == "C") {
                    if (fields.size() != 6) throw ProtocolError("malformed cancel");
                    Cancel cancel;
                    cancel.client_id = detail::ParseUnsigned(fields[1], "cancel client id");
                    cancel.ts_seen = detail::ParseUnsigned(fields[2], "cancel timestamp");
                    cancel.class_code = fields[3];
                    cancel.sec_code = fields[4];
                    ValidateTime(cancel.ts_seen);
                    if (cancel.ts_seen < last_inbound_ts_)
                        throw ProtocolError("inbound cancel timestamp moved backwards");
                    if (cancel.class_code.size() > 7 || cancel.sec_code.size() > 31)
                        throw ProtocolError("inbound instrument code exceeds protocol limit");
                    cancel.order_id = detail::ParseUnsigned(fields[5], "cancel order id");
                    if (cancel.client_id == 0) throw ProtocolError("cancel client id zero is reserved");
                    cancel.effective_ts = detail::CheckedTimeAdd(cancel.ts_seen, lookahead_);
                    if (cancel.effective_ts <= order_horizon_.load())
                        throw ProtocolError("inbound cancel violates an announced order-channel horizon");
                    last_inbound_ts_ = cancel.ts_seen;
                    std::lock_guard lock(inbound_mutex_);
                    inbound_.emplace_back(std::move(cancel));
                    inbound_cv_.notify_all();
                } else {
                    throw ProtocolError("unknown order-channel message: " + std::string(fields[0]));
                }
            }
        } catch (...) {
            {
                std::lock_guard lock(inbound_mutex_);
                if (!finished_.load()) reader_error_ = std::current_exception();
            }
            inbound_cv_.notify_all();
        }
    }

    asio::io_context io_;
    asio::ip::tcp::acceptor order_acceptor_;
    asio::ip::tcp::acceptor feed_acceptor_;
    asio::ip::tcp::socket order_socket_;
    asio::ip::tcp::socket feed_socket_;
    asio::streambuf order_buffer_;
    std::thread reader_;
    mutable std::mutex inbound_mutex_;
    std::condition_variable inbound_cv_;
    std::mutex order_send_mutex_;
    std::mutex feed_send_mutex_;
    std::deque<Inbound> inbound_;
    std::unordered_set<std::uint64_t> order_ids_;
    std::unordered_set<std::uint64_t> trade_ids_;
    std::exception_ptr reader_error_;
    SimTime lookahead_ = 0;
    SimTime engine_time_ = 0;
    SimTime venue_time_ = 0;
    std::atomic<SimTime> order_horizon_{0};
    SimTime feed_horizon_ = 0;
    SimTime last_feed_ts_ = 0;
    std::uint64_t last_seq_ = 0;
    bool started_ = false;
    bool ready_ = false;
    bool engine_finished_ = false;
    std::atomic_bool finished_{false};
    SimTime last_inbound_ts_ = 0;
    SimTime last_order_reply_ts_ = 0;
};

} // namespace cmf_driver
