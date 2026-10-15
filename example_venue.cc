// ============================================================================
// A complete venue in one file, built on the CMF driver.
//
// It is deliberately small while staying honest about the protocol: one
// instrument, snapshot-based data, displayed-depth matching and a clear loop:
//
//     Start -> Ready -> { AdvanceTo, publish, answer } -> Finish
//
// Dataset rows use: timestamp|bid_price:qty,...|ask_price:qty,...
// With no paths, a deterministic synthetic feed is generated.
// ============================================================================

#include "cmf_driver.hpp"

#include <algorithm>
#include <cstdio>
#include <charconv>
#include <cmath>
#include <cstdlib>
#include <fstream>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace {

using cmf_driver::Level;
using cmf_driver::RejectReason;
using cmf_driver::Session;
using cmf_driver::Side;
using cmf_driver::SimTime;

// ----------------------------------------------------------------------------
constexpr const char* kClassCode = "QJSIM";
constexpr const char* kSecCode = "SBER";

/// The grid this venue trades on. Prices are compared in whole ticks and never
/// as doubles: two prices that print the same can differ in their last bit.
constexpr double kTickSize = 0.01;
constexpr double kMaxTickMagnitude = 9.0e18;

/// Where the run starts, and how far apart its events are. Real venues take
/// both from a dataset.
///
/// The step is worth a thought. Synchronisation costs one exchange of null
/// messages per lookahead a gap spans, so events spaced far wider than the
/// lookahead make the run crawl: a 10 ms gap at a 1 ms lookahead is ten round
/// trips before anything happens. Real tick data is denser than any sensible
/// lookahead and never notices; a synthetic feed like this one has to be told.
constexpr SimTime kStartTs = 1'000'000'000;
constexpr SimTime kStepNs = 200'000;
constexpr int kEventCount = 200;

struct Event {
    SimTime ts = 0;
    std::uint64_t seq = 0;
    std::vector<Level> bids;
    std::vector<Level> asks;
};

[[nodiscard]] bool OnGrid(double price);

[[nodiscard]] std::uint64_t ParseUnsigned(std::string_view value, const std::string& context) {
    std::uint64_t result = 0;
    const auto [end, ec] = std::from_chars(value.data(), value.data() + value.size(), result);
    if (value.empty() || ec != std::errc{} || end != value.data() + value.size())
        throw std::runtime_error("invalid " + context + ": " + std::string(value));
    return result;
}

[[nodiscard]] std::vector<std::string_view> Split(std::string_view text, char separator) {
    std::vector<std::string_view> fields;
    std::size_t start = 0;
    for (;;) {
        const auto end = text.find(separator, start);
        fields.push_back(text.substr(start, end == std::string_view::npos ? end : end - start));
        if (end == std::string_view::npos) break;
        start = end + 1;
    }
    return fields;
}

[[nodiscard]] std::vector<Level> ParseLevels(std::string_view field, const std::string& side) {
    std::vector<Level> levels;
    if (field.empty()) return levels;
    for (const std::string_view item : Split(field, ',')) {
        const auto parts = Split(item, ':');
        if (parts.size() != 2) throw std::runtime_error("malformed " + side + " level");
        const std::string price_text(parts[0]);
        char* end = nullptr;
        const double price = std::strtod(price_text.c_str(), &end);
        if (end != price_text.c_str() + price_text.size() || !std::isfinite(price) || price <= 0.0)
            throw std::runtime_error("invalid " + side + " price");
        if (!OnGrid(price)) throw std::runtime_error(side + " price is off the configured tick grid");
        const std::uint64_t qty = ParseUnsigned(parts[1], side + " quantity");
        if (qty == 0) throw std::runtime_error(side + " quantity must be positive");
        levels.push_back({price, qty});
        if (levels.size() > 64) throw std::runtime_error(side + " side exceeds 64 levels");
    }
    return levels;
}

[[nodiscard]] std::vector<Event> LoadEvents(const std::vector<std::string>& paths) {
    std::vector<Event> events;
    std::uint64_t seq = 0;
    for (const std::string& path : paths) {
        std::ifstream input(path);
        if (!input) throw std::runtime_error("cannot open dataset: " + path);
        std::string line;
        std::size_t line_number = 0;
        while (std::getline(input, line)) {
            ++line_number;
            if (!line.empty() && line.back() == '\r') line.pop_back();
            const auto first = line.find_first_not_of(" \t");
            if (first == std::string::npos || line[first] == '#') continue;
            const auto fields = Split(std::string_view(line).substr(first), '|');
            if (fields.size() != 3)
                throw std::runtime_error(path + ":" + std::to_string(line_number) +
                                         ": expected timestamp|bids|asks");
            Event event;
            event.ts = ParseUnsigned(fields[0], "timestamp");
            if (event.ts == 0 || event.ts == std::numeric_limits<SimTime>::max())
                throw std::runtime_error(path + ":" + std::to_string(line_number) + ": reserved timestamp");
            if (!events.empty() && event.ts < events.back().ts)
                throw std::runtime_error(path + ":" + std::to_string(line_number) +
                                         ": timestamps must be non-decreasing");
            event.seq = ++seq;
            event.bids = ParseLevels(fields[1], "bid");
            event.asks = ParseLevels(fields[2], "ask");
            for (std::size_t i = 1; i < event.bids.size(); ++i)
                if (event.bids[i - 1].price <= event.bids[i].price)
                    throw std::runtime_error(path + ":" + std::to_string(line_number) + ": bids must descend");
            for (std::size_t i = 1; i < event.asks.size(); ++i)
                if (event.asks[i - 1].price >= event.asks[i].price)
                    throw std::runtime_error(path + ":" + std::to_string(line_number) + ": asks must ascend");
            events.push_back(std::move(event));
        }
        if (!input.eof()) throw std::runtime_error("error reading dataset: " + path);
    }
    return events;
}

[[nodiscard]] std::vector<Event> SyntheticEvents() {
    std::vector<Event> events;
    events.reserve(kEventCount);
    for (int i = 0; i < kEventCount; ++i) {
        const double mid = 239.50 + kTickSize * static_cast<double>(i % 20);
        events.push_back({kStartTs + static_cast<SimTime>(i) * kStepNs,
                          static_cast<std::uint64_t>(i) + 1,
                          {{mid - kTickSize, 100}, {mid - 2 * kTickSize, 250}},
                          {{mid + kTickSize, 80}, {mid + 2 * kTickSize, 40}}});
    }
    return events;
}

[[nodiscard]] long long Ticks(double price) {
    if (!std::isfinite(price) || std::abs(price / kTickSize) >= kMaxTickMagnitude) {
        throw std::runtime_error("price is outside the supported tick range");
    }
    return static_cast<long long>(price / kTickSize + (price < 0 ? -0.5 : 0.5));
}

[[nodiscard]] bool OnGrid(double price) {
    if (!(price > 0.0) || !std::isfinite(price) ||
        std::abs(price / kTickSize) >= kMaxTickMagnitude) return false;
    return std::abs(price - static_cast<double>(Ticks(price)) * kTickSize) < kTickSize / 1024.0;
}

/// One order this venue is holding on its book.
struct Working {
    std::uint64_t client_id = 0;
    std::uint64_t order_id = 0;
    Side side = Side::Buy;
    long long price_ticks = 0;
    std::uint64_t leaves = 0;
    std::uint8_t tif = 0;
};

/// A venue: a book that moves, the orders resting against it, and the ids it
/// has issued.
class ExampleVenue {
public:
    explicit ExampleVenue(Session& session) : session_(session) {
        session_.on_new_order = [this](const cmf_driver::NewOrder& order) { OnNewOrder(order); };
        session_.on_cancel = [this](const cmf_driver::Cancel& cancel) { OnCancel(cancel); };
    }

    void Run(const std::vector<Event>& events) {
        if (events.empty()) throw std::runtime_error("dataset contains no market events");
        session_.Ready(events.front().ts);

        for (const Event& event : events) {
            // The one call that matters. It blocks while the engine has not
            // allowed us this far, draining orders and announcing where we
            // stand -- so by the time it returns, everything the engine has
            // said about times up to this event has reached our handlers.
            session_.AdvanceTo(event.ts);

            if (session_.EngineFinished()) {
                break;
            }

            bids_ = event.bids;
            asks_ = event.asks;
            session_.PublishSnapshot(event.seq, event.ts, kClassCode, kSecCode, bids_, asks_);

            // Anything resting that the new book now crosses.
            MatchAgainstBook(event.ts);
        }

        session_.Finish();
    }

private:
    void OnNewOrder(const cmf_driver::NewOrder& order) {
        // The order takes effect at effective_ts, never before: that delay is
        // the latency the run is measured at, and shortening it invents an
        // edge the engine was promised it would not get.
        const SimTime effective = order.effective_ts;

        if (order.class_code != kClassCode || order.sec_code != kSecCode) {
            session_.Reject(order.client_id, effective, RejectReason::UnknownInstrument);
            return;
        }

        // REFUSED, not rounded. If the engine's grid and ours disagree, every
        // quote lands on a price this book does not have and nothing matches;
        // refusing turns the quietest possible failure into a loud one on the
        // first order.
        if (!OnGrid(order.price)) {
            session_.Reject(order.client_id, effective, RejectReason::PriceOffTickGrid);
            return;
        }

        if (order.qty == 0) {
            session_.Reject(order.client_id, effective, RejectReason::InvalidParameters);
            return;
        }

        Working working;
        working.client_id = order.client_id;
        if (next_order_id_ == std::numeric_limits<std::uint64_t>::max())
            throw std::runtime_error("order id space exhausted");
        working.order_id = ++next_order_id_;
        working.side = order.side;
        working.price_ticks = Ticks(order.price);
        working.leaves = order.qty;
        working.tif = order.tif;

        // Accepted BEFORE any fill of it: the acceptance is where the id the
        // fill refers to comes from.
        session_.Accept(working.client_id, effective, working.order_id, working.leaves);

        book_.push_back(working);
        MatchOne(book_.back(), effective);
        Reap();
    }

    void OnCancel(const cmf_driver::Cancel& cancel) {
        const SimTime effective = cancel.effective_ts;

        // order_id == 0 is not malformed: on the way out the engine pulls
        // orders it has no id for yet. Answer, do not fall over.
        for (Working& working : book_) {
            if (working.order_id == cancel.order_id && working.leaves > 0) {
                working.leaves = 0;
                session_.Cancelled(cancel.client_id, effective, working.order_id);
                Reap();
                return;
            }
        }

        session_.CancelRejected(cancel.client_id, effective, cancel.order_id,
                                RejectReason::NoSuchOrder);
    }

    void MatchAgainstBook(SimTime ts) {
        for (Working& working : book_) {
            MatchOne(working, ts);
        }
        Reap();
    }

    void MatchOne(Working& working, SimTime ts) {
        if (working.leaves == 0) return;
        if (bids_.empty() || asks_.empty()) {
            ExpireImmediate(working, ts);
            return;
        }

        auto& contra = working.side == Side::Buy ? asks_ : bids_;
        const auto eligible = [&](const Level& level) {
            const long long level_ticks = Ticks(level.price);
            return working.side == Side::Buy ? level_ticks <= working.price_ticks
                                             : level_ticks >= working.price_ticks;
        };

        if (working.tif == 2) { // FOK: all quantity or none.
            std::uint64_t available = 0;
            for (const Level& level : contra) {
                if (!eligible(level)) break;
                if (std::numeric_limits<std::uint64_t>::max() - available < level.qty) {
                    available = std::numeric_limits<std::uint64_t>::max();
                    break;
                }
                available += level.qty;
            }
            if (available < working.leaves) {
                working.leaves = 0;
                session_.Cancelled(working.client_id, ts, working.order_id);
                return;
            }
        }

        // Consume shared displayed depth best-first. A later order sees only
        // the quantity left by earlier fills against this snapshot.
        for (Level& level : contra) {
            if (working.leaves == 0 || !eligible(level)) break;
            const std::uint64_t traded = std::min(working.leaves, level.qty);
            if (traded == 0) continue;
            level.qty -= traded;
            working.leaves -= traded;
            if (next_trade_id_ == std::numeric_limits<std::uint64_t>::max())
                throw std::runtime_error("trade id space exhausted");
            session_.Fill(working.client_id, ts, working.order_id, ++next_trade_id_,
                          level.price, traded, working.leaves);
        }

        // IOC and FOK orders never rest. DAY orders keep their remainder.
        if (working.leaves != 0 && working.tif != 0) {
            ExpireImmediate(working, ts);
        }
    }

    void ExpireImmediate(Working& working, SimTime ts) {
        if (working.leaves != 0 && working.tif != 0) {
            working.leaves = 0;
            session_.Cancelled(working.client_id, ts, working.order_id);
        }
    }

    /// Nothing more may be said about an order once it is finished, so it goes
    /// off the book the moment it is.
    void Reap() {
        std::erase_if(book_, [](const Working& working) { return working.leaves == 0; });
    }

    Session& session_;

    std::vector<Level> bids_;
    std::vector<Level> asks_;
    std::vector<Working> book_;

    std::uint64_t next_order_id_ = 9000;
    std::uint64_t next_trade_id_ = 70000;
};

} // namespace

int main(int argc, char** argv) {
    try {
        const cmf_driver::Options options = cmf_driver::ParseOptions(argc, argv);

        std::fprintf(stderr, "example venue: listening on %s, feed %u, orders %u, %zu dataset(s)\n",
                     options.host.c_str(), options.feed_port, options.order_port,
                     options.datasets.size());

        Session session(options);

        // Session construction binds both ports before dataset I/O, matching
        // the engine's startup contract.
        std::vector<Event> events = options.datasets.empty()
            ? SyntheticEvents()
            : LoadEvents(options.datasets);
        std::fprintf(stderr, "example venue: loaded %zu market event(s)\n", events.size());

        const SimTime lookahead = session.Start();
        std::fprintf(stderr, "example venue: engine declared a lookahead of %llu ns\n",
                     static_cast<unsigned long long>(lookahead));

        ExampleVenue(session).Run(events);

        std::fprintf(stderr, "example venue: dataset finished, said E on both channels\n");
        return EXIT_SUCCESS;

    } catch (const cmf_driver::ProtocolError& error) {
        std::fprintf(stderr, "example venue: PROTOCOL ERROR: %s\n", error.what());
        return EXIT_FAILURE;

    } catch (const std::exception& error) {
        std::fprintf(stderr, "example venue: %s\n", error.what());
        return EXIT_FAILURE;
    }
}
