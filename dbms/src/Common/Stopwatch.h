// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <common/defines.h>
#include <common/types.h>
#include <time.h>

#include <atomic>
#include <chrono>

#ifdef __APPLE__
#include <common/apple_rt.h>
#endif

inline UInt64 clock_gettime_ns(clockid_t clock_type = CLOCK_MONOTONIC)
{
    struct timespec ts
    {
    };
    clock_gettime(clock_type, &ts);
    return UInt64(ts.tv_sec * 1000000000ULL + ts.tv_nsec);
}

/// Sometimes monotonic clock may not be monotonic (due to bug in kernel?).
/// It may cause some operations to fail with "Timeout exceeded: elapsed 18446744073.709553 seconds".
/// Takes previously returned value and returns it again if time stepped back for some reason.
inline UInt64 clock_gettime_ns_adjusted(UInt64 prev_time, clockid_t clock_type = CLOCK_MONOTONIC)
{
    UInt64 current_time = clock_gettime_ns(clock_type);
    if (likely(prev_time <= current_time))
        return current_time;

    /// Something probably went completely wrong if time stepped back for more than 1 second.
    assert(prev_time - current_time <= 1000000000ULL);
    return prev_time;
}

class Stopwatch
{
public:
    using SteadyClock = std::chrono::steady_clock;

    // The parameter is useless, just be compatible with the older implementation.
    explicit Stopwatch(clockid_t)
    {
        start();
    }

    void start()
    {
        start_tp = SteadyClock::now();
        last_tp = start_tp;
        is_running = true;
    }

    void stop()
    {
        stop_tp = SteadyClock::now();
        is_running = false;
    }

    void reset()
    {
        start_tp = SteadyClock::time_point::min();
        stop_tp = SteadyClock::time_point::min();
        last_tp = SteadyClock::time_point::min();
        is_running = false;
    }

    void restart() { start(); }

    UInt64 elapsed() const
    {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(elapsedDuration()).count();
    }
    UInt64 elapsedMilliseconds() const
    {
        return std::chrono::duration_cast<std::chrono::milliseconds>(elapsedDuration()).count();
    }
    UInt64 elapsedSeconds() const
    {
        return std::chrono::duration_cast<std::chrono::seconds>(elapsedDuration()).count();
    }

    UInt64 elapsedFromLastTime()
    {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(elapsedFromLastTimeDuration()).count();
    }
    UInt64 elapsedMillisecondsFromLastTime()
    {
        return std::chrono::duration_cast<std::chrono::milliseconds>(elapsedFromLastTimeDuration()).count();
    }
    UInt64 elapsedSecondsFromLastTime()
    {
        return std::chrono::duration_cast<std::chrono::seconds>(elapsedFromLastTimeDuration()).count();
    }

private:
    SteadyClock::duration elapsedDuration() const
    {
        return is_running ? SteadyClock::now() - start_tp : stop_tp - start_tp;
    }
    SteadyClock::duration elapsedFromLastTimeDuration()
    {
        if (is_running)
        {
            const auto now = SteadyClock::now();
            const auto elapsed = now - last_tp;
            last_tp = now;
            return elapsed;
        }
        else
        {
            return stop_tp - last_tp;
        }
    }

    SteadyClock::time_point start_tp = SteadyClock::time_point::min();
    SteadyClock::time_point stop_tp = SteadyClock::time_point::min();
    SteadyClock::time_point last_tp = SteadyClock::time_point::min();

    bool is_running = false;
};

class AtomicStopwatch
{
public:
    explicit AtomicStopwatch(clockid_t clock_type_ = CLOCK_MONOTONIC)
        : start_ns(0)
        , clock_type(clock_type_)
    {
        restart();
    }

    void restart() { start_ns = nanoseconds(0); }
    UInt64 elapsed() const
    {
        UInt64 current_start_ns = start_ns;
        return nanoseconds(current_start_ns) - start_ns;
    }
    UInt64 elapsedMilliseconds() const { return elapsed() / 1000000UL; }
    double elapsedSeconds() const { return static_cast<double>(elapsed()) / 1000000000ULL; }

    /** If specified amount of time has passed, then restarts timer and returns true.
      * Otherwise returns false.
      * This is done atomically.
      */
    bool compareAndRestart(double seconds)
    {
        UInt64 threshold = seconds * 1000000000ULL;
        UInt64 current_start_ns = start_ns;
        UInt64 current_ns = nanoseconds(current_start_ns);

        while (true)
        {
            if (current_ns < current_start_ns + threshold)
                return false;

            if (start_ns.compare_exchange_weak(current_start_ns, current_ns))
                return true;
        }
    }

    struct Lock
    {
        AtomicStopwatch * parent = nullptr;

        Lock() = default;

        explicit operator bool() const { return parent != nullptr; }

        explicit Lock(AtomicStopwatch * parent)
            : parent(parent)
        {}

        Lock(Lock &&) = default;

        ~Lock()
        {
            if (parent)
                parent->restart();
        }
    };

    /** If specified amount of time has passed and timer is not locked right now, then returns Lock object,
      *  which locks timer and, on destruction, restarts timer and releases the lock.
      * Otherwise returns object, that is implicitly casting to false.
      * This is done atomically.
      *
      * Usage:
      * if (auto lock = timer.compareAndRestartDeferred(1))
      *        /// do some work, that must be done in one thread and not more frequently than each second.
      */
    Lock compareAndRestartDeferred(double seconds)
    {
        UInt64 threshold = seconds * 1000000000ULL;
        UInt64 current_start_ns = start_ns;
        UInt64 current_ns = nanoseconds(current_start_ns);

        while (true)
        {
            if ((current_start_ns & 0x8000000000000000ULL))
                return {};

            if (current_ns < current_start_ns + threshold)
                return {};

            if (start_ns.compare_exchange_weak(current_start_ns, current_ns | 0x8000000000000000ULL))
                return Lock(this);
        }
    }

private:
    std::atomic<UInt64> start_ns;
    std::atomic<bool> lock{false};
    clockid_t clock_type;

    /// Most significant bit is a lock. When it is set, compareAndRestartDeferred method will return false.
    /// todo: we may use stead_clock instead
    UInt64 nanoseconds(UInt64 prev_time) const
    {
        return clock_gettime_ns_adjusted(prev_time, clock_type) & 0x7FFFFFFFFFFFFFFFULL;
    }
};
