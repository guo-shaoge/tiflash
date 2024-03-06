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

#ifdef __APPLE__
#include <common/apple_rt.h>
#endif

inline UInt64 clock_gettime_ns(clockid_t clock_type = CLOCK_MONOTONIC)
{
    struct timespec ts
    {
    };
    clock_gettime(clock_type, &ts);
    return static_cast<UInt64>(ts.tv_sec * 1000000000ULL + ts.tv_nsec);
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

/** Differs from Poco::Stopwatch only by using 'clock_gettime' instead of 'gettimeofday',
  *  returns nanoseconds instead of microseconds, and also by other minor differencies.
  */
class Stopwatch
{
public:
    /** CLOCK_MONOTONIC works relatively efficient (~15 million calls/sec) and doesn't lead to syscall.
      * Pass CLOCK_MONOTONIC_COARSE, if you need better performance with acceptable cost of several milliseconds of inaccuracy.
      */
    explicit Stopwatch(clockid_t clock_type_ = CLOCK_MONOTONIC)
        : clock_type(clock_type_)
    {
        start();
    }

    void start()
    {
        start_ns = nanosecondsWithBound(start_ns);
        last_ns = start_ns;
        is_running = true;
    }

    void stop()
    {
        stop_ns = nanosecondsWithBound(start_ns);
        is_running = false;
    }

    void reset()
    {
        start_ns = 0;
        stop_ns = 0;
        last_ns = 0;
        is_running = false;
    }

    void restart() { start(); }

    UInt64 elapsed() const { return is_running ? nanosecondsWithBound(start_ns) - start_ns : stop_ns - start_ns; }
    UInt64 elapsedMilliseconds() const { return elapsed() / 1000000UL; }
    double elapsedSeconds() const { return static_cast<double>(elapsed()) / 1000000000ULL; }

    UInt64 elapsedFromLastTime()
    {
        const auto now_ns = nanosecondsWithBound(last_ns);
        if (is_running)
        {
            auto rc = now_ns - last_ns;
            last_ns = now_ns;
            return rc;
        }
        else
        {
            return stop_ns - last_ns;
        }
    }

    UInt64 elapsedMillisecondsFromLastTime() { return elapsedFromLastTime() / 1000000UL; }
    double elapsedSecondsFromLastTime() { return static_cast<double>(elapsedFromLastTime()) / 1000000000ULL; }

    void stopAndAggBuild()
    {
        stop();
        agg_build += elapsed();
    }
    UInt64 getAggBuild() const { return agg_build; }
    void addAggBuild(UInt64 tmp)
    {
        agg_build += tmp;
    }

    void stopAndEmplaceHashMap()
    {
        stop();
        emplace_hash_map += elapsed();
    }
    UInt64 getEmplaceHashMap() const { return emplace_hash_map; }
    void addEmplaceHashMap(UInt64 tmp)
    {
        emplace_hash_map += tmp;
    }

    void stopAndCreateAggState()
    {
        stop();
        create_agg_state += elapsed();
    }
    UInt64 getCreateAggState() const { return create_agg_state; }
    void addCreateAggState(UInt64 tmp)
    {
        create_agg_state += tmp;
    }

    void stopAndComputeAggState()
    {
        stop();
        compute_agg_state += elapsed();
    }
    UInt64 getComputeAggState() const { return compute_agg_state; }
    void addComputeAggState(UInt64 tmp)
    {
        compute_agg_state += tmp;
    }

    void stopAndAggConvergent()
    {
        stop();
        agg_convergent += elapsed();
    }
    UInt64 getAggConvergent() const { return agg_convergent; }
    void addAggConvergent(UInt64 tmp)
    {
        agg_convergent += tmp;
    }

    void stopAndIterHashMap()
    {
        stop();
        iter_hash_map += elapsed();
    }
    UInt64 getIterHashMap() const { return iter_hash_map; }
    void addIterHashMap(UInt64 tmp)
    {
        iter_hash_map += tmp;
    }

    void stopAndInsertKeyColumns()
    {
        stop();
        insert_key_columns += elapsed();
    }
    UInt64 getInsertKeyColumns() const { return insert_key_columns; }
    void addInsertKeyColumns(UInt64 tmp)
    {
        insert_key_columns += tmp;
    }

    void stopAndInsertAggVals()
    {
        stop();
        insert_agg_vals += elapsed();
    }
    UInt64 getInsertAggVals() const { return insert_agg_vals; }
    void addInsertAggVals(UInt64 tmp)
    {
        insert_agg_vals += tmp;
    }

    void stopAndConvertToBlocks()
    {
        stop();
        convert_to_blocks += elapsed();
    }
    UInt64 getConvertToBlocks() const { return convert_to_blocks; }
    void addConvertToBlocks(UInt64 tmp)
    {
        convert_to_blocks += tmp;
    }
private:
    UInt64 start_ns = 0;
    UInt64 stop_ns = 0;
    UInt64 last_ns = 0;
    clockid_t clock_type;
    bool is_running = false;

    UInt64 agg_build = 0;
    UInt64 emplace_hash_map = 0;
    UInt64 create_agg_state = 0;
    UInt64 compute_agg_state = 0;

    UInt64 agg_convergent = 0;
    UInt64 iter_hash_map = 0;
    UInt64 insert_key_columns = 0;
    UInt64 insert_agg_vals = 0;
    UInt64 convert_to_blocks = 0;

    // Get current nano seconds, ensuring the return value is not
    // less than `lower_bound`.
    UInt64 nanosecondsWithBound(UInt64 lower_bound) const { return clock_gettime_ns_adjusted(lower_bound, clock_type); }
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
    UInt64 nanoseconds(UInt64 prev_time) const
    {
        return clock_gettime_ns_adjusted(prev_time, clock_type) & 0x7FFFFFFFFFFFFFFFULL;
    }
};
