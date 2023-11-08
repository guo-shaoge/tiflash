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

#include <Common/Stopwatch.h>

#include <gtest/gtest.h>
#include <chrono>
#include <thread>

namespace DB::tests
{

TEST(TestBasicStop, Basic)
{
    Stopwatch watch;
    std::this_thread::sleep_for(std::chrono::duration<std::chrono::seconds>(2));
    ASSERT_TRUE(watch.elapsed() >= 2 * 1'000'000'000);
    ASSERT_TRUE(watch.elapsedSeconds() >= 2);
    ASSERT_TRUE(watch.elapsedMilliseconds() >= 2000);
}

} // namespace DB::tests
