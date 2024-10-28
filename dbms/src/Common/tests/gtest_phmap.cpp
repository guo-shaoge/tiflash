// Copyright 2024 PingCAP, Inc.
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

#include <common/types.h>
#include <Common/HashTable/PhHashTable.h>
#include <Common/HashTable/Hash.h>

#include <gtest/gtest.h>

namespace DB
{
namespace tests
{
TEST(PhMapTest, int128)
{
    using TKV = UInt64;
    using TMap = PhHashTable<TKV, char *, PhHash<TKV, PhHashSeed1>>;
    TMap map;
    TKV v1 = TKV(1);
    std::vector<TKV> keys{v1};
    for (auto key : keys)
    {
        typename TMap::LookupResult it;
        bool inserted = false;
        map.emplace(key, it, inserted);
        if (inserted)
            it->getMapped() = nullptr;

        const auto hashval = it->getHash(map);
    }
}
} // namespace tests
} // namespace DB
