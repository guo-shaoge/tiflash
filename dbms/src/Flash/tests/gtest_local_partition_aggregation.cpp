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

#include <Interpreters/Context.h>
#include <TestUtils/ColumnGenerator.h>
#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/mockExecutor.h>

namespace DB
{
namespace tests
{

class LocalPartitionAggTestRunner : public ExecutorTest
{
public:
    void initializeContext() override
    {
        ExecutorTest::initializeContext();

        // 200 rows, 15 distinct keys
        {
            std::vector<std::optional<TypeTraits<int>::FieldType>> key(200);
            std::vector<std::optional<String>> value(200);
            for (size_t i = 0; i < 200; ++i)
            {
                key[i] = i % 15;
                value[i] = fmt::format("val_{}", i);
            }
            context.addMockTable(
                {"test_db", "t_15keys"},
                {{"key", TiDB::TP::TypeLong}, {"value", TiDB::TP::TypeString}},
                {toNullableVec<Int32>("key", key), toNullableVec<String>("value", value)});
        }

        // 200 rows, 200 distinct keys (all unique)
        {
            std::vector<std::optional<TypeTraits<int>::FieldType>> key(200);
            std::vector<std::optional<String>> value(200);
            for (size_t i = 0; i < 200; ++i)
            {
                key[i] = i;
                value[i] = fmt::format("val_{}", i);
            }
            context.addMockTable(
                {"test_db", "t_unique"},
                {{"key", TiDB::TP::TypeLong}, {"value", TiDB::TP::TypeString}},
                {toNullableVec<Int32>("key", key), toNullableVec<String>("value", value)});
        }

        // 200 rows, 1 distinct key (all same)
        {
            std::vector<std::optional<TypeTraits<int>::FieldType>> key(200);
            std::vector<std::optional<String>> value(200);
            for (size_t i = 0; i < 200; ++i)
            {
                key[i] = 0;
                value[i] = fmt::format("val_{}", i);
            }
            context.addMockTable(
                {"test_db", "t_single_key"},
                {{"key", TiDB::TP::TypeLong}, {"value", TiDB::TP::TypeString}},
                {toNullableVec<Int32>("key", key), toNullableVec<String>("value", value)});
        }

        // 1024 rows, 1024 distinct keys
        {
            std::vector<std::optional<TypeTraits<int>::FieldType>> key(1024);
            std::vector<std::optional<String>> value(1024);
            for (size_t i = 0; i < 1024; ++i)
            {
                key[i] = i;
                value[i] = fmt::format("val_{}", i);
            }
            context.addMockTable(
                {"test_db", "t_1024keys"},
                {{"key", TiDB::TP::TypeLong}, {"value", TiDB::TP::TypeString}},
                {toNullableVec<Int32>("key", key), toNullableVec<String>("value", value)});
        }

        // Nullable keys with NULLs
        {
            std::vector<std::optional<TypeTraits<int>::FieldType>> key1{1, 2, {}, 1, 2, {}, 3, 3, {}, 1};
            std::vector<std::optional<String>> key2{"a", "b", "a", "a", "b", {}, "c", "c", {}, "a"};
            std::vector<std::optional<TypeTraits<int>::FieldType>> val{10, 20, 30, 40, 50, 60, 70, 80, 90, 100};
            context.addMockTable(
                {"test_db", "t_nullable"},
                {{"key1", TiDB::TP::TypeLong}, {"key2", TiDB::TP::TypeString}, {"val", TiDB::TP::TypeLong}},
                {toNullableVec<Int32>("key1", key1),
                 toNullableVec<String>("key2", key2),
                 toNullableVec<Int32>("val", val)});
        }

        // Multi-type keys for hash distribution testing
        {
            size_t rows = 500;
            std::vector<std::optional<TypeTraits<Int64>::FieldType>> key_i64(rows);
            std::vector<std::optional<String>> key_str(rows);
            std::vector<std::optional<TypeTraits<int>::FieldType>> val(rows);
            for (size_t i = 0; i < rows; ++i)
            {
                key_i64[i] = static_cast<Int64>(i % 50);
                key_str[i] = fmt::format("grp_{}", i % 50);
                val[i] = static_cast<Int32>(i);
            }
            context.addMockTable(
                {"test_db", "t_multi_type"},
                {{"key_i64", TiDB::TP::TypeLongLong},
                 {"key_str", TiDB::TP::TypeString},
                 {"val", TiDB::TP::TypeLong}},
                {toNullableVec<Int64>("key_i64", key_i64),
                 toNullableVec<String>("key_str", key_str),
                 toNullableVec<Int32>("val", val)});
        }
    }

    void enableLocalPartition(bool enable_two_level = true)
    {
        context.context->setSetting("hashagg_enable_local_partition", Field(static_cast<UInt64>(1)));
        context.context->setSetting(
            "hashagg_local_partition_enable_two_level",
            Field(static_cast<UInt64>(enable_two_level ? 1 : 0)));
    }

    void disableLocalPartition()
    {
        context.context->setSetting("hashagg_enable_local_partition", Field(static_cast<UInt64>(0)));
    }
};

/// Basic correctness: local partition results must match non-local-partition baseline.
/// executeAndAssertColumnsEqual internally tests concurrencies {1, 10} x block_sizes {1, 2, 10, DEFAULT}.
TEST_F(LocalPartitionAggTestRunner, BasicCorrectness)
try
{
    std::vector<String> tables{"t_15keys", "t_unique", "t_single_key", "t_1024keys"};

    for (const auto & table : tables)
    {
        auto request
            = context.scan("test_db", table).aggregation({Max(col("value"))}, {col("key")}).build(context);

        // Baseline: no local partition, concurrency=1
        disableLocalPartition();
        auto baseline = executeStreams(request, 1);

        // Test with local partition (two_level enabled)
        enableLocalPartition(true);
        WRAP_FOR_TEST_BEGIN
        executeAndAssertColumnsEqual(request, baseline);
        WRAP_FOR_TEST_END
    }
}
CATCH

/// Verify correctness with two_level disabled in local partition mode.
TEST_F(LocalPartitionAggTestRunner, TwoLevelDisabled)
try
{
    std::vector<String> tables{"t_15keys", "t_unique", "t_single_key", "t_1024keys"};

    for (const auto & table : tables)
    {
        auto request
            = context.scan("test_db", table).aggregation({Max(col("value"))}, {col("key")}).build(context);

        disableLocalPartition();
        auto baseline = executeStreams(request, 1);

        enableLocalPartition(false);
        WRAP_FOR_TEST_BEGIN
        executeAndAssertColumnsEqual(request, baseline);
        WRAP_FOR_TEST_END
    }
}
CATCH

/// Test with nullable group-by keys.
TEST_F(LocalPartitionAggTestRunner, NullableKeys)
try
{
    // Single nullable key
    {
        auto request = context.scan("test_db", "t_nullable")
                           .aggregation({Max(col("val"))}, {col("key1")})
                           .build(context);
        disableLocalPartition();
        auto baseline = executeStreams(request, 1);

        enableLocalPartition(true);
        WRAP_FOR_TEST_BEGIN
        executeAndAssertColumnsEqual(request, baseline);
        WRAP_FOR_TEST_END
    }

    // Multiple nullable keys
    {
        auto request = context.scan("test_db", "t_nullable")
                           .aggregation({Max(col("val"))}, {col("key1"), col("key2")})
                           .build(context);
        disableLocalPartition();
        auto baseline = executeStreams(request, 1);

        enableLocalPartition(true);
        WRAP_FOR_TEST_BEGIN
        executeAndAssertColumnsEqual(request, baseline);
        WRAP_FOR_TEST_END
    }
}
CATCH

/// Test multiple aggregation functions at once.
TEST_F(LocalPartitionAggTestRunner, MultipleAggFunctions)
try
{
    auto request = context.scan("test_db", "t_multi_type")
                       .aggregation({Max(col("val")), Min(col("val")), Count(col("val"))}, {col("key_i64")})
                       .build(context);

    disableLocalPartition();
    auto baseline = executeStreams(request, 1);

    enableLocalPartition(true);
    WRAP_FOR_TEST_BEGIN
    executeAndAssertColumnsEqual(request, baseline);
    WRAP_FOR_TEST_END

    enableLocalPartition(false);
    WRAP_FOR_TEST_BEGIN
    executeAndAssertColumnsEqual(request, baseline);
    WRAP_FOR_TEST_END
}
CATCH

/// Test with multi-column group by keys including string type.
TEST_F(LocalPartitionAggTestRunner, MultiColumnGroupBy)
try
{
    auto request = context.scan("test_db", "t_multi_type")
                       .aggregation({Max(col("val"))}, {col("key_i64"), col("key_str")})
                       .build(context);

    disableLocalPartition();
    auto baseline = executeStreams(request, 1);

    enableLocalPartition(true);
    WRAP_FOR_TEST_BEGIN
    executeAndAssertColumnsEqual(request, baseline);
    WRAP_FOR_TEST_END
}
CATCH

/// Test aggregation without group by keys (global aggregation).
TEST_F(LocalPartitionAggTestRunner, NoGroupByKey)
try
{
    std::vector<String> tables{"t_15keys", "t_unique"};
    for (const auto & table : tables)
    {
        auto request
            = context.scan("test_db", table).aggregation({Max(col("value")), Count(col("key"))}, {}).build(context);

        disableLocalPartition();
        auto baseline = executeStreams(request, 1);

        enableLocalPartition(true);
        WRAP_FOR_TEST_BEGIN
        executeAndAssertColumnsEqual(request, baseline);
        WRAP_FOR_TEST_END
    }
}
CATCH

/// Test block size splitting with local partition.
TEST_F(LocalPartitionAggTestRunner, BlockSizeSplitting)
try
{
    std::vector<String> tables{"t_15keys", "t_1024keys"};
    std::vector<size_t> max_block_sizes{1, 9, 64, DEFAULT_BLOCK_SIZE};
    std::vector<size_t> expect_rows{15, 1024};

    for (size_t i = 0; i < tables.size(); ++i)
    {
        auto request
            = context.scan("test_db", tables[i]).aggregation({Max(col("value"))}, {col("key")}).build(context);

        for (auto block_size : max_block_sizes)
        {
            context.context->setSetting("max_block_size", Field(static_cast<UInt64>(block_size)));
            enableLocalPartition(true);
            WRAP_FOR_TEST_BEGIN
            auto blocks = getExecuteStreamsReturnBlocks(request, 4);
            size_t actual_rows = 0;
            for (auto & block : blocks)
            {
                ASSERT(block.rows() <= block_size);
                actual_rows += block.rows();
            }
            ASSERT_EQ(actual_rows, expect_rows[i]);
            WRAP_FOR_TEST_END
        }
    }
}
CATCH

} // namespace tests
} // namespace DB
