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

#include <Interpreters/Aggregator.h>
#include <Columns/ColumnDecimal.h>
#include <DataTypes/DataTypeDecimal.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <Interpreters/Context.h>
#include <Common/Logger.h>
#include <common/logger_useful.h>

#include <gtest/gtest.h>

namespace DB
{
namespace tests
{
class BenchAggregator : public ::testing::Test
{
public:
    using AggregatorPtr = std::shared_ptr<Aggregator>;

    void SetUp() override
    {
        log = Logger::get("bench_agg");
        try
        {
            registerAggregateFunctions();
            // todo: register agg funcs?
            col1_data_type = std::make_shared<DataTypeDecimal<Decimal128>>(prec, scale);
            col2_data_type = std::make_shared<DataTypeDecimal<Decimal128>>(prec, scale);

            auto col1 = col1_data_type->createColumn();
            auto col2 = col2_data_type->createColumn();
            before_agg_header.insert(ColumnWithTypeAndName(std::move(col1), col1_data_type, col1_name));
            before_agg_header.insert(ColumnWithTypeAndName(std::move(col2), col2_data_type, col2_name));

            context = TiFlashTestEnv::getContext();

            generateAggregator();

            input_blocks = generateBlocks(total_rows, rows_per_block);

            // const auto * col1_decimal = checkAndGetColumn<ColumnDecimal<Decimal128>>(input_blocks[0].getByPosition(0).column.get());
            // const auto * col2_decimal = checkAndGetColumn<ColumnDecimal<Decimal128>>(input_blocks[0].getByPosition(1).column.get());
            // for (size_t i = 0; i < col1_decimal->size(); ++i)
            // {
            //     LOG_DEBUG(log, "gjt debug input {}, {}", col1_decimal->dumpRow(i), col2_decimal->dumpRow(i));
            // }
        }
        catch (...)
        {
            LOG_ERROR(log, "gjt debug set up error {}", getCurrentExceptionMessage(true));
            exit(0);
        }
    }

    Block generateOneBlock(size_t rows_per_block)
    {
        auto col1 = col1_data_type->createColumn();
        auto col2 = col2_data_type->createColumn();
        for (size_t i = 0; i < rows_per_block; ++i)
        {
            // note: 1000000 == 10^scale
            Decimal128 v(i * 1000000);
            // note Field stores DecimalField
            col1->insert(Field(DecimalField<Decimal128>(v, scale)));
            col2->insert(Field(DecimalField<Decimal128>(v, scale)));
        }

        Block block;
        block.insert(ColumnWithTypeAndName(std::move(col1), col1_data_type, col1_name));
        block.insert(ColumnWithTypeAndName(std::move(col2), col2_data_type, col2_name));
        return block;
    }

    std::vector<Block> generateBlocks(size_t total_rows, size_t rows_per_block)
    {
        size_t cur_rows = 0;
        std::vector<Block> blocks;
        while (cur_rows < total_rows)
        {
            auto block = generateOneBlock(rows_per_block);
            blocks.push_back(block);
            cur_rows += rows_per_block;
        }
        return blocks;
    }

    void generateAggregator()
    {
        DataTypes arg_types{col1_data_type};
        AggregateDescription agg_description;
        // todo reg sum func?
        agg_description.function = AggregateFunctionFactory::instance().get(
                "sum", // func_name
                arg_types, // arg_types
                {}, // parameters
                0, // recursive_level
                false); // empty_input_as_null == true when group_by is empty.
        agg_description.parameters = Array();
        // sum(c1) group by c2;
        agg_description.arguments = ColumnNumbers{0};
        agg_description.argument_names = Names{col1_name};
        agg_description.column_name = fmt::format("sum({})", col1_name);

        SpillConfig spill_config(
                context->getTemporaryPath(),
                fmt::format("{}_aggregation", log->identifier()),
                context->getSettingsRef().max_cached_data_bytes_in_spiller,
                context->getSettingsRef().max_spilled_rows_per_file,
                context->getSettingsRef().max_spilled_bytes_per_file,
                context->getFileProvider(),
                context->getSettingsRef().max_threads,
                context->getSettingsRef().max_block_size);

        // todo meaning?
        // fine grained shuffle not enabled.
        // const auto agg_streams_size = 1;

        // const Settings & settings = context->getSettingsRef();
        // const bool allow_to_use_two_level_group_by = false; // isAllowToUseTwoLevelGroupBy(before_agg_streams_size, settings);
        // auto total_two_level_threshold_bytes
        //     = allow_to_use_two_level_group_by ? settings.group_by_two_level_threshold_bytes : SettingUInt64(0);

        // group by col2
        keys.push_back(1);
        Aggregator::Params params(
                before_agg_header, // src_header
                keys, // group by keys
                AggregateDescriptions{agg_description},
                0, // allow_to_use_two_level_group_by ? SettingUInt64(100000), SettingUInt64(0), // group by two level threshold
                0,// two level threshold getAverageThreshold(total_two_level_threshold_bytes, agg_streams_size),
                0,// max bytes before external getAverageThreshold(settings.max_bytes_before_external_group_by, agg_streams_size),
                false,// empty result for agg by empty set todo check this!
                spill_config,
                context->getSettingsRef().max_block_size,
                TiDB::dummy_collators);
        aggregator = std::make_shared<Aggregator>(params, /*req_id*/"bench_agg", /*concurrency*/1, /*spill_context*/nullptr);

        data_variants = std::make_unique<AggregatedDataVariants>();
    }

    const std::string col1_name = "col1_decimal128";
    const std::string col2_name = "col2_decimal128";
    const size_t rows_per_block = 4096;
    const size_t total_rows = 40000000;
    const size_t prec = 15;
    const size_t scale = 6;

    DataTypePtr col1_data_type;
    DataTypePtr col2_data_type;

    Block before_agg_header;
    ColumnNumbers keys;

    ContextPtr context;
    // note: should put aggregator before data_variants,
    // because aggregator should destroy after data_variants, check ~AggregatedDataVariants()
    std::shared_ptr<Aggregator> aggregator;
    AggregatedDataVariantsPtr data_variants;
    LoggerPtr log;

    std::vector<Block> input_blocks;
};

TEST_F(BenchAggregator, basic)
try
{
    Aggregator::AggProcessInfo info(aggregator.get());
    Stopwatch watch;
    for (auto & block : input_blocks)
    {
        info.resetBlock(block);
        ASSERT_TRUE(aggregator->executeOnBlock(/*agg_process_info*/info, /*result*/*data_variants, /*thread_index*/0, watch, false));
    }
    watch.stopAndAggBuild();

    Stopwatch convergent_watch;
    // todo meaning?
    ManyAggregatedDataVariants datas{data_variants};
    auto merging_buckets = aggregator->mergeAndConvertToBlocks(datas, /*final*/true, /*max_thread*/1);
    Block res_block;
    do
    {
        res_block = merging_buckets->getData(0, convergent_watch);
    } while (!res_block);
    convergent_watch.stopAndAggConvergent();

    // col2_decimal128 0 Decimal(19,6) Decimal128(size = 8192),
    // sum(col1_decimal128) 0 Decimal(41,6) Decimal256(size = 8192)
    auto res_structure = res_block.dumpStructure();
    LOG_DEBUG(log, "gjt debug res_structure {}, rows: {}", res_structure, res_block.rows());

    LOG_DEBUG(log, "gjt debug AggBuild: {}, EmplaceHashMap: {}, CreateAggState: {}, ComputeAggState: {}, AllocAggState: {}, AggConvert: {}, ConvertToBlocks: {}, computed IterHashTable: {}, IterHashTable: {}, InsertKeyColumns: {}, InsertAggVals: {}",
            watch.getAggBuild(), watch.getEmplaceHashMap(), watch.getCreateAggState(), watch.getComputeAggState(), watch.getAllocAggState(),
            convergent_watch.getAggConvergent(),  
            convergent_watch.getConvertToBlocks(),  
            convergent_watch.getConvertToBlocks() - convergent_watch.getInsertKeyColumns() - convergent_watch.getInsertAggVals(),
            convergent_watch.getIterHashMap(),
            convergent_watch.getInsertKeyColumns(),  
            convergent_watch.getInsertAggVals());

    EXPECT_EQ(res_block.rows(), rows_per_block);
    // auto res_col1 = res_block.getByPosition(0).column;
    // auto res_col2 = res_block.getByPosition(1).column;
    // const auto * res_col1_decimal128 = checkAndGetColumn<ColumnDecimal<Decimal128>>(res_col1.get());
    // const auto * res_col2_decimal256 = checkAndGetColumn<ColumnDecimal<Decimal256>>(res_col2.get());
    // for (size_t i = 0; i < res_block.rows(); ++i)
    // {
    //     Field field1;
    //     res_col1_decimal128->get(i, field1);
    //     auto v1 = field1.get<Int128>();

    //     Field field2;
    //     res_col2_decimal256->get(i, field2);
    //     auto v2 = field2.get<Int256>();
    //     EXPECT_EQ(v1 * 977, v2);
    // }

    // auto res_col1 = res_block.getByPosition(0).column;
    // auto res_col2 = res_block.getByPosition(1).column;
    // const auto * res_col1_decimal128 = checkAndGetColumn<ColumnDecimal<Decimal128>>(res_col1.get());
    // const auto * res_col2_decimal256 = checkAndGetColumn<ColumnDecimal<Decimal256>>(res_col2.get());
    // for (size_t i = 0; i < res_block.rows(); ++i)
    // {
    //     Field field1;
    //     res_col1_decimal128->get(i, field1);
    //     DecimalField field1_decimal(Decimal128(field1.get<Decimal128::NativeType>()), res_col1_decimal128->getScale());

    //     Field field2;
    //     res_col2_decimal256->get(i, field2);
    //     // todo how to get field not seeing column structure?
    //     DecimalField field2_decimal(Decimal256(field2.get<Decimal256::NativeType>()), res_col2_decimal256->getScale());

    //     LOG_DEBUG(log, "gjt debug col {}, {}, {}", i, field1_decimal.toString(), field2_decimal.toString());
    // }
}
CATCH

} // namespace tests
} // namespace DB
