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

#include <Common/Exception.h>
#include <Flash/Executor/PipelineExecutorContext.h>
#include <Operators/LocalPartitionExchange.h>

namespace DB
{
std::pair<std::vector<LocalPartitionSinkHolderPtr>, std::vector<LocalPartitionSourceHolderPtr>>
LocalPartitionExchange::build(
    PipelineExecutorContext & exec_context,
    size_t num_partitions,
    size_t num_producers,
    Int64 max_buffered_bytes_per_queue)
{
    RUNTIME_CHECK(num_partitions > 0 && num_producers > 0);
    Int64 max_queue_size = std::max(num_producers, num_partitions) * 5;
    CapacityLimits queue_limits(max_queue_size, max_buffered_bytes_per_queue);
    auto exchange = std::make_shared<LocalPartitionExchange>(num_partitions, queue_limits, num_producers);
    exec_context.addLocalPartitionExchange(exchange);

    // One sink holder per producer to avoid race on pending_partition tracking.
    std::vector<LocalPartitionSinkHolderPtr> sink_holders;
    sink_holders.reserve(num_producers);
    for (size_t i = 0; i < num_producers; ++i)
        sink_holders.push_back(std::make_shared<LocalPartitionSinkHolder>(exchange, num_partitions));

    std::vector<LocalPartitionSourceHolderPtr> source_holders;
    source_holders.reserve(num_partitions);
    for (size_t i = 0; i < num_partitions; ++i)
        source_holders.push_back(std::make_shared<LocalPartitionSourceHolder>(exchange, i));

    return {std::move(sink_holders), std::move(source_holders)};
}

LocalPartitionExchange::LocalPartitionExchange(
    size_t num_partitions,
    CapacityLimits queue_limits,
    size_t num_producers)
    : active_producer(num_producers)
{
    queues.reserve(num_partitions);
    for (size_t i = 0; i < num_partitions; ++i)
        queues.emplace_back(
            queue_limits,
            [](const PartitionChunk & chunk) -> Int64 {
                auto total_rows = chunk.block.rows();
                if (!chunk.selective || total_rows == 0)
                    return chunk.block.allocatedBytes();
                // Proportional estimate: column data is shared via COWPtr, so attribute
                // memory based on the fraction of rows this partition holds.
                return static_cast<Int64>(
                    chunk.block.allocatedBytes() * chunk.selective->size() / total_rows
                    + chunk.selective->size() * sizeof(UInt64));
            });
}

MPMCQueueResult LocalPartitionExchange::tryPush(size_t partition_id, PartitionChunk && chunk)
{
    return queues[partition_id].tryPush(std::move(chunk));
}

MPMCQueueResult LocalPartitionExchange::tryPop(size_t partition_id, PartitionChunk & chunk)
{
    return queues[partition_id].tryPop(chunk);
}

void LocalPartitionExchange::producerFinish()
{
    auto cur_value = active_producer.fetch_sub(1);
    RUNTIME_CHECK(cur_value >= 1);
    if (1 == cur_value)
    {
        for (auto & queue : queues)
            queue.finish();
    }
}

void LocalPartitionExchange::cancel()
{
    for (auto & queue : queues)
        queue.cancel();
}

} // namespace DB
