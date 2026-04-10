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

#include <Common/LooseBoundedMPMCQueue.h>
#include <Core/BlockInfo.h>
#include <Flash/Pipeline/Schedule/Tasks/NotifyFuture.h>
#include <Flash/Pipeline/Schedule/Tasks/Task.h>
#include <Operators/Operator.h>

#include <atomic>
#include <vector>

namespace DB
{
class PipelineExecutorContext;

/// A chunk of data for a specific partition: the original block plus
/// a selective vector indicating which rows belong to this partition.
/// Column data is shared via COWPtr — no data copy at push time.
struct PartitionChunk
{
    Block block;
    BlockSelectivePtr selective; /// Row indices for this partition (nullptr = all rows)
};

/**
 * LocalPartitionExchange partitions blocks by hash into N queues.
 *
 * N LocalPartitionSinkOp ──hash──► N partition queues ──► N LocalPartitionSourceOp
 *
 * Each sink op hashes incoming blocks and pushes PartitionChunks (block + selective)
 * to partition queues. Each source op reads from its own partition queue and
 * materializes rows.
 */
class LocalPartitionExchange;
using LocalPartitionExchangePtr = std::shared_ptr<LocalPartitionExchange>;

class LocalPartitionSinkHolder;
using LocalPartitionSinkHolderPtr = std::shared_ptr<LocalPartitionSinkHolder>;

class LocalPartitionSourceHolder;
using LocalPartitionSourceHolderPtr = std::shared_ptr<LocalPartitionSourceHolder>;

class LocalPartitionExchange
{
public:
    static std::pair<std::vector<LocalPartitionSinkHolderPtr>, std::vector<LocalPartitionSourceHolderPtr>> build(
        PipelineExecutorContext & exec_context,
        size_t num_partitions,
        size_t num_producers,
        Int64 max_buffered_bytes_per_queue = -1);

    LocalPartitionExchange(size_t num_partitions, CapacityLimits queue_limits, size_t num_producers);

    MPMCQueueResult tryPush(size_t partition_id, PartitionChunk && chunk);
    MPMCQueueResult tryPop(size_t partition_id, PartitionChunk & chunk);

    void producerFinish();

    void cancel();

    void registerReadTask(size_t partition_id, TaskPtr && task, NotifyType type)
    {
        queues[partition_id].registerPipeReadTask(std::move(task), type);
    }
    void registerWriteTask(size_t partition_id, TaskPtr && task, NotifyType type)
    {
        queues[partition_id].registerPipeWriteTask(std::move(task), type);
    }

    size_t getNumPartitions() const { return queues.size(); }

private:
    std::vector<LooseBoundedMPMCQueue<PartitionChunk>> queues;
    std::atomic_int32_t active_producer;
};

class LocalPartitionSinkHolder : public NotifyFuture
{
public:
    LocalPartitionSinkHolder(const LocalPartitionExchangePtr & exchange_, size_t num_partitions)
        : exchange(exchange_)
        , pending_partition(num_partitions)
    {}

    MPMCQueueResult tryPush(size_t partition_id, PartitionChunk && chunk)
    {
        return exchange->tryPush(partition_id, std::move(chunk));
    }
    void finish() { exchange->producerFinish(); }

    size_t getNumPartitions() const { return exchange->getNumPartitions(); }

    // For backpressure: track which partition caused FULL
    void setPendingPartition(size_t partition_id) { pending_partition = partition_id; }
    size_t getPendingPartition() const { return pending_partition; }

    void registerTask(TaskPtr && task) override
    {
        exchange->registerWriteTask(pending_partition, std::move(task), NotifyType::WAIT_ON_LOCAL_PARTITION_WRITE);
    }

private:
    LocalPartitionExchangePtr exchange;
    size_t pending_partition;
};

class LocalPartitionSourceHolder : public NotifyFuture
{
public:
    LocalPartitionSourceHolder(const LocalPartitionExchangePtr & exchange_, size_t partition_id_)
        : exchange(exchange_)
        , partition_id(partition_id_)
    {}

    MPMCQueueResult tryPop(PartitionChunk & chunk) { return exchange->tryPop(partition_id, chunk); }

    void registerTask(TaskPtr && task) override
    {
        exchange->registerReadTask(partition_id, std::move(task), NotifyType::WAIT_ON_LOCAL_PARTITION_READ);
    }

private:
    LocalPartitionExchangePtr exchange;
    size_t partition_id;
};

} // namespace DB
