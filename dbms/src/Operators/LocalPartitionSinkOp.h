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

#include <Common/WeakHash.h>
#include <Operators/LocalPartitionExchange.h>
#include <Operators/Operator.h>
#include <TiDB/Collation/Collator.h>

namespace DB
{
/**
 * Partitions each incoming block by hash(group_by_keys) and pushes
 * PartitionChunks (block + selective) to the corresponding partition queues.
 * No column data is copied — only selective index arrays are built.
 */
class LocalPartitionSinkOp : public SinkOp
{
public:
    LocalPartitionSinkOp(
        PipelineExecutorContext & exec_context_,
        const String & req_id,
        const LocalPartitionSinkHolderPtr & sink_holder_,
        const std::vector<Int64> & partition_col_ids_,
        const TiDB::TiDBCollators & collators_)
        : SinkOp(exec_context_, req_id)
        , sink_holder(sink_holder_)
        , partition_col_ids(partition_col_ids_)
        , collators(collators_)
        , partition_key_containers(partition_col_ids_.size())
        , num_partitions(sink_holder_->getNumPartitions())
        , selectives(num_partitions)
    {
        for (size_t i = 0; i < num_partitions; ++i)
            selectives[i] = std::make_shared<PartitionSelective>();
    }

    ~LocalPartitionSinkOp() override { sink_holder->finish(); }

    String getName() const override { return "LocalPartitionSinkOp"; }

    OperatorStatus prepareImpl() override;
    OperatorStatus writeImpl(Block && block) override;

private:
    OperatorStatus tryFlushPendingChunks();

    LocalPartitionSinkHolderPtr sink_holder;
    std::vector<Int64> partition_col_ids;
    TiDB::TiDBCollators collators;
    std::vector<String> partition_key_containers;
    size_t num_partitions;

    // Reusable per-partition selective arrays — cleared each block, not reallocated.
    std::vector<PartitionSelectivePtr> selectives;

    // Pending PartitionChunks waiting to be pushed.
    // pending_chunks[partition_id] = chunk for that partition.
    std::vector<PartitionChunk> pending_chunks;
    // Index of next partition to push.
    size_t next_pending_partition = 0;
};

} // namespace DB
