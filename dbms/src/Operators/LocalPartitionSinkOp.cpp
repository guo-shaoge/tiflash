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

#include <Flash/Mpp/HashBaseWriterHelper.h>
#include <Operators/LocalPartitionSinkOp.h>

#include <magic_enum.hpp>

namespace DB
{
OperatorStatus LocalPartitionSinkOp::writeImpl(Block && block)
{
    if unlikely (!block)
        return OperatorStatus::FINISHED;

    size_t rows = block.rows();
    if unlikely (rows == 0)
        return OperatorStatus::NEED_INPUT;

    // 1. Compute hash for key columns.
    WeakHash32 hash(0);
    HashBaseWriterHelper::computeHash(block, partition_col_ids, collators, partition_key_containers, hash);

    // 2. Build per-partition selective arrays (no column data copy).
    const auto & hash_data = hash.getData();
    for (size_t i = 0; i < num_partitions; ++i)
        selectives[i]->clear();
    for (size_t i = 0; i < rows; ++i)
    {
        UInt64 partition_id = hash_data[i];
        partition_id *= num_partitions;
        partition_id >>= 32u;
        selectives[partition_id]->push_back(i);
    }

    // 3. Build pending PartitionChunks. Block copy only copies ColumnPtrs (COWPtr),
    //    not the underlying column data.
    pending_chunks.resize(num_partitions);
    for (size_t part = 0; part < num_partitions; ++part)
        pending_chunks[part] = PartitionChunk{block, std::move(selectives[part])};
    next_pending_partition = 0;

    // 4. Try to push all chunks.
    return tryFlushPendingChunks();
}

OperatorStatus LocalPartitionSinkOp::prepareImpl()
{
    return pending_chunks.empty() ? OperatorStatus::NEED_INPUT : tryFlushPendingChunks();
}

OperatorStatus LocalPartitionSinkOp::tryFlushPendingChunks()
{
    while (next_pending_partition < num_partitions)
    {
        auto & chunk = pending_chunks[next_pending_partition];
        // Skip empty partitions.
        if (chunk.selective->empty())
        {
            ++next_pending_partition;
            continue;
        }

        auto result = sink_holder->tryPush(next_pending_partition, std::move(chunk));
        switch (result)
        {
        case MPMCQueueResult::OK:
            ++next_pending_partition;
            break;
        case MPMCQueueResult::FULL:
            sink_holder->setPendingPartition(next_pending_partition);
            setNotifyFuture(sink_holder.get());
            return OperatorStatus::WAIT_FOR_NOTIFY;
        case MPMCQueueResult::CANCELLED:
            pending_chunks.clear();
            return OperatorStatus::CANCELLED;
        default:
            RUNTIME_CHECK_MSG(
                false,
                "Unexpected queue result for LocalPartitionSinkOp: {}",
                magic_enum::enum_name(result));
        }
    }

    pending_chunks.clear();
    return OperatorStatus::NEED_INPUT;
}

} // namespace DB
