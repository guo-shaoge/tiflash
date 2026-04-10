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

#include <Operators/LocalPartitionSourceOp.h>

#include <magic_enum.hpp>

namespace DB
{
OperatorStatus LocalPartitionSourceOp::readImpl(Block & block)
{
    // If we already have enough accumulated rows, materialize and emit.
    if (accumulated_rows >= max_block_size)
    {
        block = materializeAccumulated();
        return OperatorStatus::HAS_OUTPUT;
    }

    // Try to pop more chunks from queue.
    PartitionChunk chunk;
    auto queue_result = source_holder->tryPop(chunk);
    switch (queue_result)
    {
    case MPMCQueueResult::OK:
    {
        size_t chunk_rows = chunk.selective ? chunk.selective->size() : chunk.block.rows();
        accumulated_rows += chunk_rows;
        pending_chunks.push_back(std::move(chunk));
        if (accumulated_rows >= max_block_size)
        {
            block = materializeAccumulated();
            return OperatorStatus::HAS_OUTPUT;
        }
        // Not enough rows yet — emit what we have to avoid busy-looping.
        // The downstream aggregator handles any block size.
        block = materializeAccumulated();
        return OperatorStatus::HAS_OUTPUT;
    }
    case MPMCQueueResult::EMPTY:
        // Queue empty but producers still active.
        if (accumulated_rows > 0)
        {
            block = materializeAccumulated();
            return OperatorStatus::HAS_OUTPUT;
        }
        setNotifyFuture(source_holder.get());
        return OperatorStatus::WAIT_FOR_NOTIFY;
    case MPMCQueueResult::FINISHED:
        // All producers done — flush remaining buffer.
        if (accumulated_rows > 0)
        {
            block = materializeAccumulated();
            return OperatorStatus::HAS_OUTPUT;
        }
        return OperatorStatus::HAS_OUTPUT; // empty block signals done
    case MPMCQueueResult::CANCELLED:
        return OperatorStatus::CANCELLED;
    default:
        RUNTIME_CHECK_MSG(
            false,
            "Unexpected queue result for LocalPartitionSourceOp: {}",
            magic_enum::enum_name(queue_result));
    }
}

Block LocalPartitionSourceOp::materializeAccumulated()
{
    Block result = header.cloneEmpty();
    size_t num_cols = result.columns();

    // Reserve total rows.
    MutableColumns columns = result.mutateColumns();
    for (size_t col = 0; col < num_cols; ++col)
        columns[col]->reserve(accumulated_rows);

    // Append selected rows from each chunk.
    for (auto & chunk : pending_chunks)
    {
        const auto & selective = chunk.selective;
        for (size_t col = 0; col < num_cols; ++col)
        {
            const auto & src_col = chunk.block.getByPosition(col).column;
            if (!selective)
            {
                columns[col]->insertRangeFrom(*src_col, 0, src_col->size());
            }
            else
            {
                for (auto idx : *selective)
                    columns[col]->insertFrom(*src_col, idx);
            }
        }
    }

    result.setColumns(std::move(columns));
    pending_chunks.clear();
    accumulated_rows = 0;
    return result;
}

} // namespace DB
