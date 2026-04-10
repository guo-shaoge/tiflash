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

#include <Operators/LocalPartitionExchange.h>
#include <Operators/Operator.h>

#include <list>

namespace DB
{
/**
 * Reads PartitionChunks from its assigned partition queue, accumulates
 * rows up to max_block_size, then materializes a full block for downstream.
 */
class LocalPartitionSourceOp : public SourceOp
{
public:
    LocalPartitionSourceOp(
        PipelineExecutorContext & exec_context_,
        const String & req_id,
        const Block & header_,
        const LocalPartitionSourceHolderPtr & source_holder_,
        size_t max_block_size_)
        : SourceOp(exec_context_, req_id)
        , source_holder(source_holder_)
        , header(header_)
        , max_block_size(max_block_size_)
    {
        setHeader(header_);
    }

    String getName() const override { return "LocalPartitionSourceOp"; }

    OperatorStatus readImpl(Block & block) override;

private:
    Block materializeAccumulated();

    LocalPartitionSourceHolderPtr source_holder;
    Block header;
    size_t max_block_size;

    std::list<PartitionChunk> pending_chunks;
    size_t accumulated_rows = 0;
};

} // namespace DB
