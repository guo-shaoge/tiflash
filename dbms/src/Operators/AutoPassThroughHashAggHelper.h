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

#pragma once

#include <Core/Block.h>
#include <Interpreters/AggregateDescription.h>

namespace DB
{
using AutoPassThroughColumnGenerator = std::function<ColumnPtr(const Block & child_block)>;

// todo description
std::vector<AutoPassThroughColumnGenerator> setupAutoPassThroughColumnGenerator(
    const Block & required_header,
    const Block & child_header,
    const AggregateDescriptions & aggregate_descriptions);

ColumnPtr genPassThroughColumnGeneric(const AggregateDescription & desc, const Block & child_block);
} // namespace DB