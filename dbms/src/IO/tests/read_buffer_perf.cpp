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

#include <Core/Types.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>

#include <fstream>
#include <iostream>
#include <string>


int main(int, char **)
{
    try
    {
        DB::ReadBufferFromFile in("test");

        DB::Int64 a = 0;
        DB::Float64 b = 0;
        DB::String c, d;

        size_t i = 0;
        while (!in.eof())
        {
            DB::readIntText(a, in);
            in.ignore();

            DB::readFloatText(b, in);
            in.ignore();

            DB::readEscapedString(c, in);
            in.ignore();

            DB::readQuotedString(d, in);
            in.ignore();

            ++i;
        }

        std::cout << a << ' ' << b << ' ' << c << '\t' << '\'' << d << '\'' << std::endl;
        std::cout << i << std::endl;
    }
    catch (const DB::Exception & e)
    {
        std::cerr << e.what() << ", " << e.displayText() << std::endl;
        return 1;
    }

    return 0;
}
