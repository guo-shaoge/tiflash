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
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wnon-virtual-dtor"
#ifdef __clang__
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
#include <kvproto/tikvpb.grpc.pb.h>
#undef DEFAULT_BLOCK_SIZE
#include <butil/iobuf.h>
#include <brpc/stream.h>
#include <butil/errno.h>
#define DEFAULT_BLOCK_SIZE 65536

#include <Common/Logger.h>
#include <common/logger_useful.h>

#pragma GCC diagnostic pop

namespace DB
{
// PacketWriter is a common interface of sync gRPC writer.
// It is used as the template parameter of `MPPTunnel`.
class PacketWriter
{
public:
    virtual ~PacketWriter() = default;

    // Write a packet and return false if any error occurs.
    virtual bool write(const mpp::MPPDataPacket & packet) = 0;

    virtual bool needDelete() { return false; }
};

class SyncPacketWriter : public PacketWriter
{
public:
    explicit SyncPacketWriter(grpc::ServerWriter<mpp::MPPDataPacket> * writer)
        : writer(writer)
    {}

    bool write(const mpp::MPPDataPacket & packet) override { return writer->Write(packet); }

private:
    ::grpc::ServerWriter<::mpp::MPPDataPacket> * writer;
};

class BRPCSyncPacketWriter : public PacketWriter
{
public:
    BRPCSyncPacketWriter(const brpc::StreamId & id, LoggerPtr log_) : stream_id(id), log(log_) {}

    virtual ~BRPCSyncPacketWriter()
    {
        auto err = brpc::StreamClose(stream_id);
        if (err != 0)
        {
            LOG_ERROR(log, "close brpc stream failed {}", err);
        }
    }

    virtual bool write(const mpp::MPPDataPacket & packet) override
    {
        butil::IOBuf msg;
        msg.append(packet.SerializeAsString());
        bool ok = true;
        while (true)
        {
            auto err = brpc::StreamWrite(stream_id, msg);
            if (err != 0)
            {
                LOG_DEBUG(log, "gjt debug brpc write: {}, beg StreamWait", err);
                if (err == EAGAIN)
                {
                    err = brpc::StreamWait(stream_id, NULL);
                    LOG_DEBUG(log, "gjt debug brpc wait done, : {}", err);
                    if (err != 0)
                    {
                        LOG_DEBUG(log, "gjt debug brpc write: {}", err);
                        ok = false;
                        break;
                    }
                    continue;
                }
                else
                {
                    ok = false;
                    break;
                }
            }
            else
            {
                ok = true;
                break;
            }
        }
        return ok;
    }

    virtual bool needDelete() override { return true; }

private:
    brpc::StreamId stream_id;
    LoggerPtr log;
};

}; // namespace DB
