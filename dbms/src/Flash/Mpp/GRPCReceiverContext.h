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

#include <Common/grpcpp.h>
#include <Flash/Coprocessor/ChunkCodec.h>
#include <Flash/Mpp/LocalRequestHandler.h>
#include <Flash/Mpp/MPPTaskManager.h>
#include <common/types.h>
#include <grpcpp/completion_queue.h>
#include <kvproto/mpp.pb.h>
#include <pingcap/kv/Cluster.h>
#include <tipb/executor.pb.h>
// /root/tiflash/contrib/brpc/src/butil/iobuf.h:68:25: error: expected member name or ';' after declaration specifiers
//    68 |     static const size_t DEFAULT_BLOCK_SIZE = 8192;
//       |     ~~~~~~~~~~~~~~~~~~~ ^
// /root/tiflash/dbms/src/Core/Defines.h:63:28: note: expanded from macro 'DEFAULT_BLOCK_SIZE'
//    63 | #define DEFAULT_BLOCK_SIZE 65536
//       |                            ^
#undef DEFAULT_BLOCK_SIZE
#include <brpc/stream.h>
#define DEFAULT_BLOCK_SIZE 65536
#include <brpc/channel.h>
#include <butil/iobuf.h>
#include <kvproto/tiflashbrpc.pb.h>

#include <memory>

namespace DB
{
using MPPDataPacket = mpp::MPPDataPacket;
using TrackedMppDataPacketPtr = std::shared_ptr<DB::TrackedMppDataPacket>;
using TrackedMPPDataPacketPtrs = std::vector<TrackedMppDataPacketPtr>;

class ExchangePacketReader
{
public:
    virtual ~ExchangePacketReader() = default;
    virtual bool read(TrackedMppDataPacketPtr & packet) = 0;
    virtual grpc::Status finish() = 0;
    virtual void cancel(const String & reason) = 0;
};
using ExchangePacketReaderPtr = std::unique_ptr<ExchangePacketReader>;

class AsyncExchangePacketReader
{
public:
    virtual ~AsyncExchangePacketReader() = default;
    virtual void init(GRPCKickTag * tag) = 0;
    virtual void read(TrackedMppDataPacketPtr & packet, GRPCKickTag * tag) = 0;
    virtual void finish(::grpc::Status & status, GRPCKickTag * tag) = 0;
    virtual grpc::ClientContext * getClientContext() = 0;
};
using AsyncExchangePacketReaderPtr = std::unique_ptr<AsyncExchangePacketReader>;

struct ExchangeRecvRequest
{
    Int64 source_index = -1;
    Int64 send_task_id
        = -2; // Do not use -1 as default, since -1 has special meaning to show it's the root sender from the TiDB.
    Int64 recv_task_id = -2;
    mpp::EstablishMPPConnectionRequest req;
    bool is_local = false;

    String debugString() const;
};

class GRPCReceiverContext
{
public:
    using Status = grpc::Status;
    using Request = ExchangeRecvRequest;
    using Reader = ExchangePacketReader;
    using AsyncReader = AsyncExchangePacketReader;

    explicit GRPCReceiverContext(
        const tipb::ExchangeReceiver & exchange_receiver_meta_,
        const mpp::TaskMeta & task_meta_,
        pingcap::kv::Cluster * cluster_,
        std::shared_ptr<MPPTaskManager> task_manager_,
        bool enable_local_tunnel_,
        bool enable_async_grpc_);

    ExchangeRecvRequest makeRequest(int index) const;

    bool supportAsync(const ExchangeRecvRequest & request) const;

    ExchangePacketReaderPtr makeReader(const ExchangeRecvRequest & request) const;

    ExchangePacketReaderPtr makeSyncReader(const ExchangeRecvRequest & request) const;

    AsyncExchangePacketReaderPtr makeAsyncReader(
        const ExchangeRecvRequest & request,
        grpc::CompletionQueue * cq,
        GRPCKickTag * tag) const;

    static Status getStatusOK() { return grpc::Status::OK; }

    void fillSchema(DAGSchema & schema) const;

    void establishMPPConnectionLocalV2(
        const ExchangeRecvRequest & request,
        size_t source_index,
        LocalRequestHandler & local_request_handler,
        bool has_remote_conn);

    static std::tuple<MPPTunnelPtr, grpc::Status> establishMPPConnectionLocalV1(
        const ::mpp::EstablishMPPConnectionRequest * request,
        const std::shared_ptr<MPPTaskManager> & task_manager);

private:
    tipb::ExchangeReceiver exchange_receiver_meta;
    mpp::TaskMeta task_meta;
    pingcap::kv::Cluster * cluster;
    std::shared_ptr<MPPTaskManager> task_manager;
    bool enable_local_tunnel;
    bool enable_async_grpc;
};

struct BRPCContext
{

    BRPCContext() = default;

    struct StreamReceiver : public brpc::StreamInputHandler
    {
        StreamReceiver(std::shared_ptr<LooseBoundedMPMCQueue<mpp::MPPDataPacket>> q_ptr_,
            LoggerPtr log_)
            : brpc::StreamInputHandler()
            , q(q_ptr_)
            , log(log_)
        {}
        virtual ~StreamReceiver() = default;

        virtual int on_received_messages(brpc::StreamId id,
                butil::IOBuf * const messages[],
                size_t size) override
        {
            RUNTIME_CHECK(id == myid);
            LOG_DEBUG(log, "gjt debug got brpc packet: {}", size);
            for (size_t i = 0; i < size; ++i)
            {
                mpp::MPPDataPacket packet;
                // TODO not copy 
                // https://github.com/apache/brpc/blob/master/docs/cn/iobuf.md#%E8%A7%A3%E6%9E%90
                // IOBufAsZeroCopyInputStream wrapper(&iobuf);
                // pb_message.ParseFromZeroCopyStream(&wrapper);
                packet.ParseFromString(messages[i]->to_string());
                q->forcePush(std::move(packet));
            }
            LOG_DEBUG(log, "gjt debug got brpc packet: done {}", size);
            return 0;
        }

        virtual void on_idle_timeout(brpc::StreamId id) override
        {
            RUNTIME_CHECK(id == myid);

            LOG_INFO(log, "brpc stream got idle timeout: {}", id);
        }

        virtual void on_closed(brpc::StreamId id) override
        {
            RUNTIME_CHECK(myid == id);

            LOG_INFO(log, "brpc stream got closed: {}", id);
            RUNTIME_CHECK(q->finish());
        }

        std::shared_ptr<LooseBoundedMPMCQueue<mpp::MPPDataPacket>> q;
        LoggerPtr log;
        brpc::StreamId myid{brpc::INVALID_STREAM_ID};
    };

    bool init(LoggerPtr log, const ExchangeRecvRequest & req)
    {
        channel = std::make_unique<brpc::Channel>();
        brpc::ChannelOptions channel_opts;
        channel_opts.timeout_ms = 10000;
        // TODO change rpc server port for brpc
        auto addr = req.req.sender_meta().address();
        auto find_res = addr.find(":");
        if (find_res == std::string::npos)
        {
            LOG_ERROR(log, "unexpected flash addr: {}", addr);
            throw Exception("unexpected flash addr", ErrorCodes::LOGICAL_ERROR);
        }
        addr.resize(find_res);
        addr = addr + ":13931";
        // auto find_res = addr.find(":3930");
        // if (find_res == std::string::npos)
        // {
        //     LOG_ERROR(log, "unexpected sender addr: {}", addr);
        //     return false;
        // }
        // addr = addr.replace(find_res, 5, ":3931");
        LOG_DEBUG(log, "brpc sender addr: {}", addr);
        if (channel->Init(addr.c_str(), &channel_opts) != 0)
        {
            LOG_ERROR(log, "init brpc channel failed");
            return false;
        }

        packet_queue = std::make_shared<LooseBoundedMPMCQueue<mpp::MPPDataPacket>>(50);
        handler = std::make_unique<StreamReceiver>(packet_queue, log);

        cntl = std::make_unique<brpc::Controller>();
        brpc::StreamOptions stream_options;
        stream_options.max_buf_size = 0;
        stream_options.min_buf_size = 0;
        stream_options.handler = handler.get();
        if (brpc::StreamCreate(&stream_id, *cntl, &stream_options) != 0)
        {
            LOG_ERROR(log, "init brpc stream failed");
            return false;
        }
        handler->myid = stream_id;

        stub = std::make_unique<tiflashbrpc::TiFlashBRPC_Stub>(channel.get());

        mpp::EstablishBRPCMPPConnectionResponse mpp_resp;
        stub->EstablishBRPCMPPConnection(cntl.get(), &req.req, &mpp_resp, NULL);
        if (cntl->Failed())
        {
            LOG_ERROR(log, "call EstablishBRPCMPPConnection for brpc failed: {}", cntl->ErrorText());
            return false;
        }
        if (mpp_resp.has_error())
        {
            LOG_ERROR(log, "call EstablishBRPCMPPConnection for brpc failed");
            return false;
        }
        return true;
    }

    bool read(TrackedMppDataPacketPtr tracked_packet) const
    {
        mpp::MPPDataPacket packet;
        auto res = packet_queue->pop(packet);
        tracked_packet->read(std::move(packet));
        return res == MPMCQueueResult::OK;
    }

    std::pair<int, std::string> getError()
    {
        return std::make_pair(cntl->ErrorCode(), cntl->ErrorText());
    }

    std::unique_ptr<brpc::Channel> channel{};
    std::unique_ptr<tiflashbrpc::TiFlashBRPC_Stub> stub{};
    std::unique_ptr<brpc::Controller> cntl{};
    brpc::StreamId stream_id{};
    std::unique_ptr<StreamReceiver> handler;
    std::shared_ptr<LooseBoundedMPMCQueue<mpp::MPPDataPacket>> packet_queue{};
};
} // namespace DB
