#include <iostream>
#include <cstdlib>
#include <string>
#include <vector>
#include <fmt/format.h>

#include <kvproto/resource_manager.pb.h>
#include <pingcap/pd/CodecClient.h>

void test(const std::string & pd_addr, const std::string & rg_name, int ru_cnt)
{
    resource_manager::TokenBucketsRequest gac_req;
    gac_req.set_client_unique_id(12875589);
    gac_req.set_target_request_period_ms(5000);

    auto * single_group_req = gac_req.add_requests();
    single_group_req->set_resource_group_name(rg_name);

    auto * ru_items = single_group_req->mutable_ru_items();
    auto * req_ru = ru_items->add_request_r_u();
    req_ru->set_type(resource_manager::RequestUnitType::RU);
    req_ru->set_value(ru_cnt);

    const pingcap::ClusterConfig config;

    auto pd_client = pingcap::pd::CodecClient(std::vector<std::string>{pd_addr}, config);
    auto resp = pd_client.acquireTokenBuckets(gac_req);

    std::cout << "req: " << gac_req.DebugString() << std::endl;
    std::cout << "resp: " << resp.DebugString() << std::endl;
}

int main(int argc, char * argv[])
{
    // bin pd_addr rgname acquire_count
    if (argc != 4)
    {
        std::cerr << "unexpected count of arg: bin pd_addr rgname acquire_count\n";
        return -1;
    }

    auto pd_addr = std::string(argv[1]);
    auto rg_name = std::string(argv[2]);
    auto ru_cnt = std::atoi(argv[3]);
    std::cout << fmt::format("pd_addr: {}, rg_name: {}, ru_cnt: {}\n", pd_addr, rg_name, ru_cnt);

    test(pd_addr, rg_name, ru_cnt);
    return 0;
}
