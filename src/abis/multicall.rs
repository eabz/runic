use alloy::sol;

sol! {
    struct Call3 {
        address target;
        bool allowFailure;
        bytes callData;
    }

    struct McResult {
        bool success;
        bytes returnData;
    }

    #[sol(rpc)]
    interface IMulticall3 {
        function aggregate3(Call3[] calldata calls) external payable returns (McResult[] memory returnData);
    }
}
