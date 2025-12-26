use alloy::sol;

sol! {
    #[sol(rpc)]
    interface IERC20 {
        function name() external view returns (string);
        function symbol() external view returns (string);
        function decimals() external view returns (uint8);
    }

    event Transfer(address indexed from, address indexed to, uint256 value);
    event Deposit(address indexed user, uint256 amount);
    event Withdrawal(address indexed user, uint256 amount);
}
