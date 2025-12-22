pub mod erc20;
pub mod multicall;
pub mod transfer;
pub mod v2;
pub mod v3;
pub mod v4;

pub use erc20::IERC20;
pub use multicall::{Call3, IMulticall3, McResult};
pub use transfer::{Deposit, Transfer, Withdrawal};
pub use v2::{Burn as V2Burn, Mint as V2Mint, PairCreated, Swap as V2Swap, Sync};
pub use v3::{
    Burn as V3Burn, Collect, Initialize as V3Initialize, Mint as V3Mint, PoolCreated,
    Swap as V3Swap,
};
pub use v4::{Initialize as V4Initialize, ModifyLiquidity, Swap as V4Swap};
