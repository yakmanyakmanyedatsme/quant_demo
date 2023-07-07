use std::{mem, ops::RangeInclusive, os::raw::c_char, ptr::NonNull};
use databento_defs::record::{Mbp10Msg, Mbp1Msg, OhlcvMsg, TbboMsg, TradeMsg};
use dbz_lib::Dbz;
use dbz_lib::DbzStreamIter;
use std::io::BufRead;
use tokio::fs::{self, DirEntry,File,OpenOptions};
use tokio::macros::support::Pin;
use tokio::pin;

pub struct SoyFutures {
    hd: i64,
    price: i64,
    size: i64,
    action: i64,
    side: i64,
    flags: i64,
    depth: i64,
    ts_recv: i64,
    ts_in_delta: i64,
    sequence: i64,
}

pub trait DeserDbz {
     fn deserialize_soy_trade(&mut self) -> Option<SoyFutures>;
}
impl<R: BufRead, T> DeserDbz for DbzStreamIter<R, T>{
    fn deserialize_soy_trade(&mut self) -> Option<SoyFutures> {
        let trade = self.next(); 
        let soy_futures = SoyFutures {
            hd: i64::try_from(trade.hd.ts_event).unwrap(),
            price: i64::from(trade.price),
            size: i64::from(trade.size),
            action: i64::from(trade.action),
            side: i64::from(trade.side),
            flags: i64::from(trade.flags),
            depth: i64::from(trade.depth),
            ts_recv: i64::try_from(trade.ts_recv).unwrap(),
            ts_in_delta: i64::from(trade.ts_in_delta),
            sequence: i64::from(trade.sequence),
        };
        Some(soy_futures)
    }
}
pub async fn deserialize_dbz(entry: &DirEntry){
    let dbz = Dbz::from_file(entry.path()).unwrap();
    let mut commodity = dbz.metadata().dataset.to_string().clone();
    let mut dbz_trades = dbz.try_into_iter::<TradeMsg>().unwrap();
    while let Some(trade) = dbz_trades.deserialize_soy_trade().unwrap() {
        println!("{:?}",trade.unwrap());
    }
}
