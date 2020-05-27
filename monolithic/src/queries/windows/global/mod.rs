use std::rc::Rc;
use timely::dataflow::operators::capture::event::link::EventLink;
use timely::dataflow::operators::capture::Replay;
use timely::dataflow::{Scope, Stream};

use crate::event::{Auction, Bid, Date, Person};

mod window_2a_rocksdb_count;
mod window_2a_rocksdb_rank;
mod window_2b_rocksdb_count;
mod window_2b_rocksdb_rank;
mod window_2_faster_count;
mod window_2_faster_rank;

pub use self::window_2a_rocksdb_count::window_2a_rocksdb_count;
pub use self::window_2a_rocksdb_rank::window_2a_rocksdb_rank;
pub use self::window_2b_rocksdb_count::window_2b_rocksdb_count;
pub use self::window_2b_rocksdb_rank::window_2b_rocksdb_rank;
pub use self::window_2_faster_count::window_2_faster_count;
pub use self::window_2_faster_rank::window_2_faster_rank;
