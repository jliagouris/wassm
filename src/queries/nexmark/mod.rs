use std::rc::Rc;
use timely::dataflow::operators::capture::event::link::EventLink;
use timely::dataflow::operators::capture::Replay;
use timely::dataflow::{Scope, Stream};

use crate::event::{Auction, Bid, Date, Person};

mod q3_managed;
mod q4;
mod q4_managed;
mod q4_q6_common_managed;
mod q5_managed;
mod q5_managed_index;
mod q6_managed;
mod q7_managed;
mod q8_managed;
mod q8_managed_map;

pub use self::q3_managed::q3_managed;
pub use self::q4::q4;
pub use self::q4_managed::q4_managed;
pub use self::q4_q6_common_managed::q4_q6_common_managed;
pub use self::q5_managed::q5_managed;
pub use self::q5_managed_index::q5_managed_index;
pub use self::q6_managed::q6_managed;
pub use self::q7_managed::q7_managed;
pub use self::q8_managed::q8_managed;
pub use self::q8_managed_map::q8_managed_map;
