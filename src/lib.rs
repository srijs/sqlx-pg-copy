#![doc = include_str!("../README.md")]

mod copy_in;
mod encode;
mod error;

pub use copy_in::CopyIn;
pub use encode::Row;
pub use error::{Error, Result};
