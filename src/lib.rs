//! Single-producer multi-consumer channels.
#![feature(get_mut_unchecked)]

pub(crate) mod common;

pub mod bounded;
pub mod error;
pub mod unbounded;
