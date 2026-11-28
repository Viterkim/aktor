//! Compact worker data in aktor's own format.
//!
//! The [`AktorData`] trait, [`Reader`] and [`Writer`] are available in every build. The
//! `macros` feature (on by default) supplies the derive, so a struct or enum shared with a
//! worker can keep its derive even in a build without `transport`. `#[aktor(skip)]` leaves
//! a field local and restores its default when receiving the value.
//!
//! A nested type with the same name as yours can look like a recursive reference.
//! If the derive misses a bound on `wire::Packet<T>`, import it as `WirePacket`.
//!
//! The derive can't see through type aliases either, `type Link<T> = Option<Box<Node<T>>>`
//! inside `Node<T>` sends it in circles, and so do two generic types holding each other like
//! `Expr<M>` and `Stmt<M>`. You find out with an "overflow evaluating" error at first use.
//! Write the bounds yourself with `#[aktor(bound = "T: AktorData")]`, on both types for the
//! pair, and the derive uses only those.
//!
//! A message can build about 256 bytes of values for every byte it carries, plus 8 MiB.
//! That keeps a small message full of `Box<()>` or empty variants of a big enum from
//! claiming gigabytes on the receiver. The encoder checks the same, so the sender hears
//! about it first.

mod bits;
mod decode;
mod encode;
mod fixed;
mod impls;
mod types;

#[cfg(feature = "transport")]
pub use decode::decode;
#[cfg(feature = "transport")]
pub use encode::encode;
pub use types::{AktorData, DataError, DataReason, Reader, Writer};

#[cfg(feature = "transport")]
use core::cell::Cell;

const MAX_DEPTH: usize = 128;

/// How many values the bytes so far may hold. Every nested value counts one, so a short
/// `Vec<()>` length can't turn into a loop over billions of units.
fn value_budget(bytes: usize) -> usize {
    bytes.saturating_mul(64).saturating_add(512)
}

/// What the values may take in memory. A value that encodes to nothing still takes its size,
/// and without this a few KB of `Box<()>` or empty enum variants could build gigabytes.
#[cfg(feature = "transport")]
#[doc(hidden)]
pub fn memory_budget(bytes: usize) -> usize {
    bytes.saturating_mul(256).saturating_add(8 << 20)
}

#[cfg(feature = "transport")]
std::thread_local! {
    /// Memory the running `encode` or `decode` may still build. `IDLE` outside of them.
    static LEFT: Cell<usize> = const { Cell::new(IDLE) };
    /// Something didn't fit. Only failures write this, the hot path stays one counter.
    static OVER: Cell<bool> = const { Cell::new(false) };
    static DECODING: Cell<bool> = const { Cell::new(false) };
}

#[cfg(feature = "transport")]
const IDLE: usize = usize::MAX;

/// Without transport there's no `encode` or `decode`, so nothing can build a `Reader` or
/// `Writer` and there's nothing to count for.
#[cfg(not(feature = "transport"))]
#[inline]
fn charge(_size: usize) -> bool {
    true
}

/// Count a value of this many bytes for the encode or decode on this thread. False once it's
/// past the limit. Encode and decode count the same values, so their totals match.
#[cfg(feature = "transport")]
#[inline]
fn charge(size: usize) -> bool {
    // 64 values per byte is all the value budget allows, at 4 bytes that's the 256 anyway.
    if size <= 4 {
        return true;
    }

    LEFT.with(|left| {
        let current = left.get();

        if current == IDLE {
            return true;
        }

        match current.checked_sub(size) {
            Some(rest) => {
                left.set(rest);
                true
            }
            // Nothing got built, so nothing comes off. The flag says why it stopped.
            None => {
                OVER.with(|over| over.set(true));
                false
            }
        }
    })
}

/// A packed struct's fields get copied out before they're encoded, borrowing one in place
/// could point off its alignment. This puts that on the field instead of on generated code.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`{Self}` has to be Copy to sit in a packed struct that derives AktorData",
    label = "copied out of the packed struct before it's encoded",
    note = "a packed field can sit off its alignment, so it can't be borrowed in place. make it Copy or remove the packed representation"
)]
pub trait PackedField: Copy {}
impl<T: Copy> PackedField for T {}

/// A field that can take `#[aktor(max_len = N)]`: anything sent with its length in front. Text
/// counts bytes, the rest counts values.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`max_len` works on `Vec`, `String`, `Box<[T]>`, `Box<str>` and `Cow` fields",
    label = "`{Self}` isn't one of them"
)]
pub trait AktorMaxLen: sealed::Sealed {
    const UNIT: &'static str = "values";

    fn max_len_count(&self) -> usize;
}

// The derive trusts `decode_max` to check the length, so only aktor's own types get to say yes.
mod sealed {
    pub trait Sealed {}
}

#[doc(hidden)]
#[inline]
pub fn write_max<T: AktorData + AktorMaxLen>(
    writer: &mut Writer,
    value: &T,
    max: usize,
) -> Result<(), DataError> {
    within(value.max_len_count(), max, T::UNIT)?;

    writer.write(value)
}

#[doc(hidden)]
#[inline]
pub fn read_max<T: AktorData + AktorMaxLen>(
    reader: &mut Reader<'_>,
    max: usize,
) -> Result<T, DataError> {
    reader.read_with(|reader| T::decode_max(reader, max))
}

#[inline]
fn within(count: usize, max: usize, unit: &'static str) -> Result<(), DataError> {
    if count > max {
        return Err(over_max_len(count, max, unit));
    }

    Ok(())
}

#[cold]
fn over_max_len(count: usize, max: usize, unit: &'static str) -> DataError {
    DataReason::MaxLen { count, max, unit }.into()
}

#[doc(hidden)]
#[inline]
pub fn write_packed<T: AktorData + PackedField>(
    writer: &mut Writer,
    field: T,
) -> Result<(), DataError> {
    writer.write(&field)
}

/// Counts for one encode or decode. A decode inside another decode builds part of the outer
/// value, so it spends from the same budget and its count goes up when it's done. Anything else
/// in the middle, like an encode inside a custom decoder, is its own message and leaves
/// the outer count alone.
#[cfg(feature = "transport")]
struct Counting {
    limit: usize,
    outer: (usize, bool, bool),
    /// A decode inside a decode, it counts toward the outer one.
    nested: bool,
    /// The limit is what the outer one had left, going over it is going over the outer one too.
    shared: bool,
}
#[cfg(feature = "transport")]
impl Counting {
    fn start(own: usize, decoding: bool) -> Self {
        let outer = (
            LEFT.with(Cell::get),
            OVER.with(Cell::get),
            DECODING.with(Cell::get),
        );
        let nested = decoding && outer.0 != IDLE && outer.2;
        let shared = nested && outer.0 <= own;
        let limit = match nested {
            true => outer.0.min(own),
            false => own,
        }
        .min(IDLE - 1);

        LEFT.with(|left| left.set(limit));
        OVER.with(|over| over.set(false));
        DECODING.with(|cell| cell.set(decoding));
        Self {
            limit,
            outer,
            nested,
            shared,
        }
    }

    fn charged(&self) -> usize {
        self.limit - LEFT.with(Cell::get)
    }

    /// Went past the limit at some point, even if an error ended things there.
    fn over(&self) -> bool {
        OVER.with(Cell::get)
    }
}
#[cfg(feature = "transport")]
impl Drop for Counting {
    fn drop(&mut self) {
        let (left, over) = match self.nested {
            true => (
                self.outer.0.saturating_sub(self.charged()),
                self.outer.1 || (self.over() && self.shared),
            ),
            false => (self.outer.0, self.outer.1),
        };

        LEFT.with(|cell| cell.set(left));
        OVER.with(|cell| cell.set(over));
        DECODING.with(|cell| cell.set(self.outer.2));
    }
}
