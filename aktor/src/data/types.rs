use alloc::{borrow::Cow, boxed::Box, vec::Vec};
use er::*;

/// The wire representation of a worker argument or result.
///
/// `#[derive(AktorData)]` writes the Rust fields and variants in order, nothing else.
/// `#[aktor(skip)]` omits a field and restores `Default::default()` when decoding it.
/// `#[aktor(max_len = N)]` on a `Vec`, `String` or boxed slice refuses anything longer, and the
/// receiver says no before it builds the field, so a size cap never needs a hand-written impl.
/// Packed structs copy each encoded field before borrowing it, so those fields must be `Copy`.
///
/// Writing one by hand is just calling [`Writer::write`] and [`Reader::read`] on the parts
/// in the same order. Those two keep the limits, so go through them for anything nested.
#[diagnostic::on_unimplemented(
    note = "Derive AktorData for the arguments and result of a worker call."
)]
pub trait AktorData: Sized {
    /// Memory a value takes beyond what its own bytes pay for, counted against the message's
    /// size. Small types that always take at least a byte, like integers and strings, set 0.
    #[doc(hidden)]
    const MEMORY: usize = core::mem::size_of::<Self>();

    fn encode_data(&self, writer: &mut Writer) -> Result<(), DataError>;

    fn decode_data(reader: &mut Reader<'_>) -> Result<Self, DataError>;

    #[doc(hidden)]
    fn encode_slice(values: &[Self], writer: &mut Writer) -> Result<(), DataError> {
        super::impls::encode_slice(values, writer)
    }

    /// `decode_data` for a `#[aktor(max_len = N)]` field. Only the length-prefixed types look at
    /// `max`, and the derive only calls it on those (see `AktorMaxLen`).
    #[doc(hidden)]
    fn decode_max(reader: &mut Reader<'_>, max: usize) -> Result<Self, DataError> {
        let _ = max;
        Self::decode_data(reader)
    }

    /// Refuses more than `max` values before building any of them, that's what
    /// `#[aktor(max_len)]` checks with. Everything else passes `usize::MAX`.
    #[doc(hidden)]
    fn decode_vec(reader: &mut Reader<'_>, max: usize) -> Result<Vec<Self>, DataError> {
        super::impls::decode_vec(reader, max)
    }

    #[doc(hidden)]
    fn encode_array<const N: usize>(
        values: &[Self; N],
        writer: &mut Writer,
    ) -> Result<(), DataError> {
        values.iter().try_for_each(|value| writer.write(value))
    }

    #[doc(hidden)]
    fn decode_array<const N: usize>(reader: &mut Reader<'_>) -> Result<[Self; N], DataError> {
        super::impls::decode_array(reader)
    }
}

/// The bytes of one message on their way out. The counters keep the encoder as strict as the
/// decoder will be, so the sender hears about a message that would be refused.
pub struct Writer {
    pub(super) bytes: Vec<u8>,
    pub(super) depth: usize,
    pub(super) values: usize,
}

pub struct Reader<'a> {
    pub(crate) memory_limit: usize,
    pub(super) rest: &'a [u8],
    /// The whole message, the value budget follows how much of it has been read.
    pub(super) total: usize,
    pub(super) depth: usize,
    pub(super) values: usize,
}

/// Why a value couldn't be written or read. One pointer wide, so a `Result<u64, DataError>`
/// comes back in registers. The reads on the hot path return a lot of those.
#[derive(Clone, PartialEq, Eq, ErFormat)]
#[er(no_constructors, format = "{reason}")]
pub struct DataError {
    pub reason: Box<DataReason>,
}

#[derive(Clone, PartialEq, Eq, Er)]
pub enum DataReason {
    #[er(format = "{count} {unit} where max_len is {max}")]
    MaxLen {
        count: usize,
        max: usize,
        unit: &'static str,
    },
    #[er(format = "worker data is nested too deeply (limit {0})")]
    Depth(usize),
    #[er(format = "worker data has too many values (limit {0})")]
    Values(usize),
    #[er(format = "worker data needs too much memory for its size (limit {0} bytes)")]
    Memory(usize),
    #[er(format = "worker data has {0} trailing bytes")]
    Trailing(usize),
    #[er(format = "incomplete fixed-width value")]
    IncompleteFixed,
    #[er(format = "missing boolean end bit")]
    MissingEndBit,
    #[er(format = "boolean count overflow")]
    CountOverflow,
    #[er(format = "no memory for this many boolean values")]
    BooleanAllocation,
    #[er(format = "nonzero padding in boolean data")]
    NonzeroPadding,
    #[er(format = "Infallible has no variants")]
    Infallible,
    #[er(format = "invalid array length")]
    ArrayLength,
    #[er(format = "invalid character {0}")]
    Character(u64),
    #[er(format = "invalid boolean {0}")]
    Boolean(u8),
    #[er(format = "invalid option tag {0}")]
    OptionTag(u8),
    #[er(format = "invalid result tag {0}")]
    ResultTag(u8),
    #[er(format = "varint is too long")]
    Varint,
    #[er(format = "worker data ended early")]
    Truncated,

    #[er(format = "{0}")]
    Message(Cow<'static, str>),
    #[er(format = "empty AktorData enum")]
    EmptyEnum,
    #[er(format = "invalid AktorData variant {0}")]
    Variant(usize),
    #[er(format = "zero failure ID")]
    ZeroFailureId,
    #[er(format = "integer out of range")]
    Integer(#[er(source)] core::num::TryFromIntError),
    #[er(format = "invalid UTF-8")]
    Utf8(#[er(source)] core::str::Utf8Error),
}
