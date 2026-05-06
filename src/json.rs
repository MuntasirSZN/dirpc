//! JSON helpers.
//!
//! On **little-endian** platforms (x86-64, ARM, RISC-V, …) [`simd-json`] is
//! used for deserialization: it processes bytes in SIMD lanes and can be
//! significantly faster than serde_json for large payloads.
//!
//! On **big-endian** platforms (s390x, powerpc64be, …) `simd-json` does not
//! yet support SIMD acceleration so we fall back to `serde_json` instead.
//! See <https://github.com/simd-lite/simd-json/issues/437>.

pub use serde_json::{Map, Value, from_str, json};

#[cfg(target_endian = "little")]
pub use simd_json::serde::{from_slice, to_string, to_vec};

#[cfg(target_endian = "big")]
pub use serde_json::{from_slice, to_string, to_vec};
