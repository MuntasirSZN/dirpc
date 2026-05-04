//! JSON helpers.
//!
//! On **little-endian** platforms (x86-64, ARM, RISC-V, …) [`simd-json`] is
//! used for deserialization: it processes bytes in SIMD lanes and can be
//! significantly faster than serde_json for large payloads.
//!
//! On **big-endian** platforms (s390x, powerpc64be, …) `simd-json` does not
//! yet support SIMD acceleration so we fall back to `serde_json` instead.
//! See <https://github.com/simd-lite/simd-json/issues/437>.
//!
//! **Serialization always uses `serde_json`** – `simd-json` does not provide a
//! serializer.

pub use serde_json::{Map, Value, json, to_string};

use serde::de::DeserializeOwned;

/// Deserialize from a mutable byte slice.
///
/// On little-endian: uses `simd-json`, which modifies the slice in-place.
/// On big-endian: delegates to `serde_json::from_slice`.
pub fn from_slice<T: DeserializeOwned>(buf: &mut [u8]) -> anyhow::Result<T> {
    #[cfg(target_endian = "little")]
    {
        simd_json::from_slice(buf).map_err(|e| anyhow::anyhow!("JSON parse error: {e}"))
    }
    #[cfg(target_endian = "big")]
    {
        serde_json::from_slice(buf).map_err(Into::into)
    }
}

/// Deserialize from a string slice.
///
/// On little-endian: allocates a temporary mutable buffer so that `simd-json`
/// can operate in-place. On big-endian: delegates to `serde_json::from_str`.
pub fn from_str<T: DeserializeOwned>(s: &str) -> anyhow::Result<T> {
    #[cfg(target_endian = "little")]
    {
        let mut buf = s.as_bytes().to_vec();
        simd_json::from_slice(&mut buf).map_err(|e| anyhow::anyhow!("JSON parse error: {e}"))
    }
    #[cfg(target_endian = "big")]
    {
        serde_json::from_str(s).map_err(Into::into)
    }
}
