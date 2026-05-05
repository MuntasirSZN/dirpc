use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tracing::warn;

/// One executable entry inside a detectable game record.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct Executable {
    pub name: String,
    #[serde(default)]
    pub is_launcher: bool,
    /// Optional required command-line arguments.
    #[serde(default)]
    pub arguments: Option<Vec<String>>,
    #[serde(default)]
    pub os: Option<String>,
}

/// A detectable game/application record.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DetectableEntry {
    pub id: String,
    pub name: String,
    pub executables: Vec<Executable>,
}

// ── Remote fetch + on-disk cache ─────────────────────────────────────────────

/// Discord's detectable-applications endpoint.
const DETECTABLE_URL: &str = "https://discord.com/api/v9/applications/detectable";
/// Re-fetch the list if the cached file is older than this.
const CACHE_TTL: Duration = Duration::from_secs(7 * 24 * 60 * 60); // 1 week

/// Platform-specific path for the on-disk detectable cache.
fn cache_path() -> PathBuf {
    #[cfg(windows)]
    let base = std::env::var("LOCALAPPDATA")
        .or_else(|_| std::env::var("APPDATA"))
        .map(PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir());

    #[cfg(not(windows))]
    let base = std::env::var("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            let home = std::env::home_dir().unwrap_or_else(|| "/tmp".to_string().into());
            PathBuf::from(home).join(".cache")
        });

    base.join("dirpc").join("detectable.json")
}

/// Return `true` when the cache file exists and was written within [`CACHE_TTL`].
async fn cache_is_fresh(path: &PathBuf) -> bool {
    let meta = match tokio::fs::metadata(path).await {
        Ok(m) => m,
        Err(_) => return false,
    };
    let modified = match meta.modified() {
        Ok(t) => t,
        Err(_) => return false,
    };

    let Ok(modified_ts) = jiff::Timestamp::try_from(modified) else {
        return false;
    };

    let Ok(ttl) = jiff::SignedDuration::try_from(CACHE_TTL) else {
        return false;
    };

    jiff::Timestamp::now().duration_since(modified_ts) < ttl
}

/// Fetch the detectable list from Discord's API.
async fn fetch_detectable() -> anyhow::Result<Vec<DetectableEntry>> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .user_agent(concat!(clap::crate_name!(), "/", clap::crate_version!()))
        .build()?;
    let bytes = client.get(DETECTABLE_URL).send().await?.bytes().await?;
    let mut data = bytes.to_vec();

    let entries: Vec<DetectableEntry> = crate::json::from_slice(&mut data)?;

    Ok(entries)
}

/// Load the detectable apps list.
///
/// Strategy (in order):
/// 1. Fresh on-disk cache → deserialise and return.
/// 2. Network fetch → persist to cache, return.
/// 3. Stale on-disk cache → return stale data with a warning.
/// 4. Embedded fallback (compile-time snapshot).
pub async fn load_detectable() -> Vec<DetectableEntry> {
    let path = cache_path();

    // 1. Fresh cache
    if cache_is_fresh(&path).await {
        if let Ok(data) = tokio::fs::read(&path).await {
            if let Ok(entries) = crate::json::from_slice::<Vec<DetectableEntry>>(&mut data.clone())
            {
                return entries;
            }
        }
    }

    // 2. Network fetch
    match fetch_detectable().await {
        Ok(entries) => {
            // Persist to disk (best-effort).
            if let Some(parent) = path.parent() {
                let _ = tokio::fs::create_dir_all(parent).await;
            }
            if let Ok(serialised) = crate::json::to_vec(&entries) {
                let _ = tokio::fs::write(&path, serialised).await;
            }
            return entries;
        }
        Err(e) => warn!("Failed to fetch detectable list: {e}"),
    }

    // 3. Stale cache
    if let Ok(data) = tokio::fs::read(&path).await {
        if let Ok(entries) = crate::json::from_slice::<Vec<DetectableEntry>>(&mut data.clone()) {
            warn!("Using stale detectable cache");
            return entries;
        }
    }
}

// ── Path-variant helpers ──────────────────────────────────────────────────────

/// Generate candidate comparison strings from a process path.
///
/// Produces up to 4 trailing path components joined with `/`, plus de-64-bit-ified
/// variants of each, to match entries like `csgo`, `game/csgo`, `hl2/game/csgo`, …
pub fn path_variants(path: &str) -> Vec<String> {
    // Support both Unix `/` and Windows `\` separators.
    let parts: Vec<&str> = path.split(['/', '\\']).filter(|s| !s.is_empty()).collect();
    let mut variants: Vec<String> = Vec::new();

    let start = if parts.len() > 4 { parts.len() - 4 } else { 0 };
    for i in start..parts.len() {
        let suffix = parts[i..].join("/");
        let cleaned = strip_64_suffix(&suffix);
        variants.push(suffix.clone());
        if cleaned != suffix {
            variants.push(cleaned);
        }
    }

    variants
}

/// Remove common 64-bit marker suffixes from a name.
///
/// Checks only at the end of the string so names like "base64encoder" are
/// left intact. Ordered from most-specific to least-specific to avoid
/// partial overwrites.
pub fn strip_64_suffix(name: &str) -> String {
    // Must be checked before the shorter patterns they contain.
    for suffix in [".x64", "_64", "x64", "64"] {
        if let Some(stripped) = name.strip_suffix(suffix) {
            return stripped.to_string();
        }
    }
    name.to_string()
}

/// Extract the last path component from a Unix or Windows path.
///
/// Returns an empty string for paths that consist entirely of separators,
/// and the full path unchanged when no separator is present.
pub fn path_filename(path: &str) -> &str {
    path.split(['/', '\\'])
        .rfind(|s| !s.is_empty())
        .unwrap_or("")
}

// ── Matching ─────────────────────────────────────────────────────────────────

/// Return the first `DetectableEntry` whose executable list matches `path` / `args`.
pub fn match_process<'a>(
    path: &str,
    args: &[&str],
    entries: &'a [DetectableEntry],
) -> Option<&'a DetectableEntry> {
    let variants = path_variants(path);
    let filename = path_filename(path);

    for entry in entries {
        for exe in &entry.executables {
            let matched = if exe.name.starts_with('>') {
                // Exact filename match only.
                &exe.name[1..] == filename
            } else {
                variants.iter().any(|v| v == &exe.name)
            };

            if !matched {
                continue;
            }

            // Check required arguments if specified.
            if let Some(required_args) = &exe.arguments {
                if !required_args.iter().all(|ra| args.iter().any(|a| a == ra)) {
                    continue;
                }
            }

            return Some(entry);
        }
    }

    None
}
