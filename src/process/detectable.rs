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

/// Discord's detectable-applications endpoint.
const DETECTABLE_URL: &str = "https://discord.com/api/v9/applications/detectable";

/// Platform-specific cache directory for dirpc.
fn cache_dir() -> PathBuf {
    #[cfg(windows)]
    let base = std::env::var("LOCALAPPDATA")
        .or_else(|_| std::env::var("APPDATA"))
        .map(PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir());

    #[cfg(not(windows))]
    let base = std::env::var("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            let home = std::env::home_dir()
                .unwrap_or_else(|| "/tmp".into());
            home.join(".cache")
        });

    base.join("dirpc")
}

fn cache_json_path() -> PathBuf {
    cache_dir().join("detectable.json")
}
fn cache_etag_path() -> PathBuf {
    cache_dir().join("detectable.etag")
}

/// Read the stored ETag from disk, if any.
async fn read_etag() -> Option<String> {
    tokio::fs::read_to_string(cache_etag_path()).await.ok()
}

/// Fetch the detectable list from Discord's API, honouring a stored ETag.
///
/// Returns `Ok(None)` when the server replies 304 Not Modified (cache still fresh).
/// Returns `Ok(Some((entries, etag)))` on a successful 200 response.
async fn fetch_detectable(
    etag: Option<&str>,
) -> anyhow::Result<Option<(Vec<DetectableEntry>, Option<String>)>> {
    let mut req = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .user_agent(concat!(clap::crate_name!(), "/", clap::crate_version!()))
        .build()?
        .get(DETECTABLE_URL);

    if let Some(tag) = etag {
        req = req.header(reqwest::header::IF_NONE_MATCH, tag);
    }

    let resp = req.send().await?;

    if resp.status() == reqwest::StatusCode::NOT_MODIFIED {
        return Ok(None);
    }

    let new_etag = resp
        .headers()
        .get(reqwest::header::ETAG)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);

    let mut bytes = resp.bytes().await?.to_vec();
    let entries: Vec<DetectableEntry> = crate::json::from_slice(&mut bytes)?;

    Ok(Some((entries, new_etag)))
}

/// Load cached detectable entries from disk. Returns `None` if absent or corrupt.
async fn load_cache() -> Option<Vec<DetectableEntry>> {
    let mut data = tokio::fs::read(cache_json_path()).await.ok()?;
    crate::json::from_slice::<Vec<DetectableEntry>>(&mut data).ok()
}

/// Persist entries and ETag to disk (best-effort).
async fn save_cache(entries: &[DetectableEntry], etag: Option<&str>) {
    let dir = cache_dir();
    let _ = tokio::fs::create_dir_all(&dir).await;
    if let Ok(bytes) = crate::json::to_vec(entries) {
        let _ = tokio::fs::write(cache_json_path(), bytes).await;
    }
    if let Some(tag) = etag {
        let _ = tokio::fs::write(cache_etag_path(), tag).await;
    }
}

/// Load the detectable apps list at runtime.
///
/// Strategy:
/// 1. Send a request to Discord's API with `If-None-Match: <stored-etag>`.
///    - **304 Not Modified** → cache is still current; deserialise and return it.
///    - **200 OK** → update the on-disk cache + ETag file, return new data.
/// 2. On any network or parse failure → fall back to the on-disk cache with a warning.
/// 3. No network **and** no cache → return an empty vec with a warning.
pub async fn load_detectable() -> Vec<DetectableEntry> {
    let etag = read_etag().await;

    match fetch_detectable(etag.as_deref()).await {
        // Server says cache is still current.
        Ok(None) => {
            if let Some(entries) = load_cache().await {
                return entries;
            }
            warn!("ETag indicated cache is fresh but cache file is missing; returning empty list");
            vec![]
        }
        // Fresh data from server.
        Ok(Some((entries, new_etag))) => {
            save_cache(&entries, new_etag.as_deref()).await;
            entries
        }
        // Network or parse error.
        Err(e) => {
            warn!("Failed to fetch detectable list: {e}");
            if let Some(entries) = load_cache().await {
                warn!("Using stale detectable cache");
                return entries;
            }
            warn!("No cached detectable data available; game detection will be unavailable");
            vec![]
        }
    }
}

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
            if exe
                .arguments
                .as_ref()
                .is_some_and(|required_args| !required_args.iter().all(|ra| args.iter().any(|a| a == ra)))
            {
                continue;
            }

            return Some(entry);
        }
    }

    None
}
