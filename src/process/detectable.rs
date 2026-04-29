use serde::{Deserialize, Serialize};

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

/// The embedded game database shipped with the binary.
pub const DETECTABLE_JSON: &str = include_str!("detectable.json");

/// Parse the embedded `detectable.json` into a `Vec<DetectableEntry>`.
pub fn load_detectable() -> Vec<DetectableEntry> {
    serde_json::from_str(DETECTABLE_JSON).unwrap_or_default()
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
