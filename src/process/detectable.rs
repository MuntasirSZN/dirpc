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
    let parts: Vec<&str> = path
        .split(|c| c == '/' || c == '\\')
        .filter(|s| !s.is_empty())
        .collect();
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

/// Remove common 64-bit marker substrings from a name.
pub fn strip_64_suffix(name: &str) -> String {
    name.replace(".x64", "")
        .replace("_64", "")
        .replace("x64", "")
        .replace("64", "")
}

// ── Matching ─────────────────────────────────────────────────────────────────

/// Return the first `DetectableEntry` whose executable list matches `path` / `args`.
pub fn match_process<'a>(
    path: &str,
    args: &[&str],
    entries: &'a [DetectableEntry],
) -> Option<&'a DetectableEntry> {
    let variants = path_variants(path);
    let filename = path
        .split(|c| c == '/' || c == '\\')
        .filter(|s| !s.is_empty())
        .next_back()
        .unwrap_or(path);

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
                if !required_args
                    .iter()
                    .all(|ra| args.iter().any(|a| a == ra))
                {
                    continue;
                }
            }

            return Some(entry);
        }
    }

    None
}
