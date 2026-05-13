use crate::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::Duration;

use ahash::AHashMap;
use compact_str::CompactString;
use fst::Set;
use redb::{Database, ReadTransaction, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use tracing::warn;

/// One executable entry inside a detectable game record.
#[derive(
    Debug,
    Clone,
    Deserialize,
    Serialize,
    PartialEq,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct Executable {
    pub name: CompactString,
    #[serde(default)]
    pub is_launcher: bool,
    /// Optional required command-line arguments.
    #[serde(default)]
    pub arguments: Option<SmallVec<[CompactString; 2]>>,
    #[serde(default)]
    pub os: Option<CompactString>,
}

/// A detectable game/application record.
#[derive(
    Debug, Clone, Deserialize, Serialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub struct DetectableEntry {
    pub id: CompactString,
    pub name: CompactString,
    pub executables: SmallVec<[Executable; 2]>,
}

/// Discord's detectable-applications endpoint.
const DETECTABLE_URL: &str = "https://discord.com/api/v9/applications/detectable";

/// redb table: app_id → rkyv-serialised `DetectableEntry` bytes.
const APPS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("apps");

/// redb table: exe_name → newline-separated list of app IDs.
const EXES_TABLE: TableDefinition<&str, &str> = TableDefinition::new("exes");

// ─── Cache paths ─────────────────────────────────────────────────────────────

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
            let home = std::env::home_dir().unwrap_or_else(|| "/tmp".into());
            home.join(".cache")
        });

    base.join("dirpc")
}

pub(crate) fn cache_db_path() -> PathBuf {
    cache_dir().join("detectable.redb")
}

fn cache_etag_path() -> PathBuf {
    cache_dir().join("detectable.etag")
}

async fn read_etag() -> Option<String> {
    tokio::fs::read_to_string(cache_etag_path()).await.ok()
}

async fn save_etag(etag: &str) {
    let _ = tokio::fs::create_dir_all(cache_dir()).await;
    let _ = tokio::fs::write(cache_etag_path(), etag).await;
}

// ─── Network fetch ───────────────────────────────────────────────────────────

/// Fetch the detectable list from Discord's API, honouring a stored ETag.
///
/// Returns `Ok(None)` when the server replies 304 Not Modified.
/// Returns `Ok(Some((entries, etag)))` on 200 OK.
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

    let entries = resp.json().await?;

    Ok(Some((entries, new_etag)))
}

/// Fetch a fresh detectable entries list from Discord (or return empty on failure).
///
/// Unlike the old `load_detectable`, this does **not** manage the on-disk cache
/// itself – the caller (`DetectableDb`) is responsible for persistence.
pub(crate) async fn load_detectable_entries() -> Vec<DetectableEntry> {
    let etag = read_etag().await;

    match fetch_detectable(etag.as_deref()).await {
        Ok(None) => {
            // 304 – caller will use existing redb data.
            vec![]
        }
        Ok(Some((entries, new_etag))) => {
            if let Some(tag) = new_etag.as_deref() {
                save_etag(tag).await;
            }
            entries
        }
        Err(e) => {
            warn!("Failed to fetch detectable list: {e}");
            vec![]
        }
    }
}

// ─── DetectableDb ────────────────────────────────────────────────────────────

/// Disk-backed KV store (redb/mmap) with a two-level in-memory fast path.
///
/// ## Hot-path hierarchy
///
/// 1. **FST** (`fst::Set`, O(|name|), pure memory) — membership pre-filter.
///    Only exe names that appear in the FST proceed to the next level.
/// 2. **`exe_index`** (`papaya::HashMap`, O(1), pure memory) — maps each exe
///    name to the list of app IDs that declare that executable.  Eliminates the
///    intermediate `EXES_TABLE` redb lookup that was previously needed.
/// 3. **redb apps table** (mmap-backed) — only reached when both (1) and (2)
///    confirm a candidate.  Provides the rkyv-serialised entry for argument
///    validation.
///
/// **Miss path** (the common case in production): FST says "not known" → no
/// allocation, no HashMap lookup, no disk I/O at all.
pub struct DetectableDb {
    db: Database,
    fst: Set<Vec<u8>>,
    /// In-memory exe_name → Vec<app_id>.
    ///
    /// Bypasses the `EXES_TABLE` redb round-trip in the hot scan path.
    /// Populated from `exe_to_ids` during `ingest_entries` and reconstructed
    /// from the `EXES_TABLE` rows during `open`.
    exe_index: HashMap<CompactString, SmallVec<[CompactString; 4]>>,
}

impl DetectableDb {
    /// Open an existing redb database and rebuild the FST and exe_index from
    /// its exe table.
    pub fn open(db_path: &std::path::Path) -> anyhow::Result<Self> {
        let db = Database::open(db_path)?;
        let mut this = Self {
            db,
            fst: empty_fst(),
            exe_index: HashMap::default(),
        };
        this.load_fst_from_db()?;
        Ok(this)
    }

    /// Delete any stale database file, create a fresh one, ingest entries, and
    /// build the FST.  Async because it needs to create the cache directory.
    pub async fn rebuild(
        db_path: &std::path::Path,
        entries: &[DetectableEntry],
    ) -> anyhow::Result<Self> {
        let _ = tokio::fs::create_dir_all(cache_dir()).await;
        let _ = tokio::fs::remove_file(db_path).await;

        let db = Database::create(db_path)?;
        let mut this = Self {
            db,
            fst: empty_fst(),
            exe_index: HashMap::default(),
        };
        this.ingest_entries(entries)?;
        Ok(this)
    }

    /// `true` when no entries have been ingested yet.
    pub fn is_empty(&self) -> bool {
        self.fst.is_empty()
    }

    /// Number of unique executable names known to the FST.
    pub fn fst_len(&self) -> usize {
        self.fst.len()
    }

    // ── internals ─────────────────────────────────────────────────────────────

    /// Write `entries` into both redb tables, populate the in-memory
    /// `exe_index`, and rebuild the in-memory FST.
    fn ingest_entries(&mut self, entries: &[DetectableEntry]) -> anyhow::Result<()> {
        // Build exe_name → Vec<app_id> with a plain AHashMap (single-threaded).
        let mut exe_to_ids: AHashMap<CompactString, SmallVec<[CompactString; 4]>> =
            AHashMap::default();

        let write_txn = self.db.begin_write()?;
        {
            let mut apps = write_txn.open_table(APPS_TABLE)?;
            let mut exes = write_txn.open_table(EXES_TABLE)?;

            for entry in entries {
                // Serialise with rkyv for zero-copy-friendly binary storage.
                let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(entry)
                    .map_err(|e| anyhow::anyhow!("rkyv serialise: {e}"))?;
                apps.insert(entry.id.as_str(), bytes.as_slice())?;

                for exe in &entry.executables {
                    exe_to_ids
                        .entry(exe.name.clone())
                        .or_default()
                        .push(entry.id.clone());
                }
            }

            for (exe_name, ids) in &exe_to_ids {
                let joined = ids
                    .iter()
                    .map(CompactString::as_str)
                    .collect::<Vec<_>>()
                    .join("\n");
                exes.insert(exe_name.as_str(), joined.as_str())?;
            }
        }
        write_txn.commit()?;

        // Populate the in-memory exe_index (bypasses EXES_TABLE in the hot path).
        {
            let pin = self.exe_index.pin();
            for (exe_name, ids) in &exe_to_ids {
                pin.insert(exe_name.clone(), ids.clone());
            }
        }

        // Build FST – keys must be inserted in sorted (lexicographic) order.
        let mut sorted_names: Vec<CompactString> = exe_to_ids.keys().cloned().collect();
        sorted_names.sort_unstable();

        let mut builder = fst::SetBuilder::memory();
        for name in sorted_names {
            builder.insert(name.as_bytes())?;
        }
        self.fst = builder.into_set();

        Ok(())
    }

    /// Reconstruct the in-memory FST and exe_index from the keys already stored
    /// in the exe table (used when opening an existing database).
    fn load_fst_from_db(&mut self) -> anyhow::Result<()> {
        let read_txn: ReadTransaction = self.db.begin_read()?;

        // The table might not exist in a freshly created (but empty) database.
        let exes: redb::ReadOnlyTable<&str, &str> = match read_txn.open_table(EXES_TABLE) {
            Ok(t) => t,
            Err(_) => return Ok(()),
        };

        let mut names: Vec<CompactString> = Vec::new();
        {
            let pin = self.exe_index.pin();
            for (k, v) in exes.iter()?.flatten() {
                let exe_name = CompactString::from(k.value());
                let ids: SmallVec<[CompactString; 4]> =
                    v.value().split('\n').map(CompactString::from).collect();
                pin.insert(exe_name.clone(), ids);
                names.push(exe_name);
            }
        }
        names.sort_unstable();

        let mut builder = fst::SetBuilder::memory();
        for name in &names {
            builder.insert(name.as_bytes())?;
        }
        self.fst = builder.into_set();

        Ok(())
    }

    // ── public hot-path ───────────────────────────────────────────────────────

    /// Return `(id, name)` of the first detectable entry that matches `path`
    /// and `args`, or `None` if no match is found.
    ///
    /// ## Hot-path hierarchy
    ///
    /// 1. **FST** — O(|name|) membership check, pure memory.  
    ///    Miss path: returns `None` immediately with no allocation or disk I/O.
    /// 2. **`exe_index`** — O(1) `papaya::HashMap` lookup, pure memory.  
    ///    Yields the candidate app IDs without touching redb at all.
    /// 3. **redb `apps` table** — mmap-backed, only reached for confirmed hits.  
    ///    Provides the rkyv-serialised entry for argument validation.
    pub fn match_process(
        &self,
        path: &str,
        args: &[&str],
    ) -> Option<(CompactString, CompactString)> {
        let variants = path_variants(path);
        let filename = path_filename(path);

        // Build the set of FST look-up candidates:
        //   • regular path variants  (e.g. "csgo", "game/csgo")
        //   • exact-filename variant  (e.g. ">csgo")
        let mut candidates: SmallVec<[CompactString; 8]> = variants.clone();
        if !filename.is_empty() {
            candidates.push(CompactString::from(format!(">{filename}")));
        }

        // ── FST pre-filter (in-memory, O(|name|) per candidate) ──────────────
        let hit_names: SmallVec<[&str; 8]> = candidates
            .iter()
            .filter(|c| self.fst.contains(c.as_bytes()))
            .map(CompactString::as_str)
            .collect();

        if hit_names.is_empty() {
            return None;
        }

        // ── exe_index lookup (in-memory papaya::HashMap, no disk I/O) ────────
        let seen: HashSet<CompactString> = HashSet::default();
        let mut app_ids: SmallVec<[CompactString; 8]> = SmallVec::new();
        {
            let pin = self.exe_index.pin();
            for exe_name in &hit_names {
                if let Some(ids) = pin.get(*exe_name) {
                    for id in ids {
                        if seen.pin().insert(id.clone()) {
                            app_ids.push(id.clone());
                        }
                    }
                }
            }
        }

        if app_ids.is_empty() {
            return None;
        }

        // ── redb APPS_TABLE lookup (mmap-backed, only on confirmed hits) ──────
        let read_txn: ReadTransaction = self.db.begin_read().ok()?;
        let apps: redb::ReadOnlyTable<&str, &[u8]> = read_txn.open_table(APPS_TABLE).ok()?;

        // Verify each candidate with the full match logic.
        for app_id in &app_ids {
            if let Ok(Some(guard)) = apps.get(app_id.as_str()) {
                let bytes: &[u8] = guard.value();

                // Copy into a 16-byte-aligned buffer so rkyv can access the
                // archive safely (redb mmap pages may not satisfy the archived
                // root's alignment requirement).
                let mut aligned = rkyv::util::AlignedVec::<RKYV_ALIGNMENT>::new();
                aligned.extend_from_slice(bytes);

                if let Ok(archived) =
                    rkyv::access::<ArchivedDetectableEntry, rkyv::rancor::Error>(&aligned)
                    && archived_match(archived, path, args)
                {
                    return Some((
                        CompactString::from(archived.id.as_str()),
                        CompactString::from(archived.name.as_str()),
                    ));
                }
            }
        }

        None
    }
}

/// Alignment required for the rkyv archived root.
///
/// rkyv's `access` function requires the buffer to be aligned to at least the
/// archived struct's alignment.  16 bytes covers all primitive types (including
/// potential future SIMD fields) without waste.
const RKYV_ALIGNMENT: usize = 16;

fn empty_fst() -> Set<Vec<u8>> {
    fst::SetBuilder::memory().into_set()
}

// ─── Helper: zero-copy match against an archived entry ───────────────────────

fn archived_match(archived: &ArchivedDetectableEntry, path: &str, args: &[&str]) -> bool {
    let variants = path_variants(path);
    let filename = path_filename(path);

    for exe in archived.executables.iter() {
        let exe_name: &str = &exe.name;

        let matched = if exe_name.starts_with('>') {
            &exe_name[1..] == filename
        } else {
            variants.iter().any(|v| v == exe_name)
        };

        if !matched {
            continue;
        }

        // Check required arguments if specified.
        let args_ok = match &exe.arguments {
            rkyv::option::ArchivedOption::None => true,
            rkyv::option::ArchivedOption::Some(required) => required
                .iter()
                .all(|ra| args.iter().any(|a| *a == ra.as_str())),
        };

        if args_ok {
            return true;
        }
    }

    false
}

/// Generate candidate comparison strings from a process path.
///
/// Produces up to 4 trailing path components joined with `/`, plus de-64-bit-ified
/// variants of each, to match entries like `csgo`, `game/csgo`, `hl2/game/csgo`, …
pub fn path_variants(path: &str) -> SmallVec<[CompactString; 8]> {
    // Support both Unix `/` and Windows `\` separators.
    let parts: SmallVec<[&str; 16]> = path.split(['/', '\\']).filter(|s| !s.is_empty()).collect();
    let mut variants: SmallVec<[CompactString; 8]> = SmallVec::new();

    let start = if parts.len() > 4 { parts.len() - 4 } else { 0 };
    for i in start..parts.len() {
        let suffix = parts[i..].join("/");
        let cleaned = strip_64_suffix(&suffix);
        variants.push(CompactString::from(suffix.as_str()));
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
pub fn strip_64_suffix(name: &str) -> CompactString {
    // Must be checked before the shorter patterns they contain.
    for suffix in [".x64", "_64", "x64", "64"] {
        if let Some(stripped) = name.strip_suffix(suffix) {
            return CompactString::from(stripped);
        }
    }
    CompactString::from(name)
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
                exe.name
                    .strip_prefix('>')
                    .is_some_and(|exact| exact == filename)
            } else {
                variants.iter().any(|v| v.as_str() == exe.name.as_str())
            };

            if !matched {
                continue;
            }

            // Check required arguments if specified.
            if exe.arguments.as_ref().is_some_and(|required_args| {
                !required_args
                    .iter()
                    .all(|ra| args.iter().any(|a| *a == ra.as_str()))
            }) {
                continue;
            }

            return Some(entry);
        }
    }

    None
}
