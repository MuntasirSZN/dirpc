use crate::HashMap;
use std::path::PathBuf;
use std::time::Duration;
use std::{collections::BTreeSet, fmt::Write as _};

use ahash::AHashMap;
use compact_str::CompactString;
use fst::Set;
use memmap2::Mmap;
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
const DETECTABLE_URL: &str = "https://discord.com/api/v10/applications/detectable";

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

/// Default path to the on-disk FST that indexes the ~700k detectable
/// process strings.  Production callers should use the parameterised
/// `DetectableDb::open_with_fst` / `rebuild_with_fst` so the FST lives next
/// to the redb file under caller-controlled paths.
pub(crate) fn cache_fst_path() -> PathBuf {
    cache_dir().join("detectable.fst")
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
    let status = resp.status();

    if status == reqwest::StatusCode::NOT_MODIFIED {
        return Ok(None);
    }

    let new_etag = resp
        .headers()
        .get(reqwest::header::ETAG)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);

    let body = resp.bytes().await?;
    if !status.is_success() {
        let body_preview = String::from_utf8_lossy(&body);
        anyhow::bail!(
            "detectable API request failed with status {status}: {}",
            compact_whitespace_preview(&body_preview, 200)
        );
    }

    let entries = parse_detectable_entries(&body)?;

    Ok(Some((entries, new_etag)))
}

fn parse_detectable_entries(body: &[u8]) -> anyhow::Result<Vec<DetectableEntry>> {
    if let Ok(entries) = serde_json::from_slice::<Vec<DetectableEntry>>(body) {
        return Ok(entries);
    }

    #[derive(Deserialize)]
    struct WrappedApplications {
        applications: Vec<DetectableEntry>,
    }

    if let Ok(wrapped) = serde_json::from_slice::<WrappedApplications>(body) {
        return Ok(wrapped.applications);
    }

    if let Ok(value) = serde_json::from_slice::<serde_json::Value>(body)
        && let Some(entries) = parse_entries_from_value(&value)
    {
        return Ok(entries);
    }

    anyhow::bail!(
        "unexpected detectable API payload shape ({})",
        describe_payload_shape(body)
    )
}

fn parse_entries_from_value(value: &serde_json::Value) -> Option<Vec<DetectableEntry>> {
    match value {
        serde_json::Value::Array(items) => parse_entries_from_array(items),
        serde_json::Value::Object(map) => {
            for key in ["applications", "data", "results"] {
                if let Some(child) = map.get(key)
                    && let Some(entries) = parse_entries_from_value(child)
                {
                    return Some(entries);
                }
            }

            for child in map.values() {
                if let Some(entries) = parse_entries_from_value(child) {
                    return Some(entries);
                }
            }

            None
        }
        _ => None,
    }
}

fn parse_entries_from_array(items: &[serde_json::Value]) -> Option<Vec<DetectableEntry>> {
    let entries: Vec<DetectableEntry> = items
        .iter()
        .filter_map(|item| DetectableEntry::deserialize(item).ok())
        .collect();

    if items.is_empty() || !entries.is_empty() {
        Some(entries)
    } else {
        None
    }
}

fn describe_payload_shape(body: &[u8]) -> String {
    match serde_json::from_slice::<serde_json::Value>(body) {
        Ok(serde_json::Value::Array(values)) => {
            let mut msg = format!("top-level array(len={})", values.len());
            if let Some(first) = values.first() {
                let _ = write!(msg, ", first={}", describe_json_value(first));
            }
            msg
        }
        Ok(serde_json::Value::Object(map)) => {
            let keys: BTreeSet<_> = map.keys().map(String::as_str).collect();
            let mut msg = format!("top-level object(keys={:?})", keys);
            for key in ["applications", "data", "results"] {
                if let Some(child) = map.get(key) {
                    let _ = write!(msg, ", {key}={}", describe_json_value(child));
                }
            }
            msg
        }
        Ok(other) => format!("top-level {}", describe_json_value(&other)),
        Err(_) => {
            let preview = String::from_utf8_lossy(body).to_string();
            format!(
                "non-JSON response preview={:?}",
                compact_whitespace_preview(&preview, 200)
            )
        }
    }
}

fn describe_json_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "null".to_owned(),
        serde_json::Value::Bool(_) => "boolean".to_owned(),
        serde_json::Value::Number(_) => "number".to_owned(),
        serde_json::Value::String(_) => "string".to_owned(),
        serde_json::Value::Array(items) => format!("array(len={})", items.len()),
        serde_json::Value::Object(map) => {
            let keys: BTreeSet<_> = map.keys().map(String::as_str).collect();
            format!("object(keys={keys:?})")
        }
    }
}

fn compact_whitespace_preview(input: &str, max_chars: usize) -> String {
    if max_chars == 0 {
        return String::new();
    }

    let mut out = String::new();
    let mut chars_used = 0;

    for token in input.split_whitespace() {
        if !out.is_empty() {
            if chars_used >= max_chars {
                break;
            }
            out.push(' ');
            chars_used += 1;
        }

        for ch in token.chars() {
            if chars_used >= max_chars {
                break;
            }
            out.push(ch);
            chars_used += 1;
        }
    }

    out
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

/// Disk-backed KV store (redb) with a two-level mmap'd fast path.
///
/// ## Hot-path hierarchy
///
/// 1. **FST** (`fst::Set` over a `memmap2::Mmap` of a caller-supplied `.fst`
///    file, O(|name|)) — membership pre-filter.  The ~700k process strings
///    are kept on disk and searched in place via the page cache; no per-process
///    heap copy and no `SetBuilder` rebuild on startup.
/// 2. **`exe_index`** (`papaya::HashMap`, O(1), pure memory) — maps each exe
///    name to the list of app IDs that declare that executable.  Eliminates the
///    intermediate `EXES_TABLE` redb lookup that was previously needed.
/// 3. **redb `apps` table** — only reached when both (1) and (2) confirm a
///    candidate.  Provides the rkyv-serialised entry for argument validation.
///
/// **Miss path** (the common case in production): FST says "not known" → no
/// allocation, no HashMap lookup, no disk I/O at all.
pub struct DetectableDb {
    db: Database,
    /// mmap-backed FST index.  The field is *always* populated: an empty FST
    /// is a 36-byte file that parses as a `Set` with zero keys.  `contains`
    /// borrows the FST bytes via the `Set`'s `Deref` and triggers page faults
    /// lazily as pages are touched.
    ///
    /// Concurrency: the underlying file is treated as immutable for the
    /// lifetime of this handle.  `rebuild` always replaces the file via
    /// `rename(2)`, so any in-flight reader keeps observing the old inode
    /// (a valid FST) until its mmap drops.  We do **not** hold a separate
    /// `File` handle — the `Mmap` is independent of the `File` per
    /// memmap2's contract.
    fst: Set<Mmap>,
    fst_path: PathBuf,
    /// In-memory exe_name → Vec<app_id>.
    ///
    /// Bypasses the `EXES_TABLE` redb round-trip in the hot scan path.
    /// Populated from `exe_to_ids` during `ingest_entries` and reconstructed
    /// from the `EXES_TABLE` rows during `open`.
    exe_index: HashMap<CompactString, SmallVec<[CompactString; 4]>>,
}

impl DetectableDb {
    /// Open an existing redb database and load the mmap'd FST at the default
    /// cache location.
    pub fn open(db_path: &std::path::Path) -> anyhow::Result<Self> {
        Self::open_with_fst(db_path, &cache_fst_path())
    }

    /// Open an existing redb database and load the mmap'd FST at `fst_path`.
    ///
    /// The FST file must be a valid FST written by `rebuild_with_fst`
    /// (or any tool that produces a v3-format FST).  If the file is missing,
    /// empty, or corrupt, an empty FST is materialised at `fst_path` and
    /// used instead — except when the `EXES_TABLE` has rows: in that case
    /// the FST is rebuilt from those rows and written back atomically.
    pub fn open_with_fst(
        db_path: &std::path::Path,
        fst_path: &std::path::Path,
    ) -> anyhow::Result<Self> {
        let db = Database::open(db_path)?;
        let mut this = Self {
            db,
            fst: empty_mmap_fst(),
            fst_path: fst_path.to_path_buf(),
            exe_index: HashMap::default(),
        };
        this.load_fst_from_db()?;
        Ok(this)
    }

    /// Delete any stale database file, create a fresh one, ingest entries,
    /// and (re)build the FST at the default cache location.
    /// Async because it needs to create the cache directory.
    pub async fn rebuild(
        db_path: &std::path::Path,
        entries: &[DetectableEntry],
    ) -> anyhow::Result<Self> {
        let _ = tokio::fs::create_dir_all(cache_dir()).await;
        Self::rebuild_with_fst(db_path, &cache_fst_path(), entries).await
    }

    /// Delete any stale redb file, create a fresh one, ingest entries, and
    /// (re)build the FST atomically at `fst_path`.
    ///
    /// The FST file is *replaced* via `rename(2)` from a sibling temp file;
    /// any concurrent `Mmap` of the previous FST continues to read the old
    /// inode (which is a valid FST) until it drops.  We do **not** delete
    /// the FST first: doing so would open a window during which a concurrent
    /// `open` would see an empty FST and silently miss every match.
    pub async fn rebuild_with_fst(
        db_path: &std::path::Path,
        fst_path: &std::path::Path,
        entries: &[DetectableEntry],
    ) -> anyhow::Result<Self> {
        if let Some(parent) = db_path.parent() {
            let _ = tokio::fs::create_dir_all(parent).await;
        }
        let _ = tokio::fs::remove_file(db_path).await;

        let db = Database::create(db_path)?;
        let mut this = Self {
            db,
            fst: empty_mmap_fst(),
            fst_path: fst_path.to_path_buf(),
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
    /// `exe_index`, and write the on-disk FST (then mmap it).
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

        // Build FST on disk, then mmap it.  An empty FST is a valid 36-byte
        // file; `SetBuilder` produces the exact bytes regardless of input.
        let sorted_names: Vec<Vec<u8>> = {
            let mut names: Vec<&[u8]> = exe_to_ids.keys().map(CompactString::as_bytes).collect();
            names.sort_unstable();
            names.into_iter().map(<[u8]>::to_vec).collect()
        };
        write_fst_file(&self.fst_path, &sorted_names)?;
        self.fst = load_mmap_fst(&self.fst_path)?;

        Ok(())
    }

    /// Reconstruct the in-memory exe_index and mmap the FST file.
    ///
    /// The FST file is the source of truth: if it exists and parses to a
    /// `Set` whose length matches the number of distinct exe names in the
    /// `EXES_TABLE`, we use it directly.  If the file is missing, empty,
    /// corrupt, or has a mismatched length, we rebuild it from the EXES
    /// rows and write the new FST atomically.  When the EXES table is
    /// empty (fresh install, or this is a read-only `open`) we fall back
    /// to an in-memory empty FST, which the field always represents
    /// correctly.
    fn load_fst_from_db(&mut self) -> anyhow::Result<()> {
        let read_txn: ReadTransaction = self.db.begin_read()?;

        // The table might not exist in a freshly created (but empty) database.
        let exes: redb::ReadOnlyTable<&str, &str> = match read_txn.open_table(EXES_TABLE) {
            Ok(t) => t,
            Err(_) => {
                self.fst = ensure_fst_file(&self.fst_path)?;
                return Ok(());
            }
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

        // If the on-disk FST agrees with EXES, mmap it directly.
        if let Ok(fst) = load_mmap_fst(&self.fst_path)
            && fst.len() == names.len()
        {
            self.fst = fst;
            return Ok(());
        }

        // Otherwise rebuild from EXES (handles missing file, empty file,
        // corrupt FST, or a length mismatch after a partial external edit).
        // The cost is one `SetBuilder::memory()` + one file write; this
        // path is taken at most once per (db_path, fst_path) pair.
        if !names.is_empty() {
            warn!(
                "FST at {} is stale or unreadable; rebuilding from EXES table",
                self.fst_path.display()
            );
            names.sort_unstable();
            let sorted: Vec<Vec<u8>> = names.iter().map(|n| n.as_bytes().to_vec()).collect();
            write_fst_file(&self.fst_path, &sorted)?;
        }

        self.fst = load_mmap_fst(&self.fst_path)?;
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
    /// 3. **redb `apps` table** — only reached for confirmed hits.  
    ///    Provides the rkyv-serialised entry for argument validation.
    pub fn match_process(
        &self,
        path: &str,
        args: &[&str],
    ) -> Option<(CompactString, CompactString)> {
        let variants = path_variants(path);
        let filename = path_filename(path);
        let exact = if filename.is_empty() {
            CompactString::default()
        } else {
            let mut exact = CompactString::with_capacity(filename.len() + 1);
            exact.push('>');
            exact.push_str(filename);
            exact
        };

        if !variants
            .iter()
            .map(CompactString::as_str)
            .chain((!exact.is_empty()).then_some(exact.as_str()))
            .any(|exe_name| self.fst.contains(exe_name.as_bytes()))
        {
            return None;
        }

        let read_txn: ReadTransaction = self.db.begin_read().ok()?;
        let apps: redb::ReadOnlyTable<&str, &[u8]> = read_txn.open_table(APPS_TABLE).ok()?;
        // Dedup buffer.  At most `variants.len() × max(ids per exe)` entries
        // ever — variants ≤ 4, ids ≤ 4 — so a stack-allocated `SmallVec` of
        // `&str` slices is both faster and zero-allocating versus a fresh
        // `AHashSet::default()`.  Declared after `pin` so the borrowed slices
        // are dropped first (Rust's reverse-declaration drop order).
        let pin = self.exe_index.pin();
        let mut seen: SmallVec<[&str; 8]> = SmallVec::new();

        for exe_name in variants
            .iter()
            .map(CompactString::as_str)
            .chain((!exact.is_empty()).then_some(exact.as_str()))
        {
            if !self.fst.contains(exe_name.as_bytes()) {
                continue;
            }

            if let Some(ids) = pin.get(exe_name) {
                for app_id in ids {
                    let id_str = app_id.as_str();
                    if seen.contains(&id_str) {
                        continue;
                    }
                    seen.push(id_str);

                    if let Ok(Some(guard)) = apps.get(app_id.as_str()) {
                        let bytes: &[u8] = guard.value();

                        // Copy into a 16-byte-aligned buffer so rkyv can access the
                        // archive safely: redb returns an internal buffer that is
                        // not guaranteed to satisfy the archived root's alignment
                        // requirement on every code path.
                        let mut aligned = rkyv::util::AlignedVec::<RKYV_ALIGNMENT>::new();
                        aligned.extend_from_slice(bytes);

                        if let Ok(archived) =
                            rkyv::access::<ArchivedDetectableEntry, rkyv::rancor::Error>(&aligned)
                            && archived_match(archived, &variants, filename, args)
                        {
                            return Some((
                                CompactString::from(archived.id.as_str()),
                                CompactString::from(archived.name.as_str()),
                            ));
                        }
                    }
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

/// Build a fresh in-memory empty `Set<Mmap>` for the field's initial value.
///
/// `Mmap` is not `Clone`, so we cannot share a single instance; instead we
/// reconstruct one from the canonical empty-FST v3 bytes (36 bytes).  This
/// is constant-time — one tiny anonymous mmap and one `Set::new` parse —
/// and only runs once per `open`/`rebuild`, never on the hot path.
fn empty_mmap_fst() -> Set<Mmap> {
    let bytes: Vec<u8> = fst::SetBuilder::memory()
        .into_inner()
        .expect("empty SetBuilder::memory into_inner");
    debug_assert!(bytes.len() >= 36, "empty FST shorter than v3 header");
    let mut mm = memmap2::MmapMut::map_anon(bytes.len()).expect("allocate empty FST backing");
    mm[..bytes.len()].copy_from_slice(&bytes);
    let mm = mm
        .make_read_only()
        .expect("freeze empty FST backing as read-only");
    Set::new(mm).expect("parse empty FST backing")
}

/// Make sure `path` contains a parseable FST file.  If the file is missing
/// or empty, write an empty FST (36 bytes) to it.  On a parse error we do
/// *not* overwrite: the caller decides whether to rebuild from EXES.
fn ensure_fst_file(path: &std::path::Path) -> anyhow::Result<Set<Mmap>> {
    if let Ok(fst) = load_mmap_fst(path) {
        return Ok(fst);
    }
    if path.exists() && path.metadata()?.len() > 0 {
        // Non-empty but unparsable — leave it alone; the caller will
        // rebuild from EXES and rename over it.
        return Err(anyhow::anyhow!(
            "FST at {} is not parseable",
            path.display()
        ));
    }
    write_fst_file(path, &[])?;
    load_mmap_fst(path)
}

/// Serialise an FST containing exactly `keys` to `path`, atomically.
///
/// Writes to `<path>.<pid>.<nanos>.tmp` first, flushes the writer, then
/// `rename(2)`s over `path`.  The rename is the only synchronisation point
/// with other readers: any concurrent `Mmap` of the previous FST continues
/// to read the old inode (which is a valid FST) until it drops.
///
/// **Durability note.** We do not `fsync(2)` the data file or its parent
/// directory.  On a power loss between `flush` and `rename` the rename may
/// not survive, leaving the on-disk FST empty or missing; the next `open`
/// detects that and rebuilds from the redb `EXES_TABLE`.  This is
/// acceptable for a cache: correctness is preserved at the cost of one
/// slower startup.  Callers that need a durable FST can `sync_all()` the
/// file before `rename`.
fn write_fst_file(path: &std::path::Path, keys: &[Vec<u8>]) -> anyhow::Result<()> {
    use std::fs::File;
    use std::io::{BufWriter, Write as _};

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let tmp = unique_tmp_sibling(path);
    {
        let file = File::create(&tmp)?;
        let mut writer = BufWriter::new(file);
        {
            let mut builder = fst::SetBuilder::new(&mut writer)
                .map_err(|e| anyhow::anyhow!("fst SetBuilder::new: {e}"))?;
            for k in keys {
                builder
                    .insert(k.as_slice())
                    .map_err(|e| anyhow::anyhow!("fst insert: {e}"))?;
            }
            builder
                .finish()
                .map_err(|e| anyhow::anyhow!("fst finish: {e}"))?;
        }
        writer.flush()?;
    }

    // Atomic replace: either succeeds completely (old file is gone) or
    // fails without modifying `path`.  Old inodes stay alive via any
    // concurrent `Mmap`.
    std::fs::rename(&tmp, path)?;
    Ok(())
}

fn unique_tmp_sibling(path: &std::path::Path) -> PathBuf {
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let mut name = path
        .file_name()
        .map(|s| s.to_os_string())
        .unwrap_or_default();
    name.push(format!(".{pid}.{nanos}.tmp"));
    path.with_file_name(name)
}

/// Memory-map the FST file at `path` and wrap it in a `Set<Mmap>`.
///
/// Returns `Err` for every expected failure: missing file, zero-byte file,
/// or a file whose contents are not a parseable FST.  On success the
/// kernel is asked to optimise for random access (`MADV_RANDOM`) — our
/// hot path is a sequence of point lookups, so read-ahead would be pure
/// waste.
///
/// # Safety justification
///
/// `MmapOptions::map` is `unsafe` because the kernel cannot, on its own,
/// prevent a process that holds a writable file descriptor from mutating
/// the bytes underneath a reader.  The contract is upheld here:
///   1. The file is opened read-only via `File::open`; no writer fd exists
///      in this process.
///   2. `write_fst_file` always replaces the file via `rename(2)`, so any
///      stale mmap keeps reading the old inode (a valid FST) until it
///      drops.  Per memmap2's contract, the `Mmap` is independent of the
///      `File` — closing the `File` after `map()` does not invalidate the
///      mapping.
fn load_mmap_fst(path: &std::path::Path) -> anyhow::Result<Set<Mmap>> {
    use std::fs::File;

    let file = File::open(path).map_err(|e| anyhow::anyhow!("open {}: {e}", path.display()))?;
    let len = file
        .metadata()
        .map_err(|e| anyhow::anyhow!("stat {}: {e}", path.display()))?
        .len();
    if len == 0 {
        return Err(anyhow::anyhow!("FST at {} is empty", path.display()));
    }

    // SAFETY: see function-level justification.
    let mmap = unsafe { memmap2::MmapOptions::new().map(&file) }
        .map_err(|e| anyhow::anyhow!("mmap {}: {e}", path.display()))?;

    // Hot-path lookups are point queries with no temporal locality between
    // them, so advise the kernel accordingly.  Ignore the result: the
    // platform may not support this advice, and it is purely a hint.
    let _ = mmap.advise(memmap2::Advice::Random);

    Set::new(mmap).map_err(|e| anyhow::anyhow!("parse FST at {}: {e}", path.display()))
}

// ─── Helper: zero-copy match against an archived entry ───────────────────────

fn archived_match(
    archived: &ArchivedDetectableEntry,
    variants: &[CompactString],
    filename: &str,
    args: &[&str],
) -> bool {
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
///
/// Builds the longest suffix once and derives shorter suffixes as substrings,
/// eliminating repeated `join("/")` heap allocations.
pub fn path_variants(path: &str) -> SmallVec<[CompactString; 8]> {
    // Support both Unix `/` and Windows `\` separators.
    let parts: SmallVec<[&str; 16]> = path.split(['/', '\\']).filter(|s| !s.is_empty()).collect();
    let mut variants: SmallVec<[CompactString; 8]> = SmallVec::new();

    let start = if parts.len() > 4 { parts.len() - 4 } else { 0 };

    if start >= parts.len() {
        return variants;
    }

    // Build the longest suffix once; derive shorter suffixes as substrings.
    let full = parts[start..].join("/");
    let mut offset = 0;
    for _ in start..parts.len() {
        let suffix = &full[offset..];
        variants.push(CompactString::from(suffix));
        // All 64-bit suffixes end with "64"; skip when impossible.
        if suffix.ends_with("64") {
            let cleaned = strip_64_suffix(suffix);
            if cleaned.len() < suffix.len() {
                variants.push(CompactString::from(cleaned));
            }
        }
        // Advance past the next '/' separator to get the next shorter suffix.
        if let Some(pos) = suffix.find('/') {
            offset += pos + 1;
        }
    }

    variants
}

/// Remove common 64-bit marker suffixes from a name.
///
/// Returns a subslice of the input with zero allocation.
/// Checks only at the end of the string so names like "base64encoder" are
/// left intact.
///
/// Uses direct byte comparisons instead of `str::strip_suffix` to avoid
/// going through the stdlib pattern-matching machinery (`strip_suffix_of`
/// → `ends_with` → `eq<u8>`), which produces variable codegen across
/// different rustc versions. The `.x64` case uses a 4-byte slice
/// comparison so LLVM can lower it to a single u32 load + compare.
#[inline]
pub fn strip_64_suffix(name: &str) -> &str {
    let b = name.as_bytes();
    let len = b.len();

    // Most-specific suffix first: ".x64" (4 chars).
    // Slice equality lets LLVM emit a single 4-byte comparison,
    // avoiding the multiple individual byte loads of a match+guard.
    if len >= 4 && b[len - 4..] == *b".x64" {
        return &name[..len - 4];
    }

    // All remaining suffixes end with "64", so bail early otherwise.
    if len < 2 || b[len - 2] != b'6' || b[len - 1] != b'4' {
        return name;
    }

    // "x64" or "_64" (3 chars)
    if len >= 3 && (b[len - 3] == b'x' || b[len - 3] == b'_') {
        return &name[..len - 3];
    }

    // Bare "64" catch-all.
    &name[..len - 2]
}

/// Extract the last path component from a Unix or Windows path.
///
/// Returns an empty string for paths that consist entirely of separators,
/// and the full path unchanged when no separator is present.
pub fn path_filename(path: &str) -> &str {
    let bytes = path.as_bytes();
    // Skip trailing separators from the end.
    let mut end = bytes.len();
    while end > 0 && matches!(bytes[end - 1], b'/' | b'\\') {
        end -= 1;
    }
    if end == 0 {
        return "";
    }
    // Scan backwards to find the separator before the filename.
    let mut start = end;
    while start > 0 && !matches!(bytes[start - 1], b'/' | b'\\') {
        start -= 1;
    }
    &path[start..end]
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

#[cfg(test)]
mod tests {
    use super::{DETECTABLE_URL, parse_detectable_entries};

    #[test]
    fn detectable_url_uses_discord_v10_api() {
        assert_eq!(
            DETECTABLE_URL,
            "https://discord.com/api/v10/applications/detectable"
        );
    }

    #[test]
    fn parse_detectable_entries_accepts_array_payload() {
        let body =
            br#"[{"id":"1","name":"Game","executables":[{"name":"game","is_launcher":false}]}]"#;
        let entries = parse_detectable_entries(body).expect("array payload should parse");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id, "1");
    }

    #[test]
    fn parse_detectable_entries_accepts_wrapped_payload() {
        let body = br#"{"applications":[{"id":"1","name":"Game","executables":[{"name":"game","is_launcher":false}]}]}"#;
        let entries = parse_detectable_entries(body).expect("wrapped payload should parse");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id, "1");
    }

    #[test]
    fn parse_detectable_entries_accepts_data_applications_payload() {
        let body = br#"{"data":{"applications":[{"id":"1","name":"Game","executables":[{"name":"game","is_launcher":false}]}]}}"#;
        let entries =
            parse_detectable_entries(body).expect("data.applications payload should parse");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id, "1");
    }

    #[test]
    fn parse_detectable_entries_accepts_results_payload() {
        let body = br#"{"results":[{"id":"1","name":"Game","executables":[{"name":"game","is_launcher":false}]}],"total":1}"#;
        let entries = parse_detectable_entries(body).expect("results payload should parse");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id, "1");
    }

    #[test]
    fn parse_detectable_entries_reports_unexpected_shape_details() {
        let body = br#"{"payload":{"items":[{"slug":"game"}]}}"#;
        let err = parse_detectable_entries(body).expect_err("payload should fail to parse");
        let msg = err.to_string();
        assert!(msg.contains("unexpected detectable API payload shape"));
        assert!(msg.contains(r#"top-level object(keys={"payload"})"#));
    }

    #[test]
    fn parse_detectable_entries_skips_invalid_array_items() {
        let body = br#"[
            {"id":"1","name":"Game","executables":[{"name":"game","is_launcher":false}]},
            {"id":"broken","name":"Broken","executables":"invalid"},
            {"id":"2","name":"Game2","executables":[{"name":"game2","is_launcher":false}]}
        ]"#;
        let entries = parse_detectable_entries(body).expect("valid entries should still parse");
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].id, "1");
        assert_eq!(entries[1].id, "2");
    }
}
