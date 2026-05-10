use dirpc::sample_entries;
#[cfg(not(miri))]
use divan::AllocProfiler;
#[cfg(not(miri))]
use mimalloc_safe::MiMalloc;

#[cfg(not(miri))]
#[global_allocator]
static GLOBAL: AllocProfiler<MiMalloc> = AllocProfiler::new(MiMalloc);

fn main() {
    divan::main();
}

// ─── IPC codec ───────────────────────────────────────────────────────────────

#[divan::bench]
fn bench_encode_small() -> Vec<u8> {
    dirpc::encode(1, r#"{"cmd":"SET_ACTIVITY","nonce":"abc"}"#)
}

#[divan::bench]
fn bench_decode_small() {
    let buf = dirpc::encode(1, r#"{"cmd":"SET_ACTIVITY","nonce":"abc"}"#);
    divan::black_box(dirpc::decode(&buf));
}

#[divan::bench(args = [64, 512, 4096])]
fn bench_encode_payload_size(bencher: divan::Bencher, n: usize) {
    let payload = "x".repeat(n);
    bencher.bench(|| dirpc::encode(1, &payload));
}

// ─── Path helpers ─────────────────────────────────────────────────────────────

#[divan::bench]
fn bench_path_variants_short() {
    divan::black_box(dirpc::path_variants("/usr/bin/csgo"));
}

#[divan::bench]
fn bench_path_variants_long() {
    divan::black_box(dirpc::path_variants(
        "/opt/games/steam/steamapps/common/CS2/linux/bin/cs2",
    ));
}

#[divan::bench]
fn bench_path_variants_win() {
    divan::black_box(dirpc::path_variants(
        r"C:\Program Files (x86)\Steam\steamapps\common\Counter-Strike 2\cs2.exe",
    ));
}

#[divan::bench(args = ["game64", "game.x64", "game_64", "gamex64", "game", "base64encoder"])]
fn bench_strip_64_suffix(bencher: divan::Bencher, name: &str) {
    bencher.bench(|| divan::black_box(dirpc::strip_64_suffix(name)));
}

#[divan::bench(args = ["/usr/bin/csgo", r"C:\games\overwatch.exe", "csgo", ""])]
fn bench_path_filename(bencher: divan::Bencher, path: &str) {
    bencher.bench(|| divan::black_box(dirpc::path_filename(path)));
}

// ─── Linear scan (old path) ───────────────────────────────────────────────────

#[divan::bench]
fn bench_match_process_hit(bencher: divan::Bencher) {
    let entries = sample_entries();
    bencher.bench(|| {
        divan::black_box(dirpc::match_process(
            "/home/user/.steam/csgo",
            &[],
            &entries,
        ))
    });
}

#[divan::bench]
fn bench_match_process_miss(bencher: divan::Bencher) {
    let entries = sample_entries();
    bencher.bench(|| {
        divan::black_box(dirpc::match_process(
            "/usr/bin/definitely-not-a-game",
            &[],
            &entries,
        ))
    });
}

// ─── DetectableDb (FST + redb fast path) ─────────────────────────────────────

/// Spin up a one-shot Tokio runtime to call the async `rebuild` helper from
/// sync benchmark setup code.
fn build_detectable_db(db_path: &std::path::Path) -> dirpc::process::detectable::DetectableDb {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");
    rt.block_on(dirpc::process::detectable::DetectableDb::rebuild(
        db_path,
        &sample_entries(),
    ))
    .expect("DetectableDb::rebuild")
}

#[divan::bench]
fn bench_detectable_db_match_hit(bencher: divan::Bencher) {
    let path = std::env::temp_dir().join("dirpc_bench_db_hit.redb");
    let db = build_detectable_db(&path);
    bencher.bench(|| divan::black_box(db.match_process("/home/user/.steam/csgo", &[])));
}

#[divan::bench]
fn bench_detectable_db_match_miss(bencher: divan::Bencher) {
    let path = std::env::temp_dir().join("dirpc_bench_db_miss.redb");
    let db = build_detectable_db(&path);
    bencher.bench(|| divan::black_box(db.match_process("/usr/bin/definitely-not-a-game", &[])));
}

#[divan::bench]
fn bench_detectable_db_match_win_path(bencher: divan::Bencher) {
    let path = std::env::temp_dir().join("dirpc_bench_db_win.redb");
    let db = build_detectable_db(&path);
    bencher.bench(|| divan::black_box(db.match_process(r"C:\games\overwatch.exe", &[])));
}

// ─── Timestamp helpers ────────────────────────────────────────────────────────

#[divan::bench]
fn bench_maybe_to_ms_seconds() {
    divan::black_box(dirpc::maybe_to_ms(1_700_000_000));
}

#[divan::bench]
fn bench_maybe_to_ms_millis() {
    divan::black_box(dirpc::maybe_to_ms(1_700_000_000_000));
}
