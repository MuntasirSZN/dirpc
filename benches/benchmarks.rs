use divan::AllocProfiler;
use mimalloc_safe::MiMalloc;

#[global_allocator]
static GLOBAL: AllocProfiler<MiMalloc> = AllocProfiler::new(MiMalloc);

fn main() {
    divan::main();
}

/// A small inline sample of detectable entries for benchmarks that need a list.
///
/// These do **not** require a network call or embedded binary, keeping the
/// benchmark self-contained and fast to set up.
fn sample_entries() -> Vec<dirpc::DetectableEntry> {
    vec![
        dirpc::DetectableEntry {
            id: "359550982723468289".to_string(),
            name: "Counter-Strike: Global Offensive".to_string(),
            executables: vec![dirpc::Executable {
                name: "csgo".to_string(),
                is_launcher: false,
                arguments: None,
                os: None,
            }],
        },
        dirpc::DetectableEntry {
            id: "356869127241924608".to_string(),
            name: "Overwatch".to_string(),
            executables: vec![dirpc::Executable {
                name: "Overwatch.exe".to_string(),
                is_launcher: false,
                arguments: None,
                os: None,
            }],
        },
        dirpc::DetectableEntry {
            id: "356869127241924609".to_string(),
            name: "Dummy Game".to_string(),
            executables: vec![dirpc::Executable {
                name: "dummy_game64".to_string(),
                is_launcher: false,
                arguments: None,
                os: None,
            }],
        },
    ]
}

// ── IPC frame encoding / decoding ─────────────────────────────────────────────

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

// ── Path helpers ──────────────────────────────────────────────────────────────

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

// ── Detectable matching ───────────────────────────────────────────────────────

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

// ── Timestamp conversion ──────────────────────────────────────────────────────

#[divan::bench]
fn bench_maybe_to_ms_seconds() {
    divan::black_box(dirpc::maybe_to_ms(1_700_000_000));
}

#[divan::bench]
fn bench_maybe_to_ms_millis() {
    divan::black_box(dirpc::maybe_to_ms(1_700_000_000_000));
}
