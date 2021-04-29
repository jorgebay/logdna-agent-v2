use std::sync::atomic::AtomicI64;
use std::sync::atomic::{AtomicU64, Ordering};

use chrono::Utc;
use jemalloc_ctl::stats::{active, active_mib, allocated, allocated_mib, resident, resident_mib};
use jemalloc_ctl::{epoch, epoch_mib};
use json::object;
use lazy_static::lazy_static;
use log::info;
use prometheus::{register_int_counter, register_int_counter_vec};
use prometheus::{IntCounter, IntCounterVec};
use std::thread::sleep;
use std::time::Duration;

lazy_static! {
    static ref METRICS: Metrics = Metrics::new();
    pub static ref FS_EVENTS: IntCounterVec =
        register_int_counter_vec!("fs_events", "Filesystem events received", labels::FS_ALL)
            .unwrap();
    pub static ref FS_LINES: IntCounter =
        register_int_counter!("fs_lines", "Filesystem lines parsed").unwrap();
    pub static ref FS_BYTES: IntCounter =
        register_int_counter!("fs_bytes", "Filesystem bytes read").unwrap();
    pub static ref FS_PARTIAL_READS: IntCounter =
        register_int_counter!("fs_partial_reads", "Filesystem partial reads").unwrap();
}

mod labels {
    pub const CREATE: &str = "create";
    pub const DELETE: &str = "delete";
    pub const WRITE: &str = "write";
    pub static FS_ALL: &[&str] = &[CREATE, WRITE, DELETE];
}

pub struct Metrics {
    last_flush: AtomicI64,
    fs: Fs,
    memory: Memory,
    http: Http,
    k8s: K8s,
    journald: Journald,
}

impl Metrics {
    fn new() -> Self {
        Self {
            last_flush: AtomicI64::new(Utc::now().timestamp()),
            fs: Fs::new(),
            memory: Memory::new(),
            http: Http::new(),
            k8s: K8s::new(),
            journald: Journald::new(),
        }
    }

    pub fn start() {
        loop {
            sleep(Duration::from_secs(60));
            info!("{}", Metrics::print());
            Metrics::reset();
        }
    }

    pub fn reset() {
        METRICS
            .last_flush
            .store(Utc::now().timestamp(), Ordering::Relaxed);
        Metrics::fs().reset();
        Metrics::memory().reset();
        Metrics::http().reset();
        Metrics::k8s().reset();
        Metrics::journald().reset();
    }

    pub fn elapsed() -> u64 {
        (Utc::now().timestamp() - METRICS.last_flush.load(Ordering::Relaxed)) as u64
    }

    pub fn fs() -> &'static Fs {
        &METRICS.fs
    }

    pub fn memory() -> &'static Memory {
        &METRICS.memory
    }

    pub fn http() -> &'static Http {
        &METRICS.http
    }

    pub fn k8s() -> &'static K8s {
        &METRICS.k8s
    }

    pub fn journald() -> &'static Journald {
        &METRICS.journald
    }

    pub fn print() -> String {
        let memory = Metrics::memory();
        let http = Metrics::http();
        let k8s = Metrics::k8s();
        let journald = Metrics::journald();

        let object = object! {
            "fs" => object!{
                "events" => FS_EVENTS.with_label_values(labels::FS_ALL).get(),
                "creates" => FS_EVENTS.with_label_values(&[labels::CREATE]).get(),
                "deletes" => FS_EVENTS.with_label_values(&[labels::DELETE]).get(),
                "writes" => FS_EVENTS.with_label_values(&[labels::WRITE]).get(),
                "lines" => FS_LINES.get(),
                "bytes" => FS_BYTES.get(),
                "partial_reads" => FS_PARTIAL_READS.get(),
            },
            "memory" => object!{
                "active" => memory.read_active(),
                "allocated" => memory.read_allocated(),
                "resident" => memory.read_resident(),
            },
            "ingest" => object!{
                "requests" => http.read_requests(),
                "throughput" => http.read_request_size(),
                "rate_limits" => http.read_limit_hits(),
                "retries" => http.read_retries(),
            },
            "k8s" => object!{
                "lines" => k8s.read_lines(),
                "polls" => k8s.read_polls(),
                "creates" => k8s.read_creates(),
                "deletes" => k8s.read_deletes(),
                "events" => k8s.read_events(),
                "notifies" => k8s.read_notifies(),
            },
            "journald" => object!{
                "lines" => journald.read_lines(),
                "bytes" => journald.read_bytes(),
            },
        };

        object.to_string()
    }
}

#[derive(Default)]
pub struct Fs {}

impl Fs {
    pub fn new() -> Self {
        Self {}
    }

    // TODO: Remove
    fn reset(&self) {}

    pub fn increment_creates(&self) {
        FS_EVENTS.with_label_values(&[labels::CREATE]).inc();
    }

    pub fn increment_deletes(&self) {
        FS_EVENTS.with_label_values(&[labels::DELETE]).inc();
    }

    pub fn increment_writes(&self) {
        FS_EVENTS.with_label_values(&[labels::WRITE]).inc();
    }

    pub fn increment_lines(&self) {
        FS_LINES.inc();
    }

    pub fn add_bytes(&self, num: u64) {
        FS_BYTES.inc_by(num);
    }

    pub fn increment_partial_reads(&self) {
        FS_PARTIAL_READS.inc();
    }
}

pub struct Memory {
    epoch_mib: epoch_mib,
    active_mib: active_mib,
    allocated_mib: allocated_mib,
    resident_mib: resident_mib,
}

impl Memory {
    pub fn new() -> Self {
        Self {
            epoch_mib: epoch::mib().unwrap(),
            active_mib: active::mib().unwrap(),
            allocated_mib: allocated::mib().unwrap(),
            resident_mib: resident::mib().unwrap(),
        }
    }

    pub fn reset(&self) {}

    pub fn read_active(&self) -> u64 {
        self.epoch_mib.advance().unwrap();
        self.active_mib.read().unwrap() as u64
    }

    pub fn read_allocated(&self) -> u64 {
        self.epoch_mib.advance().unwrap();
        self.allocated_mib.read().unwrap() as u64
    }

    pub fn read_resident(&self) -> u64 {
        self.epoch_mib.advance().unwrap();
        self.resident_mib.read().unwrap() as u64
    }
}

impl Default for Memory {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Default)]
pub struct Http {
    requests: AtomicU64,
    limit_hits: AtomicU64,
    request_size: AtomicU64,
    retries: AtomicU64,
}

impl Http {
    pub fn new() -> Self {
        Self {
            requests: AtomicU64::new(0),
            limit_hits: AtomicU64::new(0),
            request_size: AtomicU64::new(0),
            retries: AtomicU64::new(0),
        }
    }

    pub fn reset(&self) {
        self.requests.store(0, Ordering::Relaxed);
        self.limit_hits.store(0, Ordering::Relaxed);
        self.request_size.store(0, Ordering::Relaxed);
        self.retries.store(0, Ordering::Relaxed);
    }

    pub fn increment_requests(&self) {
        self.requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn read_requests(&self) -> u64 {
        self.requests.load(Ordering::Relaxed)
    }

    pub fn increment_limit_hits(&self) {
        self.limit_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn read_limit_hits(&self) -> u64 {
        self.limit_hits.load(Ordering::Relaxed)
    }

    pub fn add_request_size(&self, num: u64) {
        self.request_size.fetch_add(num, Ordering::Relaxed);
    }

    pub fn read_request_size(&self) -> u64 {
        self.request_size.load(Ordering::Relaxed)
    }

    pub fn increment_retries(&self) {
        self.retries.fetch_add(1, Ordering::Relaxed);
    }

    pub fn read_retries(&self) -> u64 {
        self.retries.load(Ordering::Relaxed)
    }
}

#[derive(Default)]
pub struct K8s {
    lines: AtomicU64,
    polls: AtomicU64,
    creates: AtomicU64,
    deletes: AtomicU64,
    events: AtomicU64,
    notifies: AtomicU64,
}

impl K8s {
    pub fn new() -> Self {
        Self {
            lines: AtomicU64::new(0),
            polls: AtomicU64::new(0),
            creates: AtomicU64::new(0),
            deletes: AtomicU64::new(0),
            events: AtomicU64::new(0),
            notifies: AtomicU64::new(0),
        }
    }

    pub fn reset(&self) {
        self.lines.store(0, Ordering::Relaxed);
        self.polls.store(0, Ordering::Relaxed);
        self.creates.store(0, Ordering::Relaxed);
        self.deletes.store(0, Ordering::Relaxed);
        self.events.store(0, Ordering::Relaxed);
        self.notifies.store(0, Ordering::Relaxed);
    }

    pub fn increment_lines(&self) {
        self.lines.fetch_add(1, Ordering::Relaxed);
    }

    pub fn read_lines(&self) -> u64 {
        self.lines.load(Ordering::Relaxed)
    }

    pub fn increment_polls(&self) {
        self.polls.fetch_add(1, Ordering::Relaxed);
    }

    pub fn read_polls(&self) -> u64 {
        self.polls.load(Ordering::Relaxed)
    }

    pub fn increment_creates(&self) {
        self.creates.fetch_add(1, Ordering::Relaxed);
    }

    pub fn read_creates(&self) -> u64 {
        self.creates.load(Ordering::Relaxed)
    }

    pub fn increment_deletes(&self) {
        self.deletes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn read_deletes(&self) -> u64 {
        self.deletes.load(Ordering::Relaxed)
    }

    pub fn increment_events(&self) {
        self.events.fetch_add(1, Ordering::Relaxed);
    }

    pub fn read_events(&self) -> u64 {
        self.events.load(Ordering::Relaxed)
    }

    pub fn increment_notifies(&self) {
        self.notifies.fetch_add(1, Ordering::Relaxed);
    }

    pub fn read_notifies(&self) -> u64 {
        self.notifies.load(Ordering::Relaxed)
    }
}

#[derive(Default)]
pub struct Journald {
    lines: AtomicU64,
    bytes: AtomicU64,
}

impl Journald {
    pub fn new() -> Self {
        Self {
            lines: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
        }
    }

    pub fn reset(&self) {
        self.lines.store(0, Ordering::Relaxed);
        self.bytes.store(0, Ordering::Relaxed);
    }

    pub fn increment_lines(&self) {
        self.lines.fetch_add(1, Ordering::Relaxed);
    }

    pub fn read_lines(&self) -> u64 {
        self.lines.load(Ordering::Relaxed)
    }

    pub fn add_bytes(&self, num: u64) {
        self.bytes.fetch_add(num, Ordering::Relaxed);
    }

    pub fn read_bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }
}
