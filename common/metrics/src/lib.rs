use jemalloc_ctl::stats::{active, active_mib, allocated, allocated_mib, resident, resident_mib};
use jemalloc_ctl::{epoch, epoch_mib};
use json::object;
use lazy_static::lazy_static;
use log::info;
use prometheus::{
    register_histogram, register_int_counter, register_int_counter_vec, Histogram, IntCounter,
    IntCounterVec,
};
use std::thread::sleep;
use std::time::Duration;

lazy_static! {
    static ref METRICS: Metrics = Metrics::new();
    static ref FS_EVENTS: IntCounterVec =
        register_int_counter_vec!("fs_events", "Filesystem events received", labels::FS_ALL)
            .unwrap();
    static ref FS_LINES: IntCounter =
        register_int_counter!("fs_lines", "Filesystem lines parsed").unwrap();
    static ref FS_BYTES: IntCounter =
        register_int_counter!("fs_bytes", "Filesystem bytes read").unwrap();
    static ref FS_PARTIAL_READS: IntCounter =
        register_int_counter!("fs_partial_reads", "Filesystem partial reads").unwrap();
    static ref INGEST_RETRIES: IntCounter = register_int_counter!(
        "ingest_retries",
        "Retry attempts made to the http ingestion service"
    )
    .unwrap();
    static ref INGEST_RATE_LIMIT_HITS: IntCounter = register_int_counter!(
        "ingest_rate_limit_hits",
        "Number of times the http request was delayed due to the rate limiter"
    )
    .unwrap();
    static ref INGEST_REQUESTS: Histogram = register_histogram!(
        "ingest_requests",
        "Size of the requests made to http ingestion service"
    )
    .unwrap();
    static ref K8S_EVENTS: IntCounterVec =
        register_int_counter_vec!("k8s_events", "Kubernetes events received", labels::K8S_ALL)
            .unwrap();
    static ref K8S_LINES: IntCounter =
        register_int_counter!("fs_lines", "Kubernetes event lines read").unwrap();
    static ref JOURNAL_RECORDS: Histogram =
        register_histogram!("journald_records", "Size of the Journald log entries read").unwrap();
}

mod labels {
    pub const CREATE: &str = "create";
    pub const DELETE: &str = "delete";
    pub const WRITE: &str = "write";
    pub static FS_ALL: &[&str] = &[CREATE, WRITE, DELETE];
    pub static K8S_ALL: &[&str] = &[CREATE, DELETE];
}

pub struct Metrics {
    fs: Fs,
    memory: Memory,
    http: Http,
    k8s: K8s,
    journald: Journald,
}

impl Metrics {
    fn new() -> Self {
        Self {
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
        }
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
            // CPU and memory metrics are exported to Prometheus by default only on linux.
            // We still rely on jemalloc stats for this periodic printing the memory metrics
            // as it supports more platforms
            "memory" => object!{
                "active" => memory.read_active(),
                "allocated" => memory.read_allocated(),
                "resident" => memory.read_resident(),
            },
            "ingest" => object!{
                "requests" => INGEST_REQUESTS.get_sample_count(),
                "requests_size" => INGEST_REQUESTS.get_sample_sum(),
                "rate_limits" => INGEST_RATE_LIMIT_HITS.get(),
                "retries" => INGEST_RETRIES.get(),
            },
            "k8s" => object!{
                "lines" => K8S_LINES.get(),
                "creates" => K8S_EVENTS.with_label_values(&[labels::CREATE]).get(),
                "deletes" => K8S_EVENTS.with_label_values(&[labels::DELETE]).get(),
                "events" => K8S_EVENTS.with_label_values(labels::K8S_ALL).get(),
            },
            "journald" => object!{
                "lines" => JOURNAL_RECORDS.get_sample_count(),
                "bytes" => JOURNAL_RECORDS.get_sample_sum(),
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
pub struct Http {}

impl Http {
    pub fn new() -> Self {
        Self {}
    }

    pub fn increment_limit_hits(&self) {
        INGEST_RATE_LIMIT_HITS.inc();
    }

    pub fn add_request_size(&self, num: u64) {
        INGEST_REQUESTS.observe(num as f64);
    }

    pub fn increment_retries(&self) {
        INGEST_RETRIES.inc();
    }
}

#[derive(Default)]
pub struct K8s {}

impl K8s {
    pub fn new() -> Self {
        Self {}
    }

    pub fn increment_lines(&self) {
        K8S_LINES.inc();
    }

    pub fn increment_creates(&self) {
        K8S_EVENTS.with_label_values(&[labels::CREATE]).inc();
    }

    pub fn increment_deletes(&self) {
        K8S_EVENTS.with_label_values(&[labels::DELETE]).inc();
    }
}

#[derive(Default)]
pub struct Journald {}

impl Journald {
    pub fn new() -> Self {
        Self {}
    }

    pub fn add_bytes(&self, num: usize) {
        JOURNAL_RECORDS.observe(num as f64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fs_events_should_be_incremented_from_labels() {
        let initial = FS_EVENTS.with_label_values(labels::FS_ALL).get();
        METRICS.fs.increment_creates();
        assert_eq!(FS_EVENTS.with_label_values(labels::FS_ALL).get(), initial + 1);
    }
}
