// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

pub trait StateMetricsCollector: Send + Sync + 'static {
    fn record_memory_usage(&self, operator_id: u32, bytes: usize);
    fn record_spill_duration(&self, operator_id: u32, duration_ms: u128);
    fn record_compaction_duration(&self, operator_id: u32, is_major: bool, duration_ms: u128);
    fn inc_io_errors(&self, operator_id: u32);
}

/// Default no-op implementation.
pub struct NoopMetricsCollector;
impl StateMetricsCollector for NoopMetricsCollector {
    fn record_memory_usage(&self, _: u32, _: usize) {}
    fn record_spill_duration(&self, _: u32, _: u128) {}
    fn record_compaction_duration(&self, _: u32, _: bool, _: u128) {}
    fn inc_io_errors(&self, _: u32) {}
}
