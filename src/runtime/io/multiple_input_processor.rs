// StreamMultipleInputProcessor - Multiple input stream processor
//
// Multiple input stream processor implementation for MultipleInputStreamOperator
// Reference Flink's StreamMultipleInputProcessor implementation

use crate::runtime::io::{
    DataInputStatus, StreamInputProcessor, AvailabilityProvider,
    input_selection::{MultipleInputSelectionHandler, InputSelection},
};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};

/// StreamMultipleInputProcessor - Multiple input stream processor
/// 
/// Multiple input stream processor implementation for MultipleInputStreamOperator
pub struct StreamMultipleInputProcessor {
    /// Input selection handler
    input_selection_handler: Arc<MultipleInputSelectionHandler>,
    /// Multiple input processors
    input_processors: Vec<Box<dyn StreamInputProcessor>>,
    /// Availability helper
    availability_helper: Arc<MultipleFuturesAvailabilityHelper>,
    /// Whether prepared
    is_prepared: AtomicBool,
    /// Last read input index (initialized to 1, always try to read from the first input)
    last_read_input_index: AtomicI32,
}

impl StreamMultipleInputProcessor {
    /// Create new multiple input processor
    pub fn new(
        input_selection_handler: Arc<MultipleInputSelectionHandler>,
        input_processors: Vec<Box<dyn StreamInputProcessor>>,
    ) -> Self {
        let availability_helper = Arc::new(MultipleFuturesAvailabilityHelper::new(
            input_processors.len(),
        ));

        Self {
            input_selection_handler,
            input_processors,
            availability_helper,
            is_prepared: AtomicBool::new(false),
            last_read_input_index: AtomicI32::new(1),
        }
    }

    /// Select first input index to read
    /// 
    /// Note: The operator's first nextSelection() call must be executed after this operator is opened,
    /// to ensure any changes to input selection in its open() method take effect.
    fn select_first_reading_input_index(&mut self) -> i32 {
        self.input_selection_handler.next_selection();
        self.is_prepared.store(true, Ordering::Relaxed);
        self.select_next_reading_input_index()
    }

    /// Select next input index to read
    fn select_next_reading_input_index(&self) -> i32 {
        if !self.input_selection_handler.is_any_input_available() {
            self.full_check_and_set_available();
        }

        let reading_input_index = self
            .input_selection_handler
            .select_next_input_index(self.last_read_input_index.load(Ordering::Relaxed));

        if reading_input_index == InputSelection::NONE_AVAILABLE {
            return InputSelection::NONE_AVAILABLE;
        }

        // To avoid starvation, if input selection is ALL and availableInputsMask is not ALL,
        // always try to check and set availability for another input
        if self
            .input_selection_handler
            .should_set_available_for_another_input()
        {
            self.full_check_and_set_available();
        }

        reading_input_index
    }

    /// Fully check and set availability
    /// 
    /// Check availability of all input processors and update input selection handler
    fn full_check_and_set_available(&self) {
        for i in 0..self.input_processors.len() {
            let input_processor = &self.input_processors[i];

            // TODO: is_available() may be an expensive operation (checking volatile).
            // If one input is continuously available while another is not, we will check this volatile once per record processed.
            // This can be optimized to check once per NetworkBuffer processed

            if input_processor.is_approximately_available() || input_processor.is_available() {
                self.input_selection_handler.set_available(i);
            }
        }
    }

    /// Close all input processors
    pub fn close(&mut self) -> Result<(), Box<dyn std::error::Error + Send>> {
        let mut first_error: Option<Box<dyn std::error::Error + Send>> = None;

        for input in &mut self.input_processors {
            if let Err(e) = input.close() {
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
        }

        if let Some(e) = first_error {
            Err(e)
        } else {
            Ok(())
        }
    }
}

impl StreamInputProcessor for StreamMultipleInputProcessor {
    fn process_input(&mut self) -> Result<DataInputStatus, Box<dyn std::error::Error + Send>> {
        let reading_input_index = if self.is_prepared.load(Ordering::Relaxed) {
            self.select_next_reading_input_index()
        } else {
            // Preparation work is not placed in the constructor because all work must be
            // executed after all operators are opened.
            self.select_first_reading_input_index()
        };

        if reading_input_index == InputSelection::NONE_AVAILABLE {
            return Ok(DataInputStatus::NothingAvailable);
        }

        self.last_read_input_index
            .store(reading_input_index, Ordering::Relaxed);

        let input_status = self.input_processors[reading_input_index as usize].process_input()?;

        self.input_selection_handler.update_status_and_selection(
            input_status,
            reading_input_index as usize,
        )
    }

    fn prepare_snapshot(
        &self,
        checkpoint_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        // Prepare snapshot for all inputs
        for processor in &self.input_processors {
            processor.prepare_snapshot(checkpoint_id)?;
        }
        Ok(())
    }
}

impl AvailabilityProvider for StreamMultipleInputProcessor {
    fn is_available(&self) -> bool {
        if self.input_selection_handler.is_any_input_available()
            || self.input_selection_handler.are_all_inputs_finished()
        {
            return true;
        }
        self.availability_helper.is_available()
    }

    fn get_available_future(&self) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        if self.input_selection_handler.is_any_input_available()
            || self.input_selection_handler.are_all_inputs_finished()
        {
            return Box::pin(async move {});
        }

        self.availability_helper.reset_to_unavailable();

        // Clone necessary references to avoid borrowing issues
        let availability_helper = self.availability_helper.clone();
        let input_selection_handler = self.input_selection_handler.clone();
        let input_processors_len = self.input_processors.len();
        
        // Collect Futures that need to wait
        let mut futures = Vec::new();
        for i in 0..input_processors_len {
            if !input_selection_handler.is_input_finished(i)
                && input_selection_handler.is_input_selected(i)
            {
                // Note: Future combination logic needs to be actually implemented here
                // Due to Rust's async model limitations, using a simplified implementation here
                futures.push(i);
            }
        }

        // Return a Future that waits for any one Future to complete
        Box::pin(async move {
            // Placeholder implementation: should actually wait for any input to become available
            // Using polling approach here
            loop {
                if availability_helper.is_available() {
                    break;
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            }
        })
    }
}

/// MultipleFuturesAvailabilityHelper - Multiple Future availability helper
/// 
/// Used to wait for any one of multiple Futures to become available
#[derive(Clone)]
pub struct MultipleFuturesAvailabilityHelper {
    num_inputs: usize,
    /// Whether any input is available
    any_available: Arc<std::sync::atomic::AtomicBool>,
}

impl MultipleFuturesAvailabilityHelper {
    /// Create new availability helper
    pub fn new(num_inputs: usize) -> Self {
        Self {
            num_inputs,
            any_available: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Reset to unavailable state
    pub fn reset_to_unavailable(&self) {
        self.any_available.store(false, std::sync::atomic::Ordering::Relaxed);
    }

    /// Combine Futures using anyOf logic
    /// 
    /// When any one Future completes, notify availability
    /// 
    /// Note: Due to Rust's async model limitations, using a simplified implementation here
    /// Actual implementation should wait for any Future to complete and set availability
    pub fn any_of(
        &self,
        _idx: usize,
        _availability_future: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
    ) {
        // Placeholder implementation
        // Actual implementation should:
        // 1. Store Future
        // 2. When Future completes, set any_available to true
        // 3. Notify waiting code
    }

    /// Set an input as available
    pub fn set_available(&self) {
        self.any_available.store(true, std::sync::atomic::Ordering::Relaxed);
    }

    /// Get combined availability Future
    pub fn get_available_future(&self) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        let any_available = self.any_available.clone();
        Box::pin(async move {
            // Wait for any input to become available
            while !any_available.load(std::sync::atomic::Ordering::Relaxed) {
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            }
        })
    }
}

impl AvailabilityProvider for MultipleFuturesAvailabilityHelper {
    fn is_available(&self) -> bool {
        self.any_available.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn get_available_future(&self) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        self.get_available_future()
    }
}

