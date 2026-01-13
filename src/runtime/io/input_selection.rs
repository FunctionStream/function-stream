// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// InputSelection - Input selection
//
// Manages selection logic for multiple input streams

use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};

/// InputSelection - Input selection
///
/// Uses bitmask to manage selection state of multiple inputs
#[derive(Debug, Clone, Copy)]
#[derive(Default)]
pub struct InputSelection {
    /// Selected input mask (each bit represents an input)
    selected_inputs_mask: u64,
}

impl InputSelection {
    /// No available input
    pub const NONE_AVAILABLE: i32 = -1;

    /// Create new input selection
    pub fn new(selected_inputs_mask: u64) -> Self {
        Self {
            selected_inputs_mask,
        }
    }

    /// Create selection that selects all inputs
    pub fn all(num_inputs: usize) -> Self {
        if num_inputs >= 64 {
            panic!("Too many inputs, maximum 63 inputs supported");
        }
        let mask = (1u64 << num_inputs) - 1;
        Self {
            selected_inputs_mask: mask,
        }
    }

    /// Create selection that selects a single input
    pub fn single(input_index: usize) -> Self {
        if input_index >= 64 {
            panic!("Input index too large, maximum 63 supported");
        }
        Self {
            selected_inputs_mask: 1u64 << input_index,
        }
    }

    /// Get selected input mask
    pub fn selected_inputs_mask(&self) -> u64 {
        self.selected_inputs_mask
    }

    /// Check if input is selected
    pub fn is_input_selected(&self, input_index: usize) -> bool {
        if input_index >= 64 {
            return false;
        }
        (self.selected_inputs_mask & (1u64 << input_index)) != 0
    }

    /// Fairly select next input index
    ///
    /// Select next from available inputs to avoid input stream starvation
    ///
    /// # Arguments
    /// - `selected_mask`: Selected input mask
    /// - `available_mask`: Available input mask
    /// - `last_read_index`: Last read input index
    ///
    /// # Returns
    /// - Next input index, or `NONE_AVAILABLE` if no available input
    pub fn fair_select_next_index(
        selected_mask: u64,
        available_mask: u64,
        last_read_index: i32,
    ) -> i32 {
        let candidates = selected_mask & available_mask;

        if candidates == 0 {
            return Self::NONE_AVAILABLE;
        }

        // Start searching from after the last read index
        let start_index = if last_read_index >= 0 {
            (last_read_index + 1) as usize
        } else {
            0
        };

        // Search from start_index
        for i in start_index..64 {
            if (candidates & (1u64 << i)) != 0 {
                return i as i32;
            }
        }

        // If not found, search from the beginning
        for i in 0..start_index {
            if (candidates & (1u64 << i)) != 0 {
                return i as i32;
            }
        }

        Self::NONE_AVAILABLE
    }
}


/// InputSelectable - Selectable input interface
///
/// Allows operators to customize input selection logic
pub trait InputSelectable: Send + Sync {
    /// Get next input selection
    fn next_selection(&self) -> InputSelection;
}

/// Operating mode enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OperatingMode {
    /// No InputSelectable
    NoInputSelectable,
    /// InputSelectable present, no data inputs finished
    InputSelectablePresentNoDataInputsFinished,
    /// InputSelectable present, some data inputs finished
    InputSelectablePresentSomeDataInputsFinished,
    /// All data inputs finished
    AllDataInputsFinished,
}

/// MultipleInputSelectionHandler - Multiple input selection handler
///
/// Manages selection logic for multiple input streams, decides next input to process
///
/// Mainly used in StreamMultipleInputProcessor to select next available input index
pub struct MultipleInputSelectionHandler {
    /// Optional input selector
    input_selectable: Option<Box<dyn InputSelectable>>,
    /// Selected input mask
    selected_inputs_mask: AtomicU64,
    /// All selected input mask (for calculation)
    all_selected_mask: u64,
    /// Available input mask
    available_inputs_mask: AtomicU64,
    /// Unfinished input mask
    not_finished_inputs_mask: AtomicU64,
    /// Input mask for data finished but partition not finished
    data_finished_but_not_partition: AtomicU64,
    /// Operating mode
    operating_mode: AtomicU8, // Uses u8 to store OperatingMode
    /// Whether to drain on end of data
    drain_on_end_of_data: AtomicBool,
}

impl MultipleInputSelectionHandler {
    /// Maximum supported input count
    pub const MAX_SUPPORTED_INPUT_COUNT: usize = 63; // Long.SIZE - 1

    /// Check supported input count
    pub fn check_supported_input_count(
        input_count: usize,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        if input_count > Self::MAX_SUPPORTED_INPUT_COUNT {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Only up to {} inputs are supported at once, while encountered {}",
                    Self::MAX_SUPPORTED_INPUT_COUNT,
                    input_count
                ),
            )) as Box<dyn std::error::Error + Send>);
        }
        Ok(())
    }

    /// Create new multiple input selection handler
    pub fn new(
        input_selectable: Option<Box<dyn InputSelectable>>,
        input_count: usize,
    ) -> Result<Self, Box<dyn std::error::Error + Send>> {
        Self::check_supported_input_count(input_count)?;

        let all_selected_mask = (1u64 << input_count) - 1;

        let operating_mode = if input_selectable.is_some() {
            OperatingMode::InputSelectablePresentNoDataInputsFinished
        } else {
            OperatingMode::NoInputSelectable
        };

        Ok(Self {
            input_selectable,
            selected_inputs_mask: AtomicU64::new(
                InputSelection::all(input_count).selected_inputs_mask(),
            ),
            all_selected_mask,
            available_inputs_mask: AtomicU64::new(all_selected_mask),
            not_finished_inputs_mask: AtomicU64::new(all_selected_mask),
            data_finished_but_not_partition: AtomicU64::new(0),
            operating_mode: AtomicU8::new(operating_mode as u8),
            drain_on_end_of_data: AtomicBool::new(true),
        })
    }

    /// Get operating mode
    fn get_operating_mode(&self) -> OperatingMode {
        match self.operating_mode.load(Ordering::Relaxed) {
            0 => OperatingMode::NoInputSelectable,
            1 => OperatingMode::InputSelectablePresentNoDataInputsFinished,
            2 => OperatingMode::InputSelectablePresentSomeDataInputsFinished,
            3 => OperatingMode::AllDataInputsFinished,
            _ => OperatingMode::NoInputSelectable,
        }
    }

    /// Set operating mode
    fn set_operating_mode(&self, mode: OperatingMode) {
        self.operating_mode.store(mode as u8, Ordering::Relaxed);
    }

    /// Update status and select next input
    pub fn update_status_and_selection(
        &self,
        input_status: crate::runtime::io::DataInputStatus,
        input_index: usize,
    ) -> Result<crate::runtime::io::DataInputStatus, Box<dyn std::error::Error + Send>> {
        match input_status {
            crate::runtime::io::DataInputStatus::MoreAvailable => {
                self.next_selection();
                // Check status: available input mask should contain current input index
                if !self.check_bit_mask(
                    self.available_inputs_mask.load(Ordering::Relaxed),
                    input_index,
                ) {
                    return Err(Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Input {} should be available", input_index),
                    )) as Box<dyn std::error::Error + Send>);
                }
                Ok(crate::runtime::io::DataInputStatus::MoreAvailable)
            }
            crate::runtime::io::DataInputStatus::NothingAvailable => {
                self.unset_available(input_index);
                self.next_selection();
                self.calculate_overall_status(input_status)
            }
            crate::runtime::io::DataInputStatus::Stopped => {
                self.drain_on_end_of_data.store(false, Ordering::Relaxed);
                // fall through
                self.set_data_finished_but_not_partition(input_index);
                self.update_mode_on_end_of_data();
                self.next_selection();
                self.calculate_overall_status(crate::runtime::io::DataInputStatus::EndOfData)
            }
            crate::runtime::io::DataInputStatus::EndOfData => {
                self.set_data_finished_but_not_partition(input_index);
                self.update_mode_on_end_of_data();
                self.next_selection();
                self.calculate_overall_status(input_status)
            }
            crate::runtime::io::DataInputStatus::EndOfInput => {
                self.unset_data_finished_but_not_partition(input_index);
                self.unset_not_finished(input_index);
                self.next_selection();
                self.calculate_overall_status(input_status)
            }
            _ => {
                Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("Unsupported inputStatus: {:?}", input_status),
                )) as Box<dyn std::error::Error + Send>)
            }
        }
    }

    /// Update mode when data ends
    fn update_mode_on_end_of_data(&self) {
        let data_finished = self.data_finished_but_not_partition.load(Ordering::Relaxed);
        let not_finished = self.not_finished_inputs_mask.load(Ordering::Relaxed);

        let all_data_inputs_finished =
            ((data_finished | !not_finished) & self.all_selected_mask) == self.all_selected_mask;

        if all_data_inputs_finished {
            self.set_operating_mode(OperatingMode::AllDataInputsFinished);
        } else if self.get_operating_mode()
            == OperatingMode::InputSelectablePresentNoDataInputsFinished
        {
            self.set_operating_mode(OperatingMode::InputSelectablePresentSomeDataInputsFinished);
        }
    }

    /// Calculate overall status
    fn calculate_overall_status(
        &self,
        updated_status: crate::runtime::io::DataInputStatus,
    ) -> Result<crate::runtime::io::DataInputStatus, Box<dyn std::error::Error + Send>> {
        if self.are_all_inputs_finished() {
            return Ok(crate::runtime::io::DataInputStatus::EndOfInput);
        }

        if updated_status == crate::runtime::io::DataInputStatus::EndOfData
            && self.get_operating_mode() == OperatingMode::AllDataInputsFinished
        {
            return Ok(if self.drain_on_end_of_data.load(Ordering::Relaxed) {
                crate::runtime::io::DataInputStatus::EndOfData
            } else {
                crate::runtime::io::DataInputStatus::Stopped
            });
        }

        if self.is_any_input_available() {
            Ok(crate::runtime::io::DataInputStatus::MoreAvailable)
        } else {
            let selected = self.selected_inputs_mask.load(Ordering::Relaxed);
            let not_finished = self.not_finished_inputs_mask.load(Ordering::Relaxed);
            let selected_not_finished = selected & not_finished;

            if selected_not_finished == 0 {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Can not make a progress: all selected inputs are already finished",
                )) as Box<dyn std::error::Error + Send>);
            }

            Ok(crate::runtime::io::DataInputStatus::NothingAvailable)
        }
    }

    /// Select next input index (fair selection, avoid starvation)
    pub fn select_next_input_index(&self, last_read_index: i32) -> i32 {
        let selected = self.selected_inputs_mask.load(Ordering::Relaxed);
        let available = self.available_inputs_mask.load(Ordering::Relaxed);
        let not_finished = self.not_finished_inputs_mask.load(Ordering::Relaxed);

        InputSelection::fair_select_next_index(selected, available & not_finished, last_read_index)
    }

    /// Select first input index
    pub fn select_first_input_index(&self) -> i32 {
        self.select_next_input_index(-1)
    }

    /// Unset available input
    fn unset_available(&self, input_index: usize) {
        let current = self.available_inputs_mask.load(Ordering::Relaxed);
        let new = self.unset_bit_mask(current, input_index);
        self.available_inputs_mask.store(new, Ordering::Relaxed);
    }

    /// Set available input
    pub fn set_available(&self, input_index: usize) {
        let current = self.available_inputs_mask.load(Ordering::Relaxed);
        let new = self.set_bit_mask(current, input_index);
        self.available_inputs_mask.store(new, Ordering::Relaxed);
    }

    /// Set data finished but partition not finished
    fn set_data_finished_but_not_partition(&self, input_index: usize) {
        let current = self.data_finished_but_not_partition.load(Ordering::Relaxed);
        let new = self.set_bit_mask(current, input_index);
        self.data_finished_but_not_partition
            .store(new, Ordering::Relaxed);
    }

    /// Unset unfinished input
    fn unset_not_finished(&self, input_index: usize) {
        let current = self.not_finished_inputs_mask.load(Ordering::Relaxed);
        let new = self.unset_bit_mask(current, input_index);
        self.not_finished_inputs_mask.store(new, Ordering::Relaxed);
    }

    /// Check if any input is available
    pub fn is_any_input_available(&self) -> bool {
        let available = self.available_inputs_mask.load(Ordering::Relaxed);
        let not_finished = self.not_finished_inputs_mask.load(Ordering::Relaxed);
        let selected = self.selected_inputs_mask.load(Ordering::Relaxed);

        (selected & available & not_finished) != 0
    }

    /// Check if all inputs are finished
    pub fn are_all_inputs_finished(&self) -> bool {
        self.not_finished_inputs_mask.load(Ordering::Relaxed) == 0
    }

    /// Check if input is finished
    pub fn is_input_finished(&self, input_index: usize) -> bool {
        if input_index >= 64 {
            return true;
        }
        let mask = 1u64 << input_index;
        (self.not_finished_inputs_mask.load(Ordering::Relaxed) & mask) == 0
    }

    /// Check if input is selected
    pub fn is_input_selected(&self, input_index: usize) -> bool {
        if input_index >= 64 {
            return false;
        }
        let mask = 1u64 << input_index;
        (self.selected_inputs_mask.load(Ordering::Relaxed) & mask) != 0
    }

    /// Check if should set availability for another input
    ///
    /// If there are unset available inputs in selected inputs, return true
    pub fn should_set_available_for_another_input(&self) -> bool {
        let selected = self.selected_inputs_mask.load(Ordering::Relaxed);
        let available = self.available_inputs_mask.load(Ordering::Relaxed);

        (selected & self.all_selected_mask & !available) != 0
    }

    /// Set unavailable input
    pub fn set_unavailable_input(&self, input_index: usize) {
        self.unset_available(input_index);
    }

    /// Unset data finished but partition not finished
    fn unset_data_finished_but_not_partition(&self, input_index: usize) {
        let mask = !(1u64 << input_index);
        self.data_finished_but_not_partition
            .fetch_and(mask, Ordering::Relaxed);
    }

    /// Check if all data inputs are finished
    pub fn are_all_data_inputs_finished(&self) -> bool {
        self.get_operating_mode() == OperatingMode::AllDataInputsFinished
    }

    /// Set bit mask
    fn set_bit_mask(&self, mask: u64, input_index: usize) -> u64 {
        mask | (1u64 << input_index)
    }

    /// Unset bit mask
    fn unset_bit_mask(&self, mask: u64, input_index: usize) -> u64 {
        mask & !(1u64 << input_index)
    }

    /// Check bit mask
    fn check_bit_mask(&self, mask: u64, input_index: usize) -> bool {
        (mask & (1u64 << input_index)) != 0
    }

    /// Update selection (get from InputSelectable or use default)
    pub fn next_selection(&self) {
        match self.get_operating_mode() {
            OperatingMode::NoInputSelectable | OperatingMode::AllDataInputsFinished => {
                // Select all inputs
                self.selected_inputs_mask
                    .store(self.all_selected_mask, Ordering::Relaxed);
            }
            OperatingMode::InputSelectablePresentNoDataInputsFinished => {
                // Get selection from InputSelectable
                if let Some(ref selectable) = self.input_selectable {
                    let selection = selectable.next_selection();
                    self.selected_inputs_mask
                        .store(selection.selected_inputs_mask(), Ordering::Relaxed);
                }
            }
            OperatingMode::InputSelectablePresentSomeDataInputsFinished => {
                // Get selection from InputSelectable，but include inputs with finished data
                if let Some(ref selectable) = self.input_selectable {
                    let selection = selectable.next_selection();
                    let data_finished =
                        self.data_finished_but_not_partition.load(Ordering::Relaxed);
                    let mask =
                        (selection.selected_inputs_mask() | data_finished) & self.all_selected_mask;
                    self.selected_inputs_mask.store(mask, Ordering::Relaxed);
                }
            }
        }
    }
}
