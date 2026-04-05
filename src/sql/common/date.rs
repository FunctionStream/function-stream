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

use serde::Serialize;
use std::convert::TryFrom;

use super::constants::{date_part_keyword, date_trunc_keyword};

#[derive(Debug, Clone, Copy, Eq, PartialEq, PartialOrd, Hash, Serialize)]
pub enum DatePart {
    Year,
    Month,
    Week,
    Day,
    Hour,
    Minute,
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
    DayOfWeek,
    DayOfYear,
}

impl TryFrom<&str> for DatePart {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let v = value.to_lowercase();
        match v.as_str() {
            date_part_keyword::YEAR => Ok(DatePart::Year),
            date_part_keyword::MONTH => Ok(DatePart::Month),
            date_part_keyword::WEEK => Ok(DatePart::Week),
            date_part_keyword::DAY => Ok(DatePart::Day),
            date_part_keyword::HOUR => Ok(DatePart::Hour),
            date_part_keyword::MINUTE => Ok(DatePart::Minute),
            date_part_keyword::SECOND => Ok(DatePart::Second),
            date_part_keyword::MILLISECOND => Ok(DatePart::Millisecond),
            date_part_keyword::MICROSECOND => Ok(DatePart::Microsecond),
            date_part_keyword::NANOSECOND => Ok(DatePart::Nanosecond),
            date_part_keyword::DOW => Ok(DatePart::DayOfWeek),
            date_part_keyword::DOY => Ok(DatePart::DayOfYear),
            _ => Err(format!("'{value}' is not a valid DatePart")),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, PartialOrd, Serialize)]
pub enum DateTruncPrecision {
    Year,
    Quarter,
    Month,
    Week,
    Day,
    Hour,
    Minute,
    Second,
}

impl TryFrom<&str> for DateTruncPrecision {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let v = value.to_lowercase();
        match v.as_str() {
            date_trunc_keyword::YEAR => Ok(DateTruncPrecision::Year),
            date_trunc_keyword::QUARTER => Ok(DateTruncPrecision::Quarter),
            date_trunc_keyword::MONTH => Ok(DateTruncPrecision::Month),
            date_trunc_keyword::WEEK => Ok(DateTruncPrecision::Week),
            date_trunc_keyword::DAY => Ok(DateTruncPrecision::Day),
            date_trunc_keyword::HOUR => Ok(DateTruncPrecision::Hour),
            date_trunc_keyword::MINUTE => Ok(DateTruncPrecision::Minute),
            date_trunc_keyword::SECOND => Ok(DateTruncPrecision::Second),
            _ => Err(format!("'{value}' is not a valid DateTruncPrecision")),
        }
    }
}
