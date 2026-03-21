use serde::Serialize;
use std::convert::TryFrom;

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
        match value.to_lowercase().as_str() {
            "year" => Ok(DatePart::Year),
            "month" => Ok(DatePart::Month),
            "week" => Ok(DatePart::Week),
            "day" => Ok(DatePart::Day),
            "hour" => Ok(DatePart::Hour),
            "minute" => Ok(DatePart::Minute),
            "second" => Ok(DatePart::Second),
            "millisecond" => Ok(DatePart::Millisecond),
            "microsecond" => Ok(DatePart::Microsecond),
            "nanosecond" => Ok(DatePart::Nanosecond),
            "dow" => Ok(DatePart::DayOfWeek),
            "doy" => Ok(DatePart::DayOfYear),
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
        match value.to_lowercase().as_str() {
            "year" => Ok(DateTruncPrecision::Year),
            "quarter" => Ok(DateTruncPrecision::Quarter),
            "month" => Ok(DateTruncPrecision::Month),
            "week" => Ok(DateTruncPrecision::Week),
            "day" => Ok(DateTruncPrecision::Day),
            "hour" => Ok(DateTruncPrecision::Hour),
            "minute" => Ok(DateTruncPrecision::Minute),
            "second" => Ok(DateTruncPrecision::Second),
            _ => Err(format!("'{value}' is not a valid DateTruncPrecision")),
        }
    }
}
