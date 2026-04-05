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

use std::time::Duration;

use datafusion::common::{Result, ScalarValue, not_impl_err, plan_err};
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::expr::{Alias, ScalarFunction};

use crate::sql::common::constants::window_fn;

use super::QualifiedField;

// ============================================================================
// Window Definitions
// ============================================================================

/// Temporal windowing semantics for streaming aggregations.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum WindowType {
    Tumbling { width: Duration },
    Sliding { width: Duration, slide: Duration },
    Session { gap: Duration },
    Instant,
}

/// How windowing is represented in the physical plan.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum WindowBehavior {
    FromOperator {
        window: WindowType,
        window_field: QualifiedField,
        window_index: usize,
        is_nested: bool,
    },
    InData,
}

// ============================================================================
// Logical Expression Parsers
// ============================================================================

pub fn extract_duration(expression: &Expr) -> Result<Duration> {
    match expression {
        Expr::Literal(ScalarValue::IntervalDayTime(Some(val)), _) => {
            let secs = (val.days as u64) * 24 * 60 * 60;
            let millis = val.milliseconds as u64;
            Ok(Duration::from_secs(secs) + Duration::from_millis(millis))
        }
        Expr::Literal(ScalarValue::IntervalMonthDayNano(Some(val)), _) => {
            if val.months != 0 {
                return not_impl_err!(
                    "Streaming engine does not support window durations specified in months due to variable month lengths."
                );
            }
            let secs = (val.days as u64) * 24 * 60 * 60;
            let nanos = val.nanoseconds as u64;
            Ok(Duration::from_secs(secs) + Duration::from_nanos(nanos))
        }
        _ => plan_err!(
            "Unsupported window duration expression. Expected an interval literal (e.g., INTERVAL '1' MINUTE), got: {}",
            expression
        ),
    }
}

pub fn extract_window_type(expression: &Expr) -> Result<Option<WindowType>> {
    match expression {
        Expr::ScalarFunction(ScalarFunction { func, args }) => match func.name() {
            name if name == window_fn::HOP => {
                if args.len() != 2 {
                    return plan_err!(
                        "hop() window function expects exactly 2 arguments (slide, width), got {}",
                        args.len()
                    );
                }

                let slide = extract_duration(&args[0])?;
                let width = extract_duration(&args[1])?;

                if width.as_nanos() % slide.as_nanos() != 0 {
                    return plan_err!(
                        "Streaming Topology Error: hop() window width {:?} must be a perfect multiple of slide {:?}",
                        width,
                        slide
                    );
                }

                if slide == width {
                    Ok(Some(WindowType::Tumbling { width }))
                } else {
                    Ok(Some(WindowType::Sliding { width, slide }))
                }
            }

            name if name == window_fn::TUMBLE => {
                if args.len() != 1 {
                    return plan_err!(
                        "tumble() window function expects exactly 1 argument (width), got {}",
                        args.len()
                    );
                }
                let width = extract_duration(&args[0])?;
                Ok(Some(WindowType::Tumbling { width }))
            }

            name if name == window_fn::SESSION => {
                if args.len() != 1 {
                    return plan_err!(
                        "session() window function expects exactly 1 argument (gap), got {}",
                        args.len()
                    );
                }
                let gap = extract_duration(&args[0])?;
                Ok(Some(WindowType::Session { gap }))
            }

            _ => Ok(None),
        },

        Expr::Alias(Alias { expr, .. }) => extract_window_type(expr),

        _ => Ok(None),
    }
}
