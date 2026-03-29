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

use datafusion::common::{Result, plan_err};
use datafusion::logical_expr::Expr;

use crate::sql::common::constants::window_fn;

use super::DFField;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum WindowType {
    Tumbling { width: Duration },
    Sliding { width: Duration, slide: Duration },
    Session { gap: Duration },
    Instant,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum WindowBehavior {
    FromOperator {
        window: WindowType,
        window_field: DFField,
        window_index: usize,
        is_nested: bool,
    },
    InData,
}

pub fn get_duration(expression: &Expr) -> Result<Duration> {
    use datafusion::common::ScalarValue;

    match expression {
        Expr::Literal(ScalarValue::IntervalDayTime(Some(val)), _) => {
            Ok(Duration::from_secs((val.days as u64) * 24 * 60 * 60)
                + Duration::from_millis(val.milliseconds as u64))
        }
        Expr::Literal(ScalarValue::IntervalMonthDayNano(Some(val)), _) => {
            if val.months != 0 {
                return datafusion::common::not_impl_err!(
                    "Windows do not support durations specified as months"
                );
            }
            Ok(Duration::from_secs((val.days as u64) * 24 * 60 * 60)
                + Duration::from_nanos(val.nanoseconds as u64))
        }
        _ => plan_err!(
            "unsupported Duration expression, expect duration literal, not {}",
            expression
        ),
    }
}

pub fn find_window(expression: &Expr) -> Result<Option<WindowType>> {
    use datafusion::logical_expr::expr::Alias;
    use datafusion::logical_expr::expr::ScalarFunction;

    match expression {
        Expr::ScalarFunction(ScalarFunction { func: fun, args }) => match fun.name() {
            name if name == window_fn::HOP => {
                if args.len() != 2 {
                    unreachable!();
                }
                let slide = get_duration(&args[0])?;
                let width = get_duration(&args[1])?;
                if width.as_nanos() % slide.as_nanos() != 0 {
                    return plan_err!(
                        "hop() width {:?} must be a multiple of slide {:?}",
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
                    unreachable!("wrong number of arguments for tumble(), expect one");
                }
                let width = get_duration(&args[0])?;
                Ok(Some(WindowType::Tumbling { width }))
            }
            name if name == window_fn::SESSION => {
                if args.len() != 1 {
                    unreachable!("wrong number of arguments for session(), expected one");
                }
                let gap = get_duration(&args[0])?;
                Ok(Some(WindowType::Session { gap }))
            }
            _ => Ok(None),
        },
        Expr::Alias(Alias { expr, .. }) => find_window(expr),
        _ => Ok(None),
    }
}
