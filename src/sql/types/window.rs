use std::time::Duration;

use datafusion::common::{Result, plan_err};
use datafusion::logical_expr::Expr;

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
            "hop" => {
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
            "tumble" => {
                if args.len() != 1 {
                    unreachable!("wrong number of arguments for tumble(), expect one");
                }
                let width = get_duration(&args[0])?;
                Ok(Some(WindowType::Tumbling { width }))
            }
            "session" => {
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
