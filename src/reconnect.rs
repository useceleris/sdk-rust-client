use std::time::Duration;

use crate::constants::{REPLAY_LOOKBACK_CAP, REPLAY_OVERLAP, RETRY_BASE_DELAY, RETRY_DELAY_CAP};

/// Full jitter: `random × min(30 s, 500 ms × 2^retry_index)`.
pub(crate) fn retry_delay(retry_index: u32, random: f64) -> Duration {
    let ceiling = RETRY_BASE_DELAY
        .checked_mul(2_u32.saturating_pow(retry_index))
        .map_or(RETRY_DELAY_CAP, |delay| delay.min(RETRY_DELAY_CAP));

    ceiling.mul_f64(random)
} // end fn retry_delay

/// The outage so far, rounded up to whole milliseconds, plus five seconds of
/// overlap, capped at the server's largest lookback.
pub(crate) fn replay_lookback(outage: Duration) -> Duration {
    let whole_milliseconds = outage.as_nanos().div_ceil(1_000_000);
    let rounded = Duration::from_millis(u64::try_from(whole_milliseconds).unwrap_or(u64::MAX));

    rounded
        .saturating_add(REPLAY_OVERLAP)
        .min(REPLAY_LOOKBACK_CAP)
} // end fn replay_lookback
