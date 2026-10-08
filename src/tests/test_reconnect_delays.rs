//! The reconnect math (REC-01, REC-02) and the random values: full jitter per
//! retry index, the replay lookback and the jitter source.

use std::time::Duration;

use crate::random::random_fraction;
use crate::reconnect::{replay_lookback, retry_delay};

/// The largest `f64` below 1.
const JUST_BELOW_ONE: f64 = 1.0 - f64::EPSILON / 2.0;

const CEILINGS_MILLISECONDS: [(u32, u64); 9] = [
    (0, 500),
    (1, 1_000),
    (2, 2_000),
    (3, 4_000),
    (4, 8_000),
    (5, 16_000),
    (6, 30_000),
    (7, 30_000),
    (20, 30_000),
];

#[test]
fn half_jitter_gives_half_of_each_ceiling() {
    for (retry_index, ceiling) in CEILINGS_MILLISECONDS {
        assert_eq!(
            retry_delay(retry_index, 0.5),
            Duration::from_millis(ceiling / 2),
            "retry {retry_index}"
        );
    }
} // end fn half_jitter_gives_half_of_each_ceiling

#[test]
fn zero_jitter_gives_no_delay() {
    for (retry_index, _) in CEILINGS_MILLISECONDS {
        assert_eq!(retry_delay(retry_index, 0.0), Duration::ZERO);
    }
} // end fn zero_jitter_gives_no_delay

#[test]
fn jitter_near_one_gives_nearly_the_whole_ceiling() {
    for (retry_index, ceiling) in CEILINGS_MILLISECONDS {
        // 0.999999 of the ceiling, in whole nanoseconds.
        assert_eq!(
            retry_delay(retry_index, 0.999_999),
            Duration::from_nanos(ceiling * 999_999),
            "retry {retry_index}"
        );

        // The largest jitter is less than half a nanosecond below the
        // ceiling, and a Duration rounds to the nearest nanosecond.
        assert_eq!(
            retry_delay(retry_index, JUST_BELOW_ONE),
            Duration::from_millis(ceiling),
            "retry {retry_index}"
        );
    }
} // end fn jitter_near_one_gives_nearly_the_whole_ceiling

#[test]
fn very_large_retry_indexes_stay_capped() {
    for retry_index in [31, 32, 33, 64, u32::MAX] {
        assert_eq!(
            retry_delay(retry_index, 0.5),
            Duration::from_secs(15),
            "retry {retry_index}"
        );
    }
} // end fn very_large_retry_indexes_stay_capped

#[test]
fn the_lookback_rounds_up_to_whole_milliseconds_and_adds_five_seconds() {
    let cases = [
        (Duration::ZERO, 5_000),
        (Duration::from_nanos(1), 5_001),
        (Duration::from_nanos(999_999), 5_001),
        (Duration::from_millis(1), 5_001),
        (Duration::from_nanos(1_000_001), 5_002),
        (Duration::from_secs(10), 15_000),
        (Duration::from_secs(3_600), 3_605_000),
    ];

    for (outage, expected) in cases {
        assert_eq!(
            replay_lookback(outage),
            Duration::from_millis(expected),
            "{outage:?}"
        );
    }
} // end fn the_lookback_rounds_up_to_whole_milliseconds_and_adds_five_seconds

#[test]
fn the_lookback_is_capped_at_4294967295_milliseconds() {
    let cap = Duration::from_millis(4_294_967_295);

    // One millisecond below the cap, at the cap, and one step past it.
    assert_eq!(
        replay_lookback(Duration::from_millis(4_294_962_294)),
        Duration::from_millis(4_294_967_294)
    );
    assert_eq!(replay_lookback(Duration::from_millis(4_294_962_295)), cap);

    assert_eq!(
        replay_lookback(Duration::from_millis(4_294_962_295) + Duration::from_nanos(1)),
        cap
    );

    assert_eq!(replay_lookback(Duration::MAX), cap);
} // end fn the_lookback_is_capped_at_4294967295_milliseconds

#[test]
fn random_fractions_are_in_the_unit_interval() {
    let fractions: Vec<f64> = (0..1_000).map(|_| random_fraction()).collect();

    assert!(
        fractions
            .iter()
            .all(|fraction| (0.0..1.0).contains(fraction))
    );

    // Two equal draws in a row from 53 random bits do not occur in practice.
    assert!(fractions.windows(2).any(|pair| pair[0] != pair[1]));
    assert!(fractions.iter().any(|fraction| *fraction < 0.5));
    assert!(fractions.iter().any(|fraction| *fraction >= 0.5));
} // end fn random_fractions_are_in_the_unit_interval
