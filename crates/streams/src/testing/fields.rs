//! The fields of the `:meta` hash. A Producer field is `p|<kind>|<producer id>`. The id is opaque
//! and ends the name, so a `|` in it makes no other field.

/// The format version of the Stream.
pub const VERSION: &str = "v";

/// The field of a Producer. Its value is 88 bytes, with big-endian numbers:
///
/// - 40, the fence, the Producer's newest epoch key. It is also the epoch of the last batch.
/// - 8, the last batch's sequence.
/// - 12 and 12, the last batch's first and last entry ids, each the ms in 8 bytes and the seq in
///   4. They are zero when the batch wrote no entry.
/// - 16, the last batch's digest, which a resend must match.
///
/// A Store refuses a value of another length.
pub fn producer(kind: i32, producer_id: &str) -> String {
    format!("p|{kind}|{producer_id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_fields_match_the_shared_vectors() {
        let case: serde_json::Value =
            serde_json::from_str(include_str!("../../testdata/fields.json")).unwrap();
        let text = |field: &str| case[field].as_str().unwrap();
        let kind = case["kind"].as_i64().unwrap() as i32;
        assert_eq!(producer(kind, text("producer_id")), text("producer"));
    }
}
