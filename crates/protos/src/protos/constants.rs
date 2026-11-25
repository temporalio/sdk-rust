//! Contains various constants that are used by core when storing/serializing data

/// Used as `marker_name` field when recording patch markers
pub const PATCH_MARKER_NAME: &str = "core_patch";

/// Used as `marker_name` field when recording local activity markers
pub const LOCAL_ACTIVITY_MARKER_NAME: &str = "core_local_activity";

/// Used as `marker_name` field when recording the results of Stream appends
pub const STREAM_APPEND_MARKER_NAME: &str = "core_stream_append";

/// Used as `marker_name` field when recording the pages of Stream reads
pub const STREAM_READ_MARKER_NAME: &str = "core_stream_read";
