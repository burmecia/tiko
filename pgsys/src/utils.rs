// pgsys/src/utils.rs
//! PostgreSQL utility functions

use std::ffi::c_char;

/// Copy a Rust string to a C char array with null termination
pub fn copy_str_to_c(dst: &mut [c_char], src: &str) {
    let c = std::ffi::CString::new(src).expect("String contains interior NULs");
    let bytes = c.as_bytes_with_nul(); // includes trailing '\0'
    let max = dst.len();
    let n = bytes.len().min(max);

    // zero the destination (ensures null-termination even if truncated)
    for d in dst.iter_mut() {
        *d = 0;
    }

    // copy bytes (including '\0' if fits)
    for (i, &b) in bytes.iter().take(n).enumerate() {
        dst[i] = b as c_char;
    }
}
