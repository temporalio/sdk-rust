#[test]
fn typed_signal_with_start_build_tests() {
    let tests = trybuild::TestCases::new();
    tests.compile_fail("tests/typed_signal_with_start/*_fail.rs");
}
