#[test]
fn workflows_procmacro_build_tests() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/workflows_trybuild/*_fail.rs");
}
