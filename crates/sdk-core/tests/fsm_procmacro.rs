#[test]
fn fsm_procmacro_build_tests() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/fsm_trybuild/*_fail.rs");
}
