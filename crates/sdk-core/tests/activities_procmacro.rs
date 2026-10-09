#[test]
fn activities_procmacro_build_tests() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/activities_trybuild/*_fail.rs");
}
