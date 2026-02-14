fn main() {
    // Tell cargo to link this as a dynamic library (extension)
    // PostgreSQL symbols will be resolved at load time by the postgres process
    println!("cargo:rustc-link-arg=-undefined");
    println!("cargo:rustc-link-arg=dynamic_lookup");
}
