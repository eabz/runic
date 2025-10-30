use std::{fs, path::Path};

use tera::Context;

use crate::{errors::RunicError, templates::render_template};

pub const RUNIC_LIB_TEMPLATE: &str = r#"pub mod config;
pub mod rpc;
"#;

pub fn write_runic_lib(project_root: &Path) -> Result<(), RunicError> {
    let bin_dir = project_root.join("src");
    let runic_lib_path = bin_dir.join("lib.rs");
    let context = Context::new();
    let runic_lib_contents =
        render_template(RUNIC_LIB_TEMPLATE, &context)?;

    fs::write(runic_lib_path, runic_lib_contents)?;
    Ok(())
}
