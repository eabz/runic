use std::{io, path::Path};

use ethers_contract_abigen::Abigen;

use crate::errors::ScaffoldError;

pub fn generate_abi_bindings(
    project_root: &Path,
    abi_source: &str,
) -> Result<(), ScaffoldError> {
    let abi_path = Path::new(abi_source);

    if !abi_path.exists() {
        return Err(ScaffoldError::Io(io::Error::new(
            io::ErrorKind::NotFound,
            format!("ABI file `{}` not found", abi_path.display()),
        )));
    }

    let source_str = abi_path
        .to_str()
        .ok_or_else(|| {
            ScaffoldError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "ABI path `{}` contains invalid UTF-8",
                    abi_path.display()
                ),
            ))
        })?
        .to_owned();

    let abigen = Abigen::new("abi", source_str)
        .map_err(|err| ScaffoldError::Abi(err.to_string()))?;

    let bindings = abigen
        .generate()
        .map_err(|err| ScaffoldError::Abi(err.to_string()))?;

    let target = project_root.join("src/abi.rs");
    bindings.write_to_file(&target)?;

    Ok(())
}
