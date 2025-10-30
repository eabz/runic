use std::{fmt, io};

#[derive(Debug)]
pub enum ScaffoldError {
    Io(io::Error),
    Serialization(toml::ser::Error),
    Template(tera::Error),
    Abi(String),
}

impl fmt::Display for ScaffoldError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ScaffoldError::Io(err) => write!(f, "io error: {err}"),
            ScaffoldError::Serialization(err) => {
                write!(f, "failed to serialize configuration: {err}")
            }
            ScaffoldError::Template(err) => {
                write!(f, "template rendering failed: {err}")
            }
            ScaffoldError::Abi(err) => {
                write!(f, "failed to generate ABI bindings: {err}")
            }
        }
    }
}

impl std::error::Error for ScaffoldError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ScaffoldError::Io(err) => Some(err),
            ScaffoldError::Serialization(err) => Some(err),
            ScaffoldError::Template(err) => Some(err),
            ScaffoldError::Abi(_err) => None,
        }
    }
}

impl From<io::Error> for ScaffoldError {
    fn from(err: io::Error) -> Self {
        ScaffoldError::Io(err)
    }
}

impl From<toml::ser::Error> for ScaffoldError {
    fn from(err: toml::ser::Error) -> Self {
        ScaffoldError::Serialization(err)
    }
}

impl From<tera::Error> for ScaffoldError {
    fn from(err: tera::Error) -> Self {
        ScaffoldError::Template(err)
    }
}

impl From<dialoguer::Error> for ScaffoldError {
    fn from(err: dialoguer::Error) -> Self {
        match err {
            dialoguer::Error::IO(inner) => ScaffoldError::Io(inner),
        }
    }
}
