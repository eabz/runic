use std::{fmt, io};

#[derive(Debug)]
pub enum RunicError {
    Io(io::Error),
    Serialization(toml::ser::Error),
    Template(tera::Error),
    Abi(String),
}

impl fmt::Display for RunicError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RunicError::Io(err) => write!(f, "io error: {err}"),
            RunicError::Serialization(err) => {
                write!(f, "failed to serialize configuration: {err}")
            }
            RunicError::Template(err) => {
                write!(f, "template rendering failed: {err}")
            }
            RunicError::Abi(err) => {
                write!(f, "failed to generate ABI bindings: {err}")
            }
        }
    }
}

impl std::error::Error for RunicError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            RunicError::Io(err) => Some(err),
            RunicError::Serialization(err) => Some(err),
            RunicError::Template(err) => Some(err),
            RunicError::Abi(_err) => None,
        }
    }
}

impl From<io::Error> for RunicError {
    fn from(err: io::Error) -> Self {
        RunicError::Io(err)
    }
}

impl From<toml::ser::Error> for RunicError {
    fn from(err: toml::ser::Error) -> Self {
        RunicError::Serialization(err)
    }
}

impl From<tera::Error> for RunicError {
    fn from(err: tera::Error) -> Self {
        RunicError::Template(err)
    }
}

impl From<dialoguer::Error> for RunicError {
    fn from(err: dialoguer::Error) -> Self {
        match err {
            dialoguer::Error::IO(inner) => RunicError::Io(inner),
        }
    }
}
