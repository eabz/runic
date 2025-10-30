use tera::{Context, Tera};

use crate::errors::RunicError;

pub mod cargo;
pub mod config;
pub mod indexer;
pub mod lib;
pub mod rpc;

fn render_template(
    template: &str,
    context: &Context,
) -> Result<String, RunicError> {
    Tera::one_off(template, context, false).map_err(RunicError::from)
}
