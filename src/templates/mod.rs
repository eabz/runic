use tera::{Context, Tera};

use crate::errors::ScaffoldError;

pub mod cargo;
pub mod config;
pub mod indexer;
pub mod lib;
pub mod rpc;

fn render_template(
    template: &str,
    context: &Context,
) -> Result<String, ScaffoldError> {
    Tera::one_off(template, context, false).map_err(ScaffoldError::from)
}
