use clap::Parser;
use runic::{
    config::{API, Database},
    scaffold::{ScaffoldSettings, run},
};
use simple_logger::SimpleLogger;
use std::process;

#[derive(Parser)]
#[command(
    name = "runic",
    author,
    version,
    about = "Scaffold an indexer project from a contract ABI.",
    long_about = "Generate boilerplate code and configuration for a Runic indexer using the supplied ABI, API surface, and database backend.",
    arg_required_else_help = true
)]
struct RunicArgs {
    /// Database backend to generate bindings for (defaults to redb).
    #[arg(long, value_enum, default_value_t = Database::Redb)]
    db: Database,

    /// Path or identifier for the ABI to scaffold against.
    #[arg(long)]
    abi: String,

    /// API surface to expose from the generated indexer (defaults to graphql).
    #[arg(long, value_enum, default_value_t = API::Graphql)]
    api: API,

    /// Block number to start indexing from. Defaults to 0.
    #[arg(long, default_value_t = 0)]
    start_block: i64,
}

fn main() {
    let log = SimpleLogger::new();

    if let Err(err) = log.init() {
        eprintln!("Logger setup failed: {err}");
    }

    let args = RunicArgs::try_parse().unwrap_or_else(|err| {
        err.print().expect("failed to write clap error");

        if err.use_stderr() {
            eprintln!("Use `runic --help` to review the available options and examples.");
        }

        process::exit(err.exit_code());
    });

    let settings = ScaffoldSettings {
        abi: args.abi.clone(),
        api: args.api,
        db: args.db,
        start_block: args.start_block,
    };

    if let Err(err) = run(settings) {
        eprintln!("Scaffolding failed: {err}");
        process::exit(1);
    }
}
