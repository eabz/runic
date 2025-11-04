use crate::errors::RunicError;
use alloy::json_abi::{Event, EventParam, JsonAbi};
use async_graphql_parser::parse_schema;
use ethers_core::abi::{ParamType, param_type::Reader};
use protoc_bin_vendored::protoc_bin_path;
use std::{
    collections::{BTreeSet, HashMap},
    fmt, fs, io,
    path::Path,
};
use tonic_build::configure;

pub struct ArtifactGenerator<'a> {
    primary_abi: &'a JsonAbi,
    child_abi: Option<&'a JsonAbi>,
}

impl<'a> ArtifactGenerator<'a> {
    pub fn new(
        primary_abi: &'a JsonAbi,
        child_abi: Option<&'a JsonAbi>,
    ) -> Self {
        Self { primary_abi, child_abi }
    }

    pub fn generate(&self) -> Result<GeneratedArtifacts, GeneratorError> {
        let events = self.collect_events()?;

        if events.is_empty() {
            return Err(GeneratorError::Abi(
                "ABI did not expose any events to generate artifacts from."
                    .to_owned(),
            ));
        }

        let mut postgres_migrations = Vec::new();
        let mut sqlite_migrations = Vec::new();
        let mut graphql_types = Vec::new();
        let mut graphql_query_fields = Vec::new();
        let mut graphql_scalars = BTreeSet::new();
        let mut grpc_messages = Vec::new();
        let mut grpc_envelope_variants = Vec::new();
        let mut rust_structs = Vec::new();
        let mut rust_uses_primitives = BTreeSet::new();
        let mut needs_json_value = false;
        let mut diesel_tables = Vec::new();

        graphql_scalars.insert(GraphqlScalar::BigInt);
        graphql_scalars.insert(GraphqlScalar::Bytes);

        for event in events {
            let struct_name = event.struct_name();
            let table_name = event.table_name();
            let graphql_type_name = struct_name.clone();
            let graphql_field_name = event.graphql_field_name();
            let grpc_message_name = struct_name.clone();

            let mut pg_columns = vec![
                "id BIGSERIAL PRIMARY KEY".to_string(),
                "block_number BIGINT NOT NULL".to_string(),
                "transaction_hash TEXT NOT NULL".to_string(),
                "log_index INTEGER NOT NULL".to_string(),
            ];
            let mut sqlite_columns = vec![
                "id INTEGER PRIMARY KEY AUTOINCREMENT".to_string(),
                "block_number INTEGER NOT NULL".to_string(),
                "transaction_hash TEXT NOT NULL".to_string(),
                "log_index INTEGER NOT NULL".to_string(),
            ];

            let mut graphql_fields = vec![
                "    id: ID!".to_string(),
                "    blockNumber: BigInt!".to_string(),
                "    transactionHash: Bytes!".to_string(),
                "    logIndex: Int!".to_string(),
            ];

            let mut grpc_fields = vec![
                "    uint64 block_number = 1;".to_string(),
                "    string transaction_hash = 2;".to_string(),
                "    uint32 log_index = 3;".to_string(),
            ];

            let mut rust_fields = vec![
                "    pub id: i64,".to_string(),
                "    pub block_number: i64,".to_string(),
                "    pub transaction_hash: String,".to_string(),
                "    pub log_index: i32,".to_string(),
            ];

            let mut rust_new_fields = vec![
                "    pub block_number: i64,".to_string(),
                "    pub transaction_hash: String,".to_string(),
                "    pub log_index: i32,".to_string(),
            ];

            let mut diesel_columns = vec![
                DieselColumn { name: "id".to_string(), ty: "BigInt" },
                DieselColumn {
                    name: "block_number".to_string(),
                    ty: "BigInt",
                },
                DieselColumn {
                    name: "transaction_hash".to_string(),
                    ty: "Text",
                },
                DieselColumn {
                    name: "log_index".to_string(),
                    ty: "Integer",
                },
            ];

            let mut grpc_field_index = 4;

            for param in &event.params {
                let mapping = map_param_type(&param.param_type);

                for scalar in &mapping.graphql_scalars {
                    graphql_scalars.insert(*scalar);
                }

                for primitive in &mapping.rust_primitives {
                    rust_uses_primitives.insert(*primitive);
                }

                if mapping.uses_json_value {
                    needs_json_value = true;
                }

                pg_columns.push(format!(
                    "{} {} NOT NULL",
                    param.column_name, mapping.sql_postgres
                ));
                sqlite_columns.push(format!(
                    "{} {} NOT NULL",
                    param.column_name, mapping.sql_sqlite
                ));

                diesel_columns.push(DieselColumn {
                    name: param.column_name.clone(),
                    ty: mapping.diesel_type,
                });

                graphql_fields.push(format!(
                    "    {}: {}",
                    param.graphql_name, mapping.graphql_type
                ));

                let grpc_prefix =
                    if mapping.grpc_repeated { "repeated " } else { "" };
                grpc_fields.push(format!(
                    "    {grpc_prefix}{} {} = {};",
                    mapping.grpc_type, param.column_name, grpc_field_index
                ));
                grpc_field_index += 1;

                rust_fields.push(format!(
                    "    pub {}: {},",
                    param.column_name, mapping.rust_type
                ));
                rust_new_fields.push(format!(
                    "    pub {}: {},",
                    param.column_name, mapping.rust_type
                ));
            }

            postgres_migrations.push(format!(
                "CREATE TABLE IF NOT EXISTS {table_name} (\n    {}\n);\nCREATE INDEX IF NOT EXISTS {table_name}_block_number_idx ON {table_name} (block_number);",
                pg_columns.join(",\n    ")
            ));

            sqlite_migrations.push(format!(
                "CREATE TABLE IF NOT EXISTS {table_name} (\n    {}\n);\nCREATE INDEX IF NOT EXISTS {table_name}_block_number_idx ON {table_name} (block_number);",
                sqlite_columns.join(",\n    ")
            ));

            graphql_types.push(format!(
                "type {graphql_type_name} {{\n{}\n}}",
                graphql_fields.join("\n")
            ));
            graphql_query_fields.push(format!(
                "    {}: [{}!]!",
                graphql_field_name, graphql_type_name
            ));

            grpc_messages.push(format!(
                "message {grpc_message_name} {{\n{}\n}}",
                grpc_fields.join("\n")
            ));
            grpc_envelope_variants.push(format!(
                "        {struct_name} {snake}_event = {idx};",
                struct_name = struct_name,
                snake = to_snake_case(&struct_name),
                idx = grpc_envelope_variants.len() + 2
            ));

            rust_structs.push(format!(
                "#[derive(Debug, Clone, Serialize, Deserialize, Identifiable, Queryable)]\n#[diesel(table_name = {table_name})]\npub struct {struct_name} {{\n{}\n}}",
                rust_fields.join("\n")
            ));
            rust_structs.push(format!(
                "#[derive(Debug, Clone, Serialize, Deserialize, Insertable)]\n#[diesel(table_name = {table_name})]\npub struct New{struct_name} {{\n{}\n}}",
                rust_new_fields.join("\n")
            ));

            diesel_tables.push(DieselTable {
                name: table_name.clone(),
                columns: diesel_columns,
            });
        }

        let graphql_schema = build_graphql_schema(
            graphql_scalars,
            graphql_types,
            graphql_query_fields,
        );
        let grpc_proto =
            build_grpc_proto(grpc_messages, grpc_envelope_variants);
        let rust_models = build_rust_models(
            rust_structs,
            rust_uses_primitives,
            needs_json_value,
        );
        let diesel_schema = build_diesel_schema(diesel_tables);

        Ok(GeneratedArtifacts {
            sql: SqlArtifacts {
                postgres: postgres_migrations.join("\n\n"),
                sqlite: sqlite_migrations.join("\n\n"),
            },
            graphql_schema,
            grpc_proto,
            rust_models,
            diesel_schema,
        })
    }

    pub fn write_to_disk(
        &self,
        base_path: &Path,
        artifacts: &GeneratedArtifacts,
    ) -> Result<(), GeneratorError> {
        let migrations_dir = base_path.join("migrations");
        let db_dir = base_path.join("src").join("db");
        let api_models_dir =
            base_path.join("src").join("api").join("models");

        fs::create_dir_all(&migrations_dir)?;
        fs::create_dir_all(&db_dir)?;
        fs::create_dir_all(&api_models_dir)?;

        fs::write(
            migrations_dir.join("postgres.sql"),
            &artifacts.sql.postgres,
        )?;
        fs::write(
            migrations_dir.join("sqlite.sql"),
            &artifacts.sql.sqlite,
        )?;
        fs::write(
            api_models_dir.join("schema.graphql"),
            &artifacts.graphql_schema,
        )?;
        fs::write(
            api_models_dir.join("indexer.proto"),
            &artifacts.grpc_proto,
        )?;
        fs::write(db_dir.join("models.rs"), &artifacts.rust_models)?;
        fs::write(db_dir.join("schema.rs"), &artifacts.diesel_schema)?;

        parse_schema(&artifacts.graphql_schema).map_err(|err| {
            GeneratorError::Graphql(format!(
                "Failed to validate GraphQL schema: {err}"
            ))
        })?;

        let proto_path = api_models_dir.join("indexer.proto");
        let protoc = protoc_bin_path().map_err(|err| {
            GeneratorError::Grpc(format!(
                "Failed to locate bundled protoc: {err}"
            ))
        })?;
        let protoc_include =
            protoc_bin_vendored::include_path().map_err(|err| {
                GeneratorError::Grpc(format!(
                    "Failed to locate bundled protobuf includes: {err}"
                ))
            })?;

        unsafe { std::env::set_var("PROTOC", &protoc) };
        unsafe { std::env::set_var("PROTOC_INCLUDE", &protoc_include) };

        configure()
            .build_client(true)
            .build_server(true)
            .out_dir(&api_models_dir)
            .compile(
                &[proto_path.to_string_lossy().to_string()],
                &[api_models_dir.clone(), protoc_include],
            )
            .map_err(|err| {
                GeneratorError::Grpc(format!(
                    "Failed to generate gRPC code: {err}"
                ))
            })?;

        let models_mod = r#"pub mod graphql {
    pub const SDL: &str = include_str!("schema.graphql");
}

pub mod grpc {
    include!("indexer.rs");
}
"#;

        fs::write(api_models_dir.join("mod.rs"), models_mod)?;

        Ok(())
    }

    fn collect_events(&self) -> Result<Vec<EventSpec>, GeneratorError> {
        let mut events = Vec::new();

        events.extend(collect_from_abi(
            self.primary_abi,
            EventSource::Primary,
        )?);

        if let Some(child) = self.child_abi {
            events.extend(collect_from_abi(child, EventSource::Child)?);
        }

        Ok(events)
    }
}

fn collect_from_abi(
    abi: &JsonAbi,
    source: EventSource,
) -> Result<Vec<EventSpec>, GeneratorError> {
    let mut specs = Vec::new();

    for (name, variants) in &abi.events {
        for (index, event) in variants.iter().enumerate() {
            let unique_name = if variants.len() > 1 {
                format!("{name}_{index}")
            } else {
                name.clone()
            };

            specs.push(EventSpec::from_event(unique_name, event, source)?);
        }
    }

    Ok(specs)
}

fn build_graphql_schema(
    scalars: BTreeSet<GraphqlScalar>,
    types: Vec<String>,
    query_fields: Vec<String>,
) -> String {
    let mut schema = String::new();
    schema.push_str("# Auto-generated GraphQL schema\n\n");

    for scalar in scalars {
        schema.push_str(scalar.declaration());
        schema.push('\n');
    }

    schema.push('\n');

    for ty in types {
        schema.push_str(&ty);
        schema.push_str("\n\n");
    }

    schema.push_str("type Query {\n");
    for field in query_fields {
        schema.push_str(&field);
        schema.push('\n');
    }
    schema.push_str("}\n");

    schema
}

fn build_grpc_proto(
    messages: Vec<String>,
    envelope_variants: Vec<String>,
) -> String {
    let mut proto = String::new();
    proto.push_str("// Auto-generated gRPC schema\n");
    proto.push_str("syntax = \"proto3\";\n\n");
    proto.push_str("package runic.indexer;\n\n");

    for message in messages {
        proto.push_str(&message);
        proto.push_str("\n\n");
    }

    proto.push_str("message EventEnvelope {\n");
    proto.push_str("    string name = 1;\n");
    proto.push_str("    oneof payload {\n");
    for variant in envelope_variants {
        proto.push_str("        ");
        proto.push_str(&variant);
        proto.push('\n');
    }
    proto.push_str("    }\n");
    proto.push_str("}\n\n");

    proto.push_str("message SubscriptionRequest {}\n\n");
    proto.push_str("service Indexer {\n");
    proto.push_str(
        "    rpc StreamEvents(SubscriptionRequest) returns (stream EventEnvelope);\n",
    );
    proto.push_str("}\n");

    proto
}

fn build_rust_models(
    structs: Vec<String>,
    primitive_imports: BTreeSet<RustPrimitive>,
    needs_json_value: bool,
) -> String {
    let mut output = String::new();
    output.push_str("// Auto-generated Rust models\n");
    output.push_str("use serde::{Deserialize, Serialize};\n");
    output.push_str("use diesel::prelude::*;\n");
    output.push_str("use super::schema::*;\n");

    if !primitive_imports.is_empty() {
        let mut primitives: Vec<&'static str> =
            primitive_imports.iter().map(|p| p.as_str()).collect();
        primitives.sort_unstable();
        output.push_str("use alloy::primitives::{");
        output.push_str(&primitives.join(", "));
        output.push_str("};\n");
    }

    if needs_json_value {
        output.push_str("use serde_json::Value;\n");
    }

    output.push('\n');

    for strukt in structs {
        output.push_str(&strukt);
        output.push('\n');
        output.push('\n');
    }

    output.trim_end().to_owned()
}

fn build_diesel_schema(tables: Vec<DieselTable>) -> String {
    let mut output = String::new();
    output.push_str("// Auto-generated Diesel schema\n");

    for table in tables {
        output.push_str("diesel::table! {\n");
        output.push_str("    use diesel::sql_types::*;\n");
        output.push_str(&format!("    {} (id) {{\n", table.name));
        for column in table.columns {
            output.push_str(&format!(
                "        {} -> {},\n",
                column.name, column.ty
            ));
        }
        output.push_str("    }\n}\n\n");
    }

    output.trim_end().to_owned()
}

#[derive(Debug)]
pub enum GeneratorError {
    Abi(String),
    Io(io::Error),
    Grpc(String),
    Graphql(String),
}

impl From<io::Error> for GeneratorError {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}

impl fmt::Display for GeneratorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GeneratorError::Abi(msg) => f.write_str(msg),
            GeneratorError::Io(err) => write!(f, "{err}"),
            GeneratorError::Grpc(msg) => f.write_str(msg),
            GeneratorError::Graphql(msg) => f.write_str(msg),
        }
    }
}

impl std::error::Error for GeneratorError {}

impl From<GeneratorError> for RunicError {
    fn from(err: GeneratorError) -> Self {
        match err {
            GeneratorError::Abi(msg) => RunicError::Abi(msg),
            GeneratorError::Io(err) => RunicError::Io(err),
            GeneratorError::Grpc(msg) => RunicError::Abi(msg),
            GeneratorError::Graphql(msg) => RunicError::Abi(msg),
        }
    }
}

#[derive(Debug)]
pub struct GeneratedArtifacts {
    pub sql: SqlArtifacts,
    pub graphql_schema: String,
    pub grpc_proto: String,
    pub rust_models: String,
    pub diesel_schema: String,
}

#[derive(Debug)]
pub struct SqlArtifacts {
    pub postgres: String,
    pub sqlite: String,
}

#[derive(Clone)]
struct TypeMapping {
    rust_type: String,
    rust_primitives: BTreeSet<RustPrimitive>,
    uses_json_value: bool,
    graphql_type: String,
    graphql_scalars: BTreeSet<GraphqlScalar>,
    grpc_type: String,
    grpc_repeated: bool,
    sql_postgres: String,
    sql_sqlite: String,
    diesel_type: &'static str,
}

struct DieselTable {
    name: String,
    columns: Vec<DieselColumn>,
}

struct DieselColumn {
    name: String,
    ty: &'static str,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum GraphqlScalar {
    Address,
    BigInt,
    Bytes,
    Json,
}

impl GraphqlScalar {
    fn declaration(&self) -> &'static str {
        match self {
            GraphqlScalar::Address => "scalar Address",
            GraphqlScalar::BigInt => "scalar BigInt",
            GraphqlScalar::Bytes => "scalar Bytes",
            GraphqlScalar::Json => "scalar JSON",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum RustPrimitive {
    Address,
    I256,
    U256,
}

impl RustPrimitive {
    fn as_str(&self) -> &'static str {
        match self {
            RustPrimitive::Address => "Address",
            RustPrimitive::I256 => "I256",
            RustPrimitive::U256 => "U256",
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum EventSource {
    Primary,
    Child,
}

impl EventSource {
    fn table_prefix(&self) -> &'static str {
        match self {
            EventSource::Primary => "events",
            EventSource::Child => "child_events",
        }
    }

    fn struct_prefix(&self) -> &'static str {
        match self {
            EventSource::Primary => "",
            EventSource::Child => "Child",
        }
    }

    fn field_prefix(&self) -> &'static str {
        match self {
            EventSource::Primary => "",
            EventSource::Child => "child",
        }
    }
}

struct EventSpec {
    name: String,
    source: EventSource,
    params: Vec<ParamSpec>,
}

impl EventSpec {
    fn from_event(
        name: String,
        event: &Event,
        source: EventSource,
    ) -> Result<Self, GeneratorError> {
        let params = build_param_specs(&event.inputs)?;
        Ok(Self { name, source, params })
    }

    fn struct_name(&self) -> String {
        format!(
            "{}{}Event",
            self.source.struct_prefix(),
            to_pascal_case(&self.name)
        )
    }

    fn table_name(&self) -> String {
        format!(
            "{}_{}",
            self.source.table_prefix(),
            to_snake_case(&self.name)
        )
    }

    fn graphql_field_name(&self) -> String {
        let prefix = self.source.field_prefix();
        let mut base = if prefix.is_empty() {
            to_camel_case(&self.name)
        } else {
            format!("{}{}", prefix, to_pascal_case(&self.name))
        };
        base.push_str("Events");
        make_lower_camel(&base)
    }
}

struct ParamSpec {
    column_name: String,
    graphql_name: String,
    param_type: ParamType,
}

fn build_param_specs(
    inputs: &[EventParam],
) -> Result<Vec<ParamSpec>, GeneratorError> {
    let mut used_names: HashMap<String, usize> = HashMap::new();
    let mut params = Vec::new();

    for (idx, input) in inputs.iter().enumerate() {
        let parsed = Reader::read(&input.ty)
            .map_err(|err| GeneratorError::Abi(err.to_string()))?;

        let raw_name = if input.name.trim().is_empty() {
            format!("param_{idx}")
        } else {
            input.name.clone()
        };

        let base_snake = to_snake_case(&raw_name);
        let entry = used_names.entry(base_snake.clone()).or_insert(0);
        let final_name = if *entry == 0 {
            base_snake.clone()
        } else {
            format!("{base_snake}_{}", entry)
        };
        *entry += 1;

        params.push(ParamSpec {
            column_name: final_name.clone(),
            graphql_name: to_camel_case(&final_name),
            param_type: parsed,
        });
    }

    Ok(params)
}

fn map_param_type(param: &ParamType) -> TypeMapping {
    match param {
        ParamType::Address => {
            let mut primitives = BTreeSet::new();
            primitives.insert(RustPrimitive::Address);
            let mut scalars = BTreeSet::new();
            scalars.insert(GraphqlScalar::Address);
            TypeMapping {
                rust_type: "Address".to_string(),
                rust_primitives: primitives,
                uses_json_value: false,
                graphql_type: "Address!".to_string(),
                graphql_scalars: scalars,
                grpc_type: "string".to_string(),
                grpc_repeated: false,
                sql_postgres: "TEXT".to_string(),
                sql_sqlite: "TEXT".to_string(),
                diesel_type: "Text",
            }
        }
        ParamType::Bool => TypeMapping {
            rust_type: "bool".to_string(),
            rust_primitives: BTreeSet::new(),
            uses_json_value: false,
            graphql_type: "Boolean!".to_string(),
            graphql_scalars: BTreeSet::new(),
            grpc_type: "bool".to_string(),
            grpc_repeated: false,
            sql_postgres: "BOOLEAN".to_string(),
            sql_sqlite: "INTEGER".to_string(),
            diesel_type: "Bool",
        },
        ParamType::Bytes | ParamType::FixedBytes(_) => {
            let mut scalars = BTreeSet::new();
            scalars.insert(GraphqlScalar::Bytes);
            TypeMapping {
                rust_type: "Vec<u8>".to_string(),
                rust_primitives: BTreeSet::new(),
                uses_json_value: false,
                graphql_type: "Bytes!".to_string(),
                graphql_scalars: scalars,
                grpc_type: "bytes".to_string(),
                grpc_repeated: false,
                sql_postgres: "BYTEA".to_string(),
                sql_sqlite: "BLOB".to_string(),
                diesel_type: "Binary",
            }
        }
        ParamType::String => TypeMapping {
            rust_type: "String".to_string(),
            rust_primitives: BTreeSet::new(),
            uses_json_value: false,
            graphql_type: "String!".to_string(),
            graphql_scalars: BTreeSet::new(),
            grpc_type: "string".to_string(),
            grpc_repeated: false,
            sql_postgres: "TEXT".to_string(),
            sql_sqlite: "TEXT".to_string(),
            diesel_type: "Text",
        },
        ParamType::Uint(size) => map_uint(*size),
        ParamType::Int(size) => map_int(*size),
        ParamType::Array(inner) | ParamType::FixedArray(inner, _) => {
            let inner_mapping = map_param_type(inner);
            let primitives = inner_mapping.rust_primitives.clone();
            let scalars = inner_mapping.graphql_scalars.clone();
            TypeMapping {
                rust_type: format!("Vec<{}>", inner_mapping.rust_type),
                rust_primitives: primitives,
                uses_json_value: inner_mapping.uses_json_value,
                graphql_type: make_graphql_list(
                    &inner_mapping.graphql_type,
                ),
                graphql_scalars: scalars,
                grpc_type: inner_mapping.grpc_type,
                grpc_repeated: true,
                sql_postgres: "JSONB".to_string(),
                sql_sqlite: "TEXT".to_string(),
                diesel_type: "Text",
            }
        }
        ParamType::Tuple(_) => {
            let mut scalars = BTreeSet::new();
            scalars.insert(GraphqlScalar::Json);
            TypeMapping {
                rust_type: "Value".to_string(),
                rust_primitives: BTreeSet::new(),
                uses_json_value: true,
                graphql_type: "JSON!".to_string(),
                graphql_scalars: scalars,
                grpc_type: "string".to_string(),
                grpc_repeated: false,
                sql_postgres: "JSONB".to_string(),
                sql_sqlite: "TEXT".to_string(),
                diesel_type: "Text",
            }
        }
    }
}

fn map_uint(size: usize) -> TypeMapping {
    if size <= 32 {
        TypeMapping {
            rust_type: "u32".to_string(),
            rust_primitives: BTreeSet::new(),
            uses_json_value: false,
            graphql_type: "Int!".to_string(),
            graphql_scalars: BTreeSet::new(),
            grpc_type: "uint32".to_string(),
            grpc_repeated: false,
            sql_postgres: "INTEGER".to_string(),
            sql_sqlite: "INTEGER".to_string(),
            diesel_type: "Integer",
        }
    } else if size <= 64 {
        let mut scalars = BTreeSet::new();
        scalars.insert(GraphqlScalar::BigInt);
        TypeMapping {
            rust_type: "u64".to_string(),
            rust_primitives: BTreeSet::new(),
            uses_json_value: false,
            graphql_type: "BigInt!".to_string(),
            graphql_scalars: scalars,
            grpc_type: "uint64".to_string(),
            grpc_repeated: false,
            sql_postgres: "BIGINT".to_string(),
            sql_sqlite: "INTEGER".to_string(),
            diesel_type: "BigInt",
        }
    } else {
        let mut primitives = BTreeSet::new();
        primitives.insert(RustPrimitive::U256);
        let mut scalars = BTreeSet::new();
        scalars.insert(GraphqlScalar::BigInt);
        TypeMapping {
            rust_type: "U256".to_string(),
            rust_primitives: primitives,
            uses_json_value: false,
            graphql_type: "BigInt!".to_string(),
            graphql_scalars: scalars,
            grpc_type: "string".to_string(),
            grpc_repeated: false,
            sql_postgres: "NUMERIC".to_string(),
            sql_sqlite: "TEXT".to_string(),
            diesel_type: "Numeric",
        }
    }
}

fn map_int(size: usize) -> TypeMapping {
    if size <= 32 {
        TypeMapping {
            rust_type: "i32".to_string(),
            rust_primitives: BTreeSet::new(),
            uses_json_value: false,
            graphql_type: "Int!".to_string(),
            graphql_scalars: BTreeSet::new(),
            grpc_type: "int32".to_string(),
            grpc_repeated: false,
            sql_postgres: "INTEGER".to_string(),
            sql_sqlite: "INTEGER".to_string(),
            diesel_type: "Integer",
        }
    } else if size <= 64 {
        let mut scalars = BTreeSet::new();
        scalars.insert(GraphqlScalar::BigInt);
        TypeMapping {
            rust_type: "i64".to_string(),
            rust_primitives: BTreeSet::new(),
            uses_json_value: false,
            graphql_type: "BigInt!".to_string(),
            graphql_scalars: scalars,
            grpc_type: "int64".to_string(),
            grpc_repeated: false,
            sql_postgres: "BIGINT".to_string(),
            sql_sqlite: "INTEGER".to_string(),
            diesel_type: "BigInt",
        }
    } else {
        let mut primitives = BTreeSet::new();
        primitives.insert(RustPrimitive::I256);
        let mut scalars = BTreeSet::new();
        scalars.insert(GraphqlScalar::BigInt);
        TypeMapping {
            rust_type: "I256".to_string(),
            rust_primitives: primitives,
            uses_json_value: false,
            graphql_type: "BigInt!".to_string(),
            graphql_scalars: scalars,
            grpc_type: "string".to_string(),
            grpc_repeated: false,
            sql_postgres: "NUMERIC".to_string(),
            sql_sqlite: "TEXT".to_string(),
            diesel_type: "Numeric",
        }
    }
}

fn make_graphql_list(inner: &str) -> String {
    let trimmed = inner.trim_end_matches('!');
    format!("[{trimmed}!]!")
}

fn to_snake_case(input: &str) -> String {
    let mut result = String::new();
    let mut prev_is_upper = false;
    for (idx, ch) in input.chars().enumerate() {
        if ch.is_ascii_uppercase() {
            if idx != 0 && !prev_is_upper {
                result.push('_');
            }
            result.push(ch.to_ascii_lowercase());
            prev_is_upper = true;
        } else if ch == '-' || ch == ' ' {
            if !result.ends_with('_') {
                result.push('_');
            }
            prev_is_upper = false;
        } else {
            result.push(ch);
            prev_is_upper = false;
        }
    }
    if result.is_empty() {
        return input.to_ascii_lowercase();
    }
    result
}

fn to_pascal_case(input: &str) -> String {
    let mut result = String::new();
    let mut capitalize_next = true;
    for ch in input.chars() {
        if ch == '_' || ch == '-' || ch == ' ' {
            capitalize_next = true;
            continue;
        }
        if capitalize_next {
            result.push(ch.to_ascii_uppercase());
            capitalize_next = false;
        } else {
            result.push(ch.to_ascii_lowercase());
        }
    }
    result
}

fn to_camel_case(input: &str) -> String {
    let pascal = to_pascal_case(input);
    make_lower_camel(&pascal)
}

fn make_lower_camel(input: &str) -> String {
    if input.is_empty() {
        return String::new();
    }
    let mut chars = input.chars();
    let first = chars.next().unwrap().to_ascii_lowercase();
    let mut result = String::with_capacity(input.len());
    result.push(first);
    for ch in chars {
        result.push(ch);
    }
    result
}
