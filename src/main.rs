use std::{env, io::Write, time::Duration};

use chrono::{DateTime, Datelike, FixedOffset, NaiveDateTime, NaiveTime, Timelike};
use nu_plugin::{
    serve_plugin, EngineInterface, EvaluatedCall, MsgPackSerializer, PluginCommand,
    SimplePluginCommand,
};
use nu_protocol::{LabeledError, Record, ShellError, Signature, Span, SyntaxShape, Value};
use pg_query::NodeEnum;
use postgres::{
    fallible_iterator::FallibleIterator,
    types::{FromSql, Oid, ToSql, Type},
    Client,
};
use rustls::RootCertStore;
use tokio_postgres_rustls::MakeRustlsConnect;

fn main() {
    serve_plugin(&PgPlugin::new(), MsgPackSerializer);
}

struct PgPlugin {}

impl PgPlugin {
    pub fn new() -> Self {
        Self {}
    }
}

impl nu_plugin::Plugin for PgPlugin {
    fn version(&self) -> String {
        env!("CARGO_PKG_VERSION").into()
    }

    fn commands(&self) -> Vec<Box<dyn PluginCommand<Plugin = Self>>> {
        vec![Box::new(PgCommand)]
    }
}

struct PgCommand;

impl SimplePluginCommand for PgCommand {
    type Plugin = PgPlugin;

    fn name(&self) -> &str {
        "pg"
    }

    fn signature(&self) -> Signature {
        Signature::new("pg")
            .add_help()
            .required("query", SyntaxShape::String, "query to execute")
    }

    fn usage(&self) -> &str {
        ""
    }

    fn run(
        &self,
        _plugin: &PgPlugin,
        engine: &EngineInterface,
        call: &EvaluatedCall,
        input: &Value,
    ) -> Result<Value, LabeledError> {
        let args = Args::parse(call)?;
        let mut config = load_config(engine)?;

        let input = match input {
            Value::Nothing { .. } => b"",
            Value::String { val, .. } => val.as_bytes(),
            Value::Binary { val, .. } => val.as_slice(),
            _ => {
                return Err(LabeledError::new(format!(
                    "expected `string` or `binary` input, but got `{}`",
                    input.get_type(),
                )))
            }
        };

        config.connect_timeout(Duration::from_secs(30));

        let mut client = config.connect(tls_connector()).map_err(from_pg_error)?;

        let mut output_values = Vec::new();

        let parse_result =
            pg_query::parse(&args.query).map_err(|err| LabeledError::new(err.to_string()))?;

        for stmt in &parse_result.protobuf.stmts {
            let stmt = stmt.stmt.as_ref().unwrap();
            let node = stmt.node.as_ref().unwrap();
            let query = stmt
                .deparse()
                .map_err(|err| LabeledError::new(err.to_string()))?;

            match node {
                NodeEnum::SelectStmt(_) => {
                    let value = execute_query(&mut client, &query)?;

                    output_values.push(value);
                }
                NodeEnum::CopyStmt(stmt) => {
                    if !stmt.is_from && !stmt.is_program {
                        return Err(LabeledError::new("`COPY … TO STDOUT` is not supported"));
                    }

                    if !stmt.is_from || stmt.is_program {
                        execute_query(&mut client, &query)?;
                        continue;
                    }

                    let mut writer = client
                        .copy_in(&query)
                        .map_err(|err| LabeledError::new(err.to_string()))?;

                    writer
                        .write_all(input)
                        .map_err(|err| LabeledError::new(err.to_string()))?;

                    writer
                        .finish()
                        .map_err(|err| LabeledError::new(err.to_string()))?;
                }
                _ => {
                    execute_query(&mut client, &query)?;
                }
            }
        }

        // execute_query(&mut client, &args.query)

        if output_values.len() == 1 {
            Ok(output_values.into_iter().next().unwrap())
        } else {
            Ok(Value::list(output_values, Span::unknown()))
        }
    }
}

fn execute_query(client: &mut Client, query: &str) -> Result<Value, LabeledError> {
    let params: [&dyn ToSql; 0] = [];
    let mut rows = client.query_raw(query, params).map_err(from_pg_error)?;

    let mut nu_rows = Vec::new();

    while let Some(row) = rows.next().transpose() {
        let row = &row.map_err(from_pg_error)?;
        let mut nu_row = Record::with_capacity(row.len());

        let span = Span::unknown();

        for (i, col) in row.columns().iter().enumerate() {
            let value = match *col.type_() {
                Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => {
                    row_get_opt(row, i, |value: String| Value::string(value, span))
                }
                Type::BOOL => row_get_opt(row, i, |value: bool| Value::bool(value, span)),
                Type::CHAR => row_get_opt(row, i, |value: i8| Value::int(value.into(), span)),
                Type::INT2 => row_get_opt(row, i, |value: i16| Value::int(value.into(), span)),
                Type::INT4 => row_get_opt(row, i, |value: i32| Value::int(value.into(), span)),
                Type::INT8 => row_get_opt(row, i, |value: i64| Value::int(value, span)),
                Type::FLOAT4 => row_get_opt(row, i, |value: f32| Value::float(value.into(), span)),
                Type::FLOAT8 => row_get_opt(row, i, |value: f64| Value::float(value, span)),
                Type::JSON | Type::JSONB => {
                    row_get_opt(row, i, |value: serde_json::Value| json_to_nu(value))
                }
                Type::TIMESTAMPTZ => row_get_opt(row, i, |value: DateTime<FixedOffset>| {
                    Value::date(value, span)
                }),
                Type::TIMESTAMP => row_get_opt(row, i, |value: NaiveDateTime| {
                    let mut date_time = Record::with_capacity(6);
                    date_time.insert("year", Value::int(value.year().into(), span));
                    date_time.insert("month", Value::int(value.month().into(), span));
                    date_time.insert("day", Value::int(value.day().into(), span));
                    date_time.insert("hour", Value::int(value.hour().into(), span));
                    date_time.insert("minute", Value::int(value.minute().into(), span));
                    date_time.insert("second", Value::int(value.second().into(), span));
                    date_time.insert("nanosecond", Value::int(value.nanosecond().into(), span));

                    Value::record(date_time, span)
                }),
                Type::TIME => row_get_opt(row, i, |value: NaiveTime| {
                    let mut time = Record::with_capacity(4);
                    time.insert("hour", Value::duration(value.hour().into(), span));
                    time.insert("minute", Value::int(value.minute().into(), span));
                    time.insert("second", Value::int(value.second().into(), span));
                    time.insert("nanosecond", Value::int(value.nanosecond().into(), span));

                    Value::record(time, span)
                }),
                Type::OID => row_get_opt(row, i, |value: Oid| Value::int(value.into(), span)),
                ref r#type => {
                    return Err(LabeledError::new(format!(
                        "column `{}` has unsupported type `{type}`",
                        col.name(),
                    )))
                }
            };

            nu_row.insert(col.name(), value);
        }

        nu_rows.push(Value::record(nu_row, span));
    }

    Ok(Value::list(nu_rows, Span::unknown()))
}

fn row_get_opt<'a, T: FromSql<'a>>(
    row: &'a postgres::Row,
    i: usize,
    f: impl FnOnce(T) -> Value,
) -> Value {
    row.get::<_, Option<T>>(i)
        .map(f)
        .unwrap_or_else(|| Value::nothing(Span::unknown()))
}

fn from_pg_error(err: postgres::Error) -> LabeledError {
    let Some(db_err) = err.as_db_error() else {
        return LabeledError::new(err.to_string());
    };

    let msg = db_err.to_string();

    LabeledError::new(msg).with_code(db_err.code().code())
}

fn tls_connector() -> MakeRustlsConnect {
    let root_store = webpki_roots::TLS_SERVER_ROOTS
        .iter()
        .cloned()
        .collect::<RootCertStore>();

    let config = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    MakeRustlsConnect::new(config)
}

struct Args {
    query: String,
}

impl Args {
    fn parse(call: &EvaluatedCall) -> Result<Self, ShellError> {
        let query = call.req::<String>(0)?;

        Ok(Self { query })
    }
}

fn load_config(engine: &EngineInterface) -> Result<postgres::Config, ShellError> {
    let pg_url = env_var("PG_URL", engine)?;
    let config = pg_url.parse::<postgres::Config>().map_err(|err| {
        LabeledError::new(err.to_string())
            .with_help("The allowed syntax for `PG_URL` can be found at\nhttps://docs.rs/postgres/0.19.7/postgres/config/struct.Config.html#url")
    })?;

    Ok(config)
}

fn env_var(name: &str, engine: &EngineInterface) -> Result<String, ShellError> {
    env_var_opt(name, engine)?.ok_or_else(|| ShellError::EnvVarNotFoundAtRuntime {
        envvar_name: name.into(),
        span: Span::unknown(),
    })
}

fn env_var_opt(name: &str, engine: &EngineInterface) -> Result<Option<String>, ShellError> {
    let value = match engine.get_env_var(name)? {
        Some(value) => value,
        None => return Ok(None),
    };

    value.into_string().map(Some)
}

fn json_to_nu(value: serde_json::Value) -> Value {
    let span = Span::unknown();

    match value {
        serde_json::Value::Null => Value::nothing(span),
        serde_json::Value::Bool(value) => Value::bool(value, span),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Value::int(value, span)
            } else if let Some(value) = value.as_f64() {
                Value::float(value, span)
            } else {
                Value::string(value.to_string(), span)
            }
        }
        serde_json::Value::String(value) => Value::string(value, span),
        serde_json::Value::Array(values) => {
            let values = values.into_iter().map(json_to_nu).collect();

            Value::list(values, span)
        }
        serde_json::Value::Object(values) => {
            let mut record = Record::with_capacity(values.len());

            for (k, v) in values {
                let v = json_to_nu(v);

                record.insert(k, v);
            }

            Value::record(record, span)
        }
    }
}
