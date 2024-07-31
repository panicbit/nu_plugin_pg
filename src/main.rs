use std::{env, time::Duration};

use chrono::{DateTime, Datelike, FixedOffset, NaiveDateTime, NaiveTime, Timelike};
use nu_plugin::{
    serve_plugin, EngineInterface, EvaluatedCall, MsgPackSerializer, PluginCommand,
    SimplePluginCommand,
};
use nu_protocol::{LabeledError, Record, ShellError, Signature, Span, SyntaxShape, Value};
use postgres::{
    config::SslMode,
    fallible_iterator::FallibleIterator,
    types::{FromSql, ToSql, Type},
    Client, GenericClient, NoTls, SimpleQueryMessage,
};
use rustls::RootCertStore;
use tokio_postgres_rustls::MakeRustlsConnect;

fn main() {
    serve_plugin(&mut PgPlugin::new(), MsgPackSerializer);
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
        plugin: &PgPlugin,
        engine: &EngineInterface,
        call: &EvaluatedCall,
        input: &Value,
    ) -> Result<Value, LabeledError> {
        let args = Args::parse(call)?;
        let mut config = load_config(&engine)?;

        config.connect_timeout(Duration::from_secs(30));

        let mut client = connect(config)?;

        let params: [&dyn ToSql; 0] = [];
        let mut rows = client
            .query_raw(&args.query, params)
            .map_err(from_pg_error)?;

        println!("Affected rows: {:?}", rows.rows_affected());

        let mut nu_rows = Vec::new();

        while let Some(row) = rows.next().transpose() {
            let row = &row.map_err(from_pg_error)?;
            let mut nu_row = Record::with_capacity(row.len());

            let span = Span::unknown();

            for (i, col) in row.columns().iter().enumerate() {
                let value = match *col.type_() {
                    Type::TEXT | Type::VARCHAR | Type::BPCHAR => {
                        row_get_opt(row, i, |value: String| Value::string(value, span))
                    }
                    Type::BOOL => row_get_opt(row, i, |value: bool| Value::bool(value, span)),
                    Type::CHAR => row_get_opt(row, i, |value: i8| Value::int(value.into(), span)),
                    Type::INT2 => row_get_opt(row, i, |value: i16| Value::int(value.into(), span)),
                    Type::INT4 => row_get_opt(row, i, |value: i32| Value::int(value.into(), span)),
                    Type::INT8 => row_get_opt(row, i, |value: i64| Value::int(value, span)),
                    Type::FLOAT4 => {
                        row_get_opt(row, i, |value: f32| Value::float(value.into(), span))
                    }
                    Type::FLOAT8 => {
                        row_get_opt(row, i, |value: f64| Value::float(value.into(), span))
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
                    ref r#type => {
                        return Err(LabeledError::new(format!(
                            "unsupported column type: {type}"
                        )))
                    }
                };

                nu_row.insert(col.name(), value);
            }

            nu_rows.push(Value::record(nu_row, span));
        }

        Ok(Value::list(nu_rows, Span::unknown()))
    }
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

fn connect(config: postgres::Config) -> Result<Client, LabeledError> {
    match config.get_ssl_mode() {
        SslMode::Disable => config
            .connect(NoTls)
            .map_err(|err| LabeledError::new(err.to_string())),
        SslMode::Prefer => {
            let error_without_tls = match config.connect(tls_connector()) {
                Ok(client) => return Ok(client),
                Err(error_without_tls) => error_without_tls,
            };

            let error_with_tls = match config.connect(tls_connector()) {
                Ok(client) => return Ok(client),
                Err(error_with_tls) => error_with_tls,
            };

            let combined_error = LabeledError::new("failed to connect")
                .with_inner(LabeledError::new(format!(
                    "without tls: {error_without_tls}"
                )))
                .with_inner(LabeledError::new(format!("with tls: {error_with_tls}")));

            Err(combined_error)
        }
        SslMode::Require => config
            .connect(tls_connector())
            .map_err(|err| LabeledError::new(err.to_string())),
        ssl_mode => Err(LabeledError::new(format!(
            "ssl mode `{ssl_mode:?}` is not implemented"
        ))),
    }
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
