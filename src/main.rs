use std::{env, time::Duration};

use nu_plugin::{
    serve_plugin, EngineInterface, EvaluatedCall, MsgPackSerializer, PluginCommand,
    SimplePluginCommand,
};
use nu_protocol::{LabeledError, ShellError, Signature, Span, SyntaxShape, Value};
use postgres::{config::SslMode, Client, NoTls};
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

        let client = connect(config)?;

        Ok(Value::nothing(Span::unknown()))
    }
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
        ShellError::GenericError {
            error: err.to_string(),
            msg: "The allowed syntax for `PG_URL` can be found at\nhttps://docs.rs/postgres/0.19.7/postgres/config/struct.Config.html#url".into(),
            span: None,
            help: None,
            inner: vec![],
         }
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
