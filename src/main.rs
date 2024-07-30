use std::env;

use nu_plugin::{serve_plugin, EvaluatedCall, LabeledError, MsgPackSerializer};
use nu_protocol::{PluginSignature, ShellError, Span, SyntaxShape, Value};

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
    fn signature(&self) -> Vec<PluginSignature> {
        let signature =
            PluginSignature::build("pg").required("query", SyntaxShape::String, "query to execute");

        vec![signature]
    }

    fn run(
        &mut self,
        name: &str,
        call: &EvaluatedCall,
        input: &Value,
    ) -> Result<Value, LabeledError> {
        let args = Args::parse(call)?;
        let config = load_config()?;

        Ok(Value::nothing(Span::unknown()))
    }
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

fn load_config() -> Result<postgres::Config, ShellError> {
    let pg_url = env_var("PG_URL")?;
    let config = pg_url.parse::<postgres::Config>().map_err(|err| {
        ShellError::GenericError(
            err.to_string(),
            "The allowed syntax for `PG_URL` can be found at\nhttps://docs.rs/postgres/0.19.7/postgres/config/struct.Config.html#url".into(),
            None,
            None,
            vec![],
        )
    })?;
}

fn env_var(name: &str) -> Result<String, ShellError> {
    env_var_opt(name)?.ok_or_else(|| ShellError::EnvVarNotFoundAtRuntime {
        envvar_name: name.into(),
        span: Span::unknown(),
    })
}

fn env_var_opt(name: &str) -> Result<Option<String>, ShellError> {
    env::var(name).map(Some).or_else(|err| match err {
        env::VarError::NotPresent => Ok(None),
        env::VarError::NotUnicode(_) => Err(ShellError::EnvVarNotAString {
            envvar_name: name.into(),
            span: Span::unknown(),
        }),
    })
}
