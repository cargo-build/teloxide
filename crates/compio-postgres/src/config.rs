use std::{
    collections::BTreeMap,
    fmt, io,
    path::{Path, PathBuf},
};

use tokio_postgres::{
    Config,
    config::{ChannelBinding, Host},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Verification {
    None,
    Certificate,
    Full,
}

#[derive(Debug, Clone)]
pub(crate) struct TlsSettings {
    pub(crate) verification: Verification,
    pub(crate) plaintext_first: bool,
    pub(crate) root: Option<CertificateInput>,
    pub(crate) certificate: Option<CertificateInput>,
    pub(crate) key: Option<CertificateInput>,
}

#[derive(Clone)]
pub(crate) enum CertificateInput {
    Path(PathBuf),
    Pem(Vec<u8>),
}

impl fmt::Debug for CertificateInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Path(path) => f.debug_tuple("Path").field(path).finish(),
            Self::Pem(bytes) => write!(f, "InlinePem({} bytes)", bytes.len()),
        }
    }
}

impl CertificateInput {
    fn from_environment(value: String) -> Self {
        match value.starts_with("-----BEGIN ") {
            true => Self::Pem(value.into_bytes()),
            false => Self::Path(value.into()),
        }
    }

    #[cfg(any(feature = "native-tls", feature = "rustls"))]
    pub(crate) fn read(&self) -> io::Result<Vec<u8>> {
        match self {
            Self::Path(path) => std::fs::read(path),
            Self::Pem(bytes) => Ok(bytes.clone()),
        }
    }
}

pub(crate) fn parse_config(database_url: &str) -> io::Result<(Config, TlsSettings, usize)> {
    let mut tls = TlsSettings {
        verification: Verification::None,
        plaintext_first: false,
        root: env_option("PGSSLROOTCERT")?.map(CertificateInput::from_environment),
        certificate: env_option("PGSSLCERT")?.map(CertificateInput::from_environment),
        key: env_option("PGSSLKEY")?.map(CertificateInput::from_environment),
    };
    let url = url::Url::parse(database_url).map_err(io::Error::other)?;
    let mut base_url = url.clone();
    base_url.set_query(None);
    let base: Config = base_url.as_str().parse().map_err(io::Error::other)?;
    let mut endpoint = endpoint_options(&base, &url)?;
    let mut options = Vec::new();
    let mut startup_options = env_option("PGOPTIONS")?.into_iter().collect::<Vec<_>>();
    let mut statement_cache_capacity = 100;
    if let Some(mode) = env_option("PGSSLMODE")? {
        options.push(("sslmode".to_owned(), ssl_mode(&mode, &mut tls)?.to_owned()));
    }
    for (key, value) in url.query_pairs() {
        match key.as_ref() {
            "host" | "hostaddr" | "port" | "user" | "password" | "dbname" | "application_name" => {
                endpoint.insert(key.into_owned(), value.into_owned());
            }
            "sslmode" | "ssl-mode" => {
                options.push(("sslmode".to_owned(), ssl_mode(&value, &mut tls)?.to_owned()))
            }
            "sslrootcert" | "ssl-root-cert" | "ssl-ca" => {
                tls.root = Some(CertificateInput::Path(PathBuf::from(value.as_ref())))
            }
            "sslcert" | "ssl-cert" => {
                tls.certificate = Some(CertificateInput::Path(PathBuf::from(value.as_ref())))
            }
            "sslkey" | "ssl-key" => {
                tls.key = Some(CertificateInput::Path(PathBuf::from(value.as_ref())))
            }
            "statement-cache-capacity" => {
                statement_cache_capacity = value.parse().map_err(io::Error::other)?
            }
            "options" => startup_options.push(value.into_owned()),
            key if key.starts_with("options[") && key.ends_with(']') => {
                let name = &key[8..key.len() - 1];
                startup_options.push(format!("-c {name}={value}"));
            }
            _ => options.push((key.into_owned(), value.into_owned())),
        }
    }
    if !startup_options.is_empty() {
        options.push(("options".to_owned(), startup_options.join(" ")));
    }
    options.extend(endpoint);
    let mut url = url::Url::parse("postgres:///").map_err(io::Error::other)?;
    url.query_pairs_mut().extend_pairs(options);
    // libpq percent-decodes query values without form encoding's '+' space
    // rule.
    let encoded = url.as_str().replace('+', "%20");
    let mut config: Config = encoded.parse().map_err(io::Error::other)?;
    if config.get_channel_binding() == ChannelBinding::Require {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "channel_binding=require is unavailable: this adapter does not provide peer \
             certificate bindings",
        ));
    }
    apply_environment(&mut config)?;
    if tls.certificate.is_some() != tls.key.is_some() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "sslcert and sslkey must be supplied together",
        ));
    }
    Ok((config, tls, statement_cache_capacity))
}

fn endpoint_options(base: &Config, url: &url::Url) -> io::Result<BTreeMap<String, String>> {
    let mut endpoint = BTreeMap::new();
    if !base.get_hosts().is_empty() {
        let hosts = base
            .get_hosts()
            .iter()
            .map(|host| match host {
                Host::Tcp(host) => host.clone(),
                #[cfg(unix)]
                Host::Unix(path) => path.to_string_lossy().into_owned(),
            })
            .collect::<Vec<_>>()
            .join(",");
        endpoint.insert("host".to_owned(), hosts);
    }
    if let Some(port) = url.port() {
        endpoint.insert("port".to_owned(), port.to_string());
    }
    if let Some(user) = base.get_user() {
        endpoint.insert("user".to_owned(), user.to_owned());
    }
    if let Some(password) = base.get_password() {
        let password = String::from_utf8(password.to_vec()).map_err(io::Error::other)?;
        endpoint.insert("password".to_owned(), password);
    }
    if let Some(database) = base.get_dbname() {
        endpoint.insert("dbname".to_owned(), database.to_owned());
    }
    Ok(endpoint)
}

fn ssl_mode(mode: &str, tls: &mut TlsSettings) -> io::Result<&'static str> {
    tls.verification = Verification::None;
    tls.plaintext_first = false;
    match mode {
        "disable" => Ok("disable"),
        "allow" => {
            tls.plaintext_first = true;
            Ok("prefer")
        }
        "prefer" => Ok("prefer"),
        "require" => Ok("require"),
        "verify-ca" => {
            tls.verification = Verification::Certificate;
            Ok("require")
        }
        "verify-full" => {
            tls.verification = Verification::Full;
            Ok("require")
        }
        _ => Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid PostgreSQL sslmode")),
    }
}

fn apply_environment(config: &mut Config) -> io::Result<()> {
    if config.get_ports().is_empty() {
        let port = match env_option("PGPORT")? {
            Some(port) => port.parse().map_err(io::Error::other)?,
            None => 5432,
        };
        config.port(port);
    }
    if config.get_hosts().is_empty() && config.get_hostaddrs().is_empty() {
        let host = env_option("PGHOSTADDR")?.or(env_option("PGHOST")?);
        let host = match host {
            Some(host) => host,
            None => {
                let port = config.get_ports()[0];
                let socket = format!(".s.PGSQL.{port}");
                match ["/var/run/postgresql", "/private/tmp", "/tmp"]
                    .into_iter()
                    .find(|path| Path::new(path).join(&socket).exists())
                {
                    Some(path) => path.to_owned(),
                    None => "localhost".to_owned(),
                }
            }
        };
        config.host(host);
    }
    if config.get_user().is_none() {
        config.user(match env_option("PGUSER")? {
            Some(user) => user,
            None => whoami::username(),
        });
    }
    if config.get_dbname().is_none()
        && let Some(database) = env_option("PGDATABASE")?
    {
        config.dbname(database);
    }
    if config.get_application_name().is_none()
        && let Some(name) = env_option("PGAPPNAME")?
    {
        config.application_name(name);
    }
    if config.get_password().is_none()
        && let Some(password) = env_option("PGPASSWORD")?
    {
        config.password(password);
    }
    if config.get_password().is_none() {
        let host = match config.get_hosts().first() {
            Some(Host::Tcp(host)) => host.clone(),
            #[cfg(unix)]
            Some(Host::Unix(path)) => path.to_string_lossy().into_owned(),
            None => match config.get_hostaddrs().first() {
                Some(address) => address.to_string(),
                None => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "PostgreSQL host is missing",
                    ));
                }
            },
        };
        let user = config.get_user().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "PostgreSQL user is missing")
        })?;
        let password =
            crate::pgpass::load_password(&host, config.get_ports()[0], user, config.get_dbname());
        if let Some(password) = password {
            config.password(password);
        }
    }
    Ok(())
}

fn env_option(name: &str) -> io::Result<Option<String>> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(_) => {
            Err(io::Error::new(io::ErrorKind::InvalidInput, format!("{name} is not valid Unicode")))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_aliases_preserve_tls_and_startup_options() {
        let (config, tls, capacity) = parse_config(
            "postgres://user:p%2B%20ass@localhost/database?sslmode=verify-full&ssl-ca=root.pem&\
             options[search_path]=public&options=-c%20statement_timeout=10&\
             statement-cache-capacity=4",
        )
        .unwrap();
        assert_eq!(tls.verification, Verification::Full);
        assert!(
            matches!(tls.root, Some(CertificateInput::Path(path)) if path == Path::new("root.pem"))
        );
        assert!(
            config
                .get_options()
                .unwrap()
                .ends_with("-c search_path=public -c statement_timeout=10")
        );
        assert_eq!(capacity, 4);
        assert_eq!(config.get_password(), Some(b"p+ ass".as_slice()));
    }

    #[test]
    fn required_channel_binding_is_rejected_explicitly() {
        let error =
            parse_config("postgres://user:password@localhost/database?channel_binding=require")
                .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Unsupported);
        assert!(error.to_string().contains("channel_binding=require"));
    }

    #[test]
    fn query_endpoint_fields_override_uri_fields() {
        let (config, _, _) = parse_config(
            "postgres://old:old@old-host:5432/old?host=localhost&port=55439&user=new&password=new&\
             dbname=new",
        )
        .unwrap();
        assert_eq!(config.get_hosts(), &[Host::Tcp("localhost".to_owned())]);
        assert_eq!(config.get_ports(), &[55439]);
        assert_eq!(config.get_user(), Some("new"));
        assert_eq!(config.get_password(), Some(b"new".as_slice()));
        assert_eq!(config.get_dbname(), Some("new"));
    }

    #[test]
    fn inline_private_key_debug_is_redacted() {
        let input = CertificateInput::from_environment(
            "-----BEGIN PRIVATE KEY-----\nsecret-key\n-----END PRIVATE KEY-----".to_owned(),
        );
        assert!(!format!("{input:?}").contains("secret-key"));
    }
}
