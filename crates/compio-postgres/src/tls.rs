use std::io;

use crate::config::{TlsSettings, Verification};

pub(super) enum Connector {
    #[cfg(feature = "native-tls")]
    Native(tokio_native_tls::TlsConnector),
    #[cfg(all(feature = "rustls", not(feature = "native-tls")))]
    Rustls(tokio_rustls::TlsConnector),
}

impl TlsSettings {
    pub(super) async fn connector(&self) -> io::Result<Connector> {
        let settings = self.clone();
        compio::runtime::spawn_blocking(move || settings.build()).await.map_err(io::Error::from)?
    }

    #[cfg(feature = "native-tls")]
    fn build(&self) -> io::Result<Connector> {
        let mut builder = native_tls::TlsConnector::builder();
        builder.danger_accept_invalid_certs(self.verification == Verification::None);
        builder.danger_accept_invalid_hostnames(self.verification != Verification::Full);
        if let Some(root) = &self.root {
            let certificate =
                native_tls::Certificate::from_pem(&root.read()?).map_err(io::Error::other)?;
            builder.add_root_certificate(certificate);
        }
        if let (Some(certificate), Some(key)) = (&self.certificate, &self.key) {
            let identity = native_tls::Identity::from_pkcs8(&certificate.read()?, &key.read()?)
                .map_err(io::Error::other)?;
            builder.identity(identity);
        }
        builder
            .build()
            .map(|connector| Connector::Native(connector.into()))
            .map_err(io::Error::other)
    }

    #[cfg(all(feature = "rustls", not(feature = "native-tls")))]
    fn build(&self) -> io::Result<Connector> {
        use std::sync::Arc;

        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut roots = rustls::RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        if let Some(root) = &self.root {
            let bytes = root.read()?;
            let certificates =
                rustls_pemfile::certs(&mut bytes.as_slice()).collect::<Result<Vec<_>, _>>()?;
            if certificates.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "sslrootcert contains no certificates",
                ));
            }
            for certificate in certificates {
                roots.add(certificate).map_err(io::Error::other)?;
            }
        }
        let verifier = Arc::new(Verifier {
            verification: self.verification,
            roots,
            provider: provider.clone(),
        });
        let builder = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(io::Error::other)?
            .dangerous()
            .with_custom_certificate_verifier(verifier);
        let mut config = match (&self.certificate, &self.key) {
            (Some(certificate), Some(key)) => {
                let certificate = certificate.read()?;
                let certificate = rustls_pemfile::certs(&mut certificate.as_slice())
                    .collect::<Result<Vec<_>, _>>()?;
                let bytes = key.read()?;
                let mut key = bytes.as_slice();
                let key = rustls_pemfile::private_key(&mut key)?.ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "sslkey contains no private key")
                })?;
                builder.with_client_auth_cert(certificate, key).map_err(io::Error::other)?
            }
            _ => builder.with_no_client_auth(),
        };
        config.alpn_protocols.push(b"postgresql".to_vec());
        Ok(Connector::Rustls(Arc::new(config).into()))
    }
}

#[cfg(all(feature = "rustls", not(feature = "native-tls")))]
#[derive(Debug)]
struct Verifier {
    verification: Verification,
    roots: rustls::RootCertStore,
    provider: std::sync::Arc<rustls::crypto::CryptoProvider>,
}

#[cfg(all(feature = "rustls", not(feature = "native-tls")))]
impl rustls::client::danger::ServerCertVerifier for Verifier {
    fn verify_server_cert(
        &self,
        end_entity: &rustls::pki_types::CertificateDer<'_>,
        intermediates: &[rustls::pki_types::CertificateDer<'_>],
        server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp: &[u8],
        now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        if self.verification != Verification::None {
            let certificate = rustls::server::ParsedCertificate::try_from(end_entity)?;
            rustls::client::verify_server_cert_signed_by_trust_anchor(
                &certificate,
                &self.roots,
                intermediates,
                now,
                self.provider.signature_verification_algorithms.all,
            )?;
            if self.verification == Verification::Full {
                rustls::client::verify_server_name(&certificate, server_name)?;
            }
        }
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        certificate: &rustls::pki_types::CertificateDer<'_>,
        signature: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            certificate,
            signature,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        certificate: &rustls::pki_types::CertificateDer<'_>,
        signature: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            certificate,
            signature,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.provider.signature_verification_algorithms.supported_schemes()
    }
}
