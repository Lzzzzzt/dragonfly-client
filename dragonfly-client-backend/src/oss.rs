use dragonfly_client_core::*;
use dragonfly_client_util::tls::NoVerifier;
use rustls_pki_types::CertificateDer;
use url::Url;

use crate::*;

struct OSSConfig<'a> {
    endpoint: &'a str,
    bucket: &'a str,
    version: Option<&'a str>,
}

impl<'a> TryFrom<&'a str> for OSSConfig<'a> {
    type Error = Error;

    fn try_from(_url: &'a str) -> std::result::Result<Self, Self::Error> {
        todo!("Parse the url to generate the oss config")
    }
}

#[derive(Default)]
pub struct OSS;

impl OSS {
    pub fn new() -> Self {
        Self
    }

    // client returns a new reqwest client.
    fn client(
        &self,
        client_certs: Option<Vec<CertificateDer<'static>>>,
    ) -> Result<reqwest::Client> {
        let client_config_builder = match client_certs.as_ref() {
            Some(client_certs) => {
                let mut root_cert_store = rustls::RootCertStore::empty();
                root_cert_store.add_parsable_certificates(client_certs.to_owned());

                // TLS client config using the custom CA store for lookups.
                rustls::ClientConfig::builder()
                    .with_root_certificates(root_cert_store)
                    .with_no_client_auth()
            }
            // Default TLS client config with native roots.
            None => rustls::ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(NoVerifier::new())
                .with_no_client_auth(),
        };

        let client = reqwest::Client::builder()
            .use_preconfigured_tls(client_config_builder)
            .build()?;
        Ok(client)
    }
}

#[tonic::async_trait]
impl Backend for OSS {
    async fn head(&self, request: HeadRequest) -> Result<HeadResponse> {
        // 1. get client for sending request
        // 2. build opendal::services::Oss
        // 3. parse the url and get the config
        // 4. set the config for Oss Service
        // 5. call the `stat` function if the request is for file,
        //    otherwise call the `stat` and `list` function both
        // 6. build the `HeadResponse`

        todo!()
    }

    async fn get(&self, request: GetRequest) -> Result<GetResponse<Body>> {
        // 1. get client for sending request
        // 2. build opendal::services::Oss
        // 3. parse the url and get the config
        // 4. set the config for Oss Service
        // 5. call the `reader` function to get the bytes_stream
        // 6. build the `HeadResponse`
        todo!()
    }
}
