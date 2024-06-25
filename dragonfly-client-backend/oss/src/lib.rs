use std::time::Duration;

use dragonfly_client_backend::*;
use dragonfly_client_core::*;
use dragonfly_client_util::tls::NoVerifier;
use opendal::{raw::HttpClient, Metakey, Operator};
use reqwest::header::HeaderMap;
use rustls_pki_types::CertificateDer;
use tokio_util::io::StreamReader;
use tracing::info;
use url::Url;

const BACKEND_NAME: &str = "OSS";

/// the config that parsed from the url
struct OSSConfig {
    url: Url,
    bucket: String,
    endpoint: String,
    version: Option<String>,
}

impl OSSConfig {
    fn is_dir(&self) -> bool {
        self.url.path().ends_with('/')
    }

    #[inline]
    fn key(&self) -> &str {
        self.url.path()
    }

    fn get_url_with_same_endpoint(&self, path: &str) -> String {
        let mut url = self.url.clone();
        url.set_path(path);
        url.to_string()
    }
}

impl TryFrom<Url> for OSSConfig {
    type Error = Error;

    fn try_from(url: Url) -> std::result::Result<Self, Self::Error> {
        let host_str = url
            .host_str()
            .ok_or_else(|| Error::InvalidURI(url.to_string()))?;

        let (bucket, endpoint) = host_str
            .split_once('.')
            .map(|(s1, s2)| (s1.to_string(), s2.to_string())) // add scheme for endpoint
            .ok_or(Error::InvalidURI(url.to_string()))?;

        let version = url
            .query_pairs()
            .find(|(key, _)| key == "versionId")
            .map(|(_, version)| version.to_string());

        Ok(Self {
            url,
            endpoint,
            bucket,
            version,
        })
    }
}

#[derive(Default)]
pub struct OSS;

impl OSS {
    pub fn new() -> Self {
        Self
    }

    // client returns a new reqwest client.
    fn client_builder(
        &self,
        client_certs: Option<Vec<CertificateDer<'static>>>,
    ) -> reqwest::ClientBuilder {
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

        reqwest::Client::builder().use_preconfigured_tls(client_config_builder)
    }

    fn get_op_and_config(
        &self,
        header: HeaderMap,
        certs: Option<Vec<CertificateDer<'static>>>,
        timeout: Duration,
        url: String,
        access_key_id: Option<&str>,
        access_key_secret: Option<&str>,
    ) -> Result<(OSSConfig, Operator)> {
        let client = self
            .client_builder(certs)
            .timeout(timeout)
            .default_headers(header)
            .build()?;

        let url: Url = url.parse().map_err(|_| Error::InvalidURI(url))?;

        let config = OSSConfig::try_from(url)?;

        let mut oss_builder = opendal::services::Oss::default();

        oss_builder
            .access_key_id(access_key_id.ok_or(Error::InvalidParameter)?)
            .access_key_secret(access_key_secret.ok_or(Error::InvalidParameter)?)
            .http_client(HttpClient::with(client))
            .root("/")
            .bucket(&config.bucket)
            .endpoint(&config.endpoint);

        Ok((
            config,
            Operator::new(oss_builder).map_err(map_sdk_error)?.finish(),
        ))
    }
}

#[tonic::async_trait]
impl Backend for OSS {
    async fn head(&self, request: HeadRequest) -> Result<HeadResponse> {
        info!(
            "head request {} {}: {:?}",
            request.task_id, request.url, request.http_header
        );

        let (config, op) = self.get_op_and_config(
            request.http_header.clone().unwrap_or_default(),
            request.client_certs,
            request.timeout,
            request.url,
            request.access_key_id.as_deref(),
            request.access_key_secret.as_deref(),
        )?;

        // get the entries if url point to a directory
        let entries = if config.is_dir() {
            Some(
                op.list_with(config.key())
                    .limit(request.file_number_limit)
                    .recursive(request.recursive)
                    .metakey(Metakey::ContentLength | Metakey::Mode)
                    .await // do the list op here
                    .map_err(map_sdk_error)?
                    .into_iter()
                    .map(|entry| {
                        let path = entry.path();
                        let metadata = entry.metadata();
                        let content_length = metadata.content_length() as usize;
                        let is_dir = metadata.is_dir();

                        DirEntry {
                            url: config.get_url_with_same_endpoint(path),
                            content_length,
                            is_dir,
                        }
                    })
                    .collect(),
            )
        } else {
            None
        };

        let mut stat_op = op.stat_with(config.key());

        if let Some(ref version) = config.version {
            stat_op = stat_op.version(version);
        }

        let stat = stat_op.await.map_err(map_sdk_error)?;

        Ok(HeadResponse {
            success: true,
            content_length: Some(stat.content_length()),
            http_header: request.http_header,
            http_status_code: Some(reqwest::StatusCode::OK),
            error_message: None,
            entries,
        })
    }

    async fn get(&self, request: GetRequest) -> Result<GetResponse<Body>> {
        info!(
            "get request {} {}: {:?}",
            request.piece_id, request.url, request.http_header
        );

        let (config, op) = self.get_op_and_config(
            request.http_header.clone().unwrap_or_default(),
            request.client_certs,
            request.timeout,
            request.url,
            request.access_key_id.as_deref(),
            request.access_key_secret.as_deref(),
        )?;

        let mut reader_op = op.reader_with(config.key());

        if let Some(ref version) = config.version {
            reader_op = reader_op.version(version);
        }

        let reader = reader_op.await.map_err(map_sdk_error)?;

        let stream = match request.range {
            Some(range) => reader
                .into_bytes_stream(range.start..range.start + range.length)
                .await
                .map_err(map_sdk_error)?,
            None => reader.into_bytes_stream(..).await.map_err(map_sdk_error)?,
        };

        Ok(GetResponse {
            success: true,
            http_header: None,
            http_status_code: Some(reqwest::StatusCode::OK),
            reader: Box::new(StreamReader::new(stream)),
            error_message: None,
        })
    }
}

fn map_sdk_error(e: opendal::Error) -> Error {
    Error::BackendSDKError(BACKEND_NAME.into(), e.to_string())
}
