use std::{sync::Arc, time::Duration};

use super::Error as CribError;
use async_trait::async_trait;
use aws_sdk_s3::config::ProvideCredentials;
use aws_config::{meta::region::RegionProviderChain, BehaviorVersion};
use http::Method;
use object_store::{aws::AmazonS3Builder, signer::Signer, BackoffConfig, RetryConfig};
use url::Url;

pub fn signed_url(object_url: &Url) -> Result<Url, CribError>{
    let (_, path) = object_store::parse_url(object_url).map_err(|err| CribError::ObjectStore(err.to_string()))?;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .map_err(|err| CribError::InternalError(err.to_string()))?;

    let signed_url = runtime.block_on(async {
        let region_provider = RegionProviderChain::default_provider().or_else("useast-1");
        let config = aws_config::defaults(BehaviorVersion::latest())
            .region(region_provider)
            .load()
            .await;
        let region = config.region().unwrap();
        let mut builder = AmazonS3Builder::from_env()
            .with_region(region.to_string())
            .with_url(object_url.as_str())
            .with_retry(RetryConfig {
                backoff: BackoffConfig::default(),
                max_retries: 0,
                retry_timeout: Duration::default(),
            });
        if let Some(endpoint_url) = config.endpoint_url() {
            builder = builder.with_endpoint(endpoint_url.to_string());
        }
        let maybe_provider = config.credentials_provider();
        match maybe_provider {
            Some(provider) => {
                let provider = Arc::new(AwsCredentialProvider { provider });
                builder = builder.with_credentials(provider);
                let storage = Arc::new(builder.build().map_err(|err| CribError::ObjectStore(err.to_string()))?);
                let url = storage
                    .signed_url(Method::GET, &path, Duration::from_secs(5 * 60))
                    .await;
                url.map_err(|err| CribError::ObjectStore(err.to_string()))
            }
            None => Err(CribError::ObjectStore("credentials were not provided".to_string())),
        }
    });
    signed_url
}

#[derive(Debug)]
struct AwsCredentialProvider {
    provider: aws_credential_types::provider::SharedCredentialsProvider,
}

#[async_trait]
impl object_store::CredentialProvider for AwsCredentialProvider {
    type Credential = object_store::aws::AwsCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
        let credentials = self.provider.provide_credentials().await.map_err(|e| {
            object_store::Error::Generic {
                store: "S3",
                source: Box::new(e),
            }
        })?;
        Ok(Arc::new(object_store::aws::AwsCredential {
            key_id: credentials.access_key_id().to_string(),
            secret_key: credentials.secret_access_key().to_string(),
            token: credentials.session_token().map(ToString::to_string),
        }))
    }
}