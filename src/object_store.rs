use std::{sync::Arc, time::Duration};
use async_trait::async_trait;
use aws_config::{meta::region::RegionProviderChain, BehaviorVersion};
use aws_sdk_s3::config::ProvideCredentials;
use http::Method;
use object_store::{aws::{AmazonS3, AmazonS3Builder}, signer::Signer, BackoffConfig, ObjectStore, RetryConfig};
use url::Url;

use crate::Error as CribError;

pub async fn use_aws_config(object_url: &Url) -> Result<(Arc<AmazonS3>, object_store::path::Path), CribError> {
    let (_, path) = object_store::parse_url(object_url)?;

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
            let storage = Arc::new(builder.build()?);
            Ok((storage, path))
        }
        None => Err(CribError::ObjectStore("credentials were not provided".to_string())),
    }
}

pub async fn presigned_urls(s3_url: &Url) -> Result<Vec<Url>, CribError> {
    let mut presigned_urls = Vec::new();
    let (object_store, path) = use_aws_config(s3_url).await?;
    if s3_url.path().ends_with('/') {
        let listing = object_store
            .list_with_delimiter(Some(&path))
            .await?;
        for object in listing.objects {
            let url = object_store
                .signed_url(Method::GET, &object.location, Duration::from_secs(5 * 60))
                .await?;
            presigned_urls.push(url);
        }
    } else {
        let url = object_store
            .signed_url(Method::GET, &path, Duration::from_secs(5 * 60))
            .await?;
        presigned_urls.push(url);
    }
    Ok(presigned_urls)
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