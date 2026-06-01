use aws_config::SdkConfig;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_sdk_s3::{
    Client,
    config::{Credentials, Region, RequestChecksumCalculation},
};

use super::server::TestServer;

const REGION: &str = "us-east-1";

pub fn client(server: &TestServer) -> Client {
    client_with_credentials(server, server.access_key(), server.secret_key())
}

pub fn bad_secret_client(server: &TestServer) -> Client {
    client_with_credentials(server, server.access_key(), "wrong-secret-key-for-tests")
}

pub fn bad_access_key_client(server: &TestServer) -> Client {
    client_with_credentials(server, "wrong-access-key-for-tests", server.secret_key())
}

fn client_with_credentials(server: &TestServer, access_key: &str, secret_key: &str) -> Client {
    let credentials = Credentials::new(access_key, secret_key, None, None, "s3ite-test");
    let config = SdkConfig::builder()
        .credentials_provider(SharedCredentialsProvider::new(credentials))
        .region(Region::new(REGION))
        .endpoint_url(server.endpoint())
        .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
        .build();
    let config = aws_sdk_s3::config::Builder::from(&config)
        .force_path_style(true)
        .build();

    Client::from_conf(config)
}
