// This example doesn't use all the crate dependencies and that's all right.
#![expect(unused_crate_dependencies)]

use futures::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let org = "myorg";
    let bucket = "mybucket";
    let influx_url = "http://localhost:9999";
    let token = "my-token";

    let client = influxdb2_client::Client::new(influx_url, token);

    let points = vec![
        influxdb2_client::models::DataPoint::builder("cpu_load_short")
            .tag("host", "server01")
            .tag("region", "us-west")
            .field("value", 0.64)
            .build()?,
        influxdb2_client::models::DataPoint::builder("cpu_load_short")
            .tag("host", "server01")
            .field("value", 27.99)
            .build()?,
    ];

    client.write(org, bucket, stream::iter(points)).await?;

    Ok(())
}
