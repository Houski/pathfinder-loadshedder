# Pathfinder Load Shedder

### === This is a currently in an experimental stage. Buyer-beware! Right now it relies on Axum, should be generalized to just use Tower. ===

## Overview

Pathfinder Load Shedder is a middleware service built using the `tower` library in Rust. It is designed to manage and limit the number of concurrent requests globally, particularly targeting your heaviest paths to shed excess load by returning a 503 Service Unavailable status with a customizable body.

## Features

- **Global Request Limiting**: Define paths to be globally monitored and limit concurrent requests across these paths.
- **Customizable 503 Responses**: Configure custom response bodies for 503 status.
- **Timeouts**: A timeout can be set specifically for the requests affected by this middleware.

## Example

A basic example of how to integrate Pathfinder Load Shedder into an Axum application.

```rust

use axum::{Router, routing::get, response::IntoResponse};
use tower::ServiceBuilder;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    // Define paths to globally limit.
    // Any requests to paths not in this list are simply passed through, unaffected.
    let paths = vec![
        PathfinderPath { path: "/api/v1/resource" },
        PathfinderPath { path: "/api/v1/other" },
    ];

    // Create PathfinderLoadShedderLayer with a global max of 2 concurrent requests with a 10 second timeout.
    let load_shedder_layer = PathfinderLoadShedderLayer::new(paths, 2, 10000)
        .set_pathfinder_503_body("Service Temporarily Unavailable");

    // Build our application with some routes
    let app = Router::new()
        .route("/api/v1/resource", get(handler))
        .route("/api/v1/other", get(handler))
        .layer(ServiceBuilder::new().layer(load_shedder_layer));

    // Run our application
    axum::Server::bind(&"0.0.0.0:3000".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}

async fn handler() -> impl IntoResponse {
    "Hello, world!"
}
```
