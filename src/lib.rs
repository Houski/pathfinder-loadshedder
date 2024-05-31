use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{Arc, RwLock},
    task::{Context, Poll},
    time::Instant,
};

use axum::body::Body;
use hyper::{Request, Response, StatusCode};
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::time::timeout;
use tower::{Layer, Service};

#[derive(Clone, Debug)]
struct PathfinderPathTransforms {
    transforms: Vec<Vec<&'static str>>,
}

impl PathfinderPathTransforms {
    fn new(transforms: Vec<&'static str>) -> Self {
        Self {
            transforms: transforms
                .into_iter()
                .map(|transform| transform.split('/').collect())
                .collect(),
        }
    }

    fn normalize_path(&self, path: &str) -> Option<String> {
        let path_segments: Vec<&str> = path.split('/').collect();
        for transform_segments in &self.transforms {
            if let Some(normalized) = self.match_and_transform(&path_segments, transform_segments) {
                return Some(normalized);
            }
        }
        None
    }

    fn match_and_transform(
        &self,
        path_segments: &[&str],
        transform_segments: &[&'static str],
    ) -> Option<String> {
        let mut normalized_segments = vec![];

        for (path_segment, transform_segment) in path_segments.iter().zip(transform_segments.iter())
        {
            if *transform_segment == "*" || *transform_segment == *path_segment {
                normalized_segments.push(*transform_segment);
            } else {
                return None;
            }
        }

        Some(normalized_segments.join("/"))
    }
}

#[derive(Debug)]
struct PathfinderPaths {
    paths: HashMap<String, Vec<f64>>,
}

impl PathfinderPaths {
    fn update_path_latency(&mut self, path: &str, latency: f64) {
        println!("Updating latency for path: {}", path);
        self.paths
            .entry(path.to_string())
            .or_insert_with(Vec::new)
            .push(latency);

        // remove if over 5 latencies
        if let Some(latencies) = self.paths.get_mut(path) {
            if latencies.len() > 100 {
                latencies.remove(0);
            }
        }

        println!(
            "Updated latencies for path {}: {:?}",
            path,
            self.paths.get(path)
        );
    }

    fn get_path_average_latency(&self, path: &str) -> f64 {
        let average_latency = self.paths.get(path).map_or(0.0, |latencies| {
            let sum: f64 = latencies.iter().sum();
            let count = latencies.len();
            sum / count as f64
        });

        println!("Average latency for path {}: {}", path, average_latency);
        average_latency
    }
}
#[derive(Debug, Clone)]
struct PathfinderCurrentlyRequestedPaths {
    currently_requested_paths: Arc<RwLock<HashMap<String, u32>>>,
}

impl PathfinderCurrentlyRequestedPaths {
    fn new() -> Self {
        Self {
            currently_requested_paths: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn add_path(&self, path: &str) {
        println!("Adding path to currently requested paths: {}", path);
        let mut paths = self.currently_requested_paths.write().unwrap();
        let count = paths.entry(path.to_string()).or_insert(0);
        *count += 1;
    }

    fn remove_path(&self, path: &str) {
        println!("Removing path from currently requested paths: {}", path);
        let mut paths = self.currently_requested_paths.write().unwrap();
        if let Some(count) = paths.get_mut(path) {
            *count -= 1;
            if *count == 0 {
                paths.remove(path);
            }
        }
    }

    fn get_currently_requested_paths(&self) -> Vec<(String, u32)> {
        let paths = self
            .currently_requested_paths
            .read()
            .unwrap()
            .iter()
            .map(|(path, &count)| (path.clone(), count))
            .collect();
        println!("Currently requested paths: {:?}", self);
        paths
    }

    fn get_total_latency_of_currently_requested_paths(&self, paths: &PathfinderPaths) -> f64 {
        let currently_requested_paths = self.get_currently_requested_paths();
        let mut total_latency = 0.0;
        let mut total_count = 0;

        for (path, count) in currently_requested_paths {
            let avg_latency = paths.get_path_average_latency(&path);
            total_latency += avg_latency * count as f64;
            total_count += count;
        }

        // Corrected system total latency calculation
        let system_total_latency = total_latency;

        println!(
            "Total latency of currently requested paths: {}",
            system_total_latency
        );
        system_total_latency
    }
}

#[derive(Debug, Clone)]
pub struct PathfinderLoadShedder<S> {
    max_allowable_system_average_latency_ms: f64,
    currently_requested_paths: Arc<RwLock<PathfinderCurrentlyRequestedPaths>>,
    paths: Arc<RwLock<PathfinderPaths>>,
    pathfinder_path_transforms: PathfinderPathTransforms,
    inner: S,
    semaphore: Arc<Semaphore>,
    timeout_duration_ms: f64,
}

impl<S> PathfinderLoadShedder<S> {
    fn new(
        max_allowable_system_average_latency_ms: f64,
        pathfinder_path_transforms: PathfinderPathTransforms,
        inner: S,
        max_concurrent_requests: usize,
        timeout_duration_ms: f64,
    ) -> Self {
        Self {
            max_allowable_system_average_latency_ms,
            paths: Arc::new(RwLock::new(PathfinderPaths {
                paths: HashMap::new(),
            })),
            currently_requested_paths: Arc::new(RwLock::new(
                PathfinderCurrentlyRequestedPaths::new(),
            )),
            pathfinder_path_transforms,
            inner,
            semaphore: Arc::new(Semaphore::new(max_concurrent_requests)),
            timeout_duration_ms,
        }
    }
}

impl<S> Service<Request<Body>> for PathfinderLoadShedder<S>
where
    S: Service<Request<Body>, Response = Response<Body>> + Send + Sync + 'static,
    S::Future: Send + 'static,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    type Response = Response<Body>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let path = req.uri().path().to_string();
        let start = Instant::now();
        let fut = self.inner.call(req);
        let paths = self.paths.clone();
        let currently_requested_paths = self.currently_requested_paths.clone();
        let semaphore = self.semaphore.clone();
        let timeout_duration: Duration = Duration::from_millis(self.timeout_duration_ms as u64);
        let max_allowable_system_average_latency_ms = self.max_allowable_system_average_latency_ms;

        let normalized_path = match self.pathfinder_path_transforms.normalize_path(&path) {
            Some(p) => p,
            None => {
                return Box::pin(async move {
                    let res = fut.await?;
                    Ok(res)
                });
            }
        };

        Box::pin(async move {
            // Acquire a permit from the semaphore
            let permit = match semaphore.try_acquire() {
                Ok(permit) => permit,
                Err(_) => {
                    {
                        currently_requested_paths
                            .write()
                            .unwrap()
                            .remove_path(&normalized_path);
                    }
                    // Return 503 response
                    return Ok(Response::builder()
                        .status(StatusCode::SERVICE_UNAVAILABLE)
                        .body("Server is under high load, please try again".into())
                        .unwrap());
                }
            };

            // Calculate the system average latency including the current request
            let system_average_latency;
            {
                let paths = paths.read().unwrap();
                let currently_requested_paths = currently_requested_paths.read().unwrap();
                system_average_latency = currently_requested_paths
                    .get_total_latency_of_currently_requested_paths(&*paths);
            }

            // Check if the system is under high load including the current request
            if system_average_latency > max_allowable_system_average_latency_ms {
                // Remove the path from currently requested paths as we are not going to process it
                {
                    currently_requested_paths
                        .write()
                        .unwrap()
                        .remove_path(&normalized_path);
                }

                // Release the permit back to the semaphore
                drop(permit);

                // Return 503 response
                return Ok(Response::builder()
                    .status(StatusCode::SERVICE_UNAVAILABLE)
                    .header(
                        "Retry-After",
                        (max_allowable_system_average_latency_ms / 1000.0).ceil() as u32,
                    )
                    .body("Server is under high load, please try again".into())
                    .unwrap());
            }

            // Add the path to currently requested paths
            currently_requested_paths
                .write()
                .unwrap()
                .add_path(&normalized_path);

            let res = timeout(timeout_duration, fut).await;

            let latency = start.elapsed().as_secs_f64() * 1000.0;

            let capped_latency = if latency >= max_allowable_system_average_latency_ms - 1.0 {
                max_allowable_system_average_latency_ms - 1.0
            } else {
                latency
            };

            {
                paths
                    .write()
                    .unwrap()
                    .update_path_latency(&normalized_path, capped_latency);
                currently_requested_paths
                    .write()
                    .unwrap()
                    .remove_path(&normalized_path);
            }

            // Release the permit back to the semaphore
            drop(permit);

            match res {
                Ok(Ok(response)) => Ok(response),
                Ok(Err(err)) => Err(err),
                Err(_) => Ok(Response::builder()
                    .status(StatusCode::SERVICE_UNAVAILABLE)
                    .body("Request timed out".into())
                    .unwrap()),
            }
        })
    }
}

#[derive(Clone)]
pub struct PathfinderLoadShedderLayer {
    max_allowable_system_average_latency_ms: f64,
    pathfinder_path_transforms: PathfinderPathTransforms,
    max_concurrent_requests: usize,
    timeout_duration_ms: f64,
}

impl PathfinderLoadShedderLayer {
    pub fn new(
        max_allowable_system_average_latency_ms: f64,
        pathfinder_path_transforms: Vec<&'static str>,
        max_concurrent_requests: usize,
        timeout_duration_ms: f64,
    ) -> Self {
        Self {
            max_allowable_system_average_latency_ms,
            pathfinder_path_transforms: PathfinderPathTransforms::new(pathfinder_path_transforms),
            max_concurrent_requests,
            timeout_duration_ms,
        }
    }
}

impl<S> Layer<S> for PathfinderLoadShedderLayer {
    type Service = PathfinderLoadShedder<S>;

    fn layer(&self, inner: S) -> Self::Service {
        PathfinderLoadShedder::new(
            self.max_allowable_system_average_latency_ms,
            self.pathfinder_path_transforms.clone(),
            inner,
            self.max_concurrent_requests,
            self.timeout_duration_ms,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use hyper::{Request, Response, StatusCode};
    use tower::{service_fn, Service};

    #[tokio::test]
    async fn test_normalize_path() {
        let paths = vec![
            "/location/average/*/*/*/*/*",
            "/location/*/*/*/*",
            "/property/*/*/*/*/*",
            "/search/*",
        ];

        let transformer = PathfinderPathTransforms::new(paths);

        let test_paths = vec![
            (
                "/property/CA/AB/Calgary/Downtown/123",
                "/property/*/*/*/*/*",
            ),
            ("/property/ca", "/property/*"),
            (
                "/property/CA/AB/Calgary/Downtown/123?whatever=123",
                "/property/*/*/*/*/*",
            ),
            ("/location/CA/AB/Calgary/Downtown", "/location/*/*/*/*"),
            (
                "/location/average/somefield/CA/AB/Calgary/Downtown",
                "/location/average/*/*/*/*/*",
            ),
            ("/search/Calgary", "/search/*"),
        ];

        for (path, expected) in test_paths {
            assert_eq!(transformer.normalize_path(path).unwrap(), expected);
        }
    }

    #[test]
    fn test_update_path_latency() {
        let mut paths = PathfinderPaths {
            paths: HashMap::new(),
        };

        paths.update_path_latency("/test", 100.0);
        paths.update_path_latency("/test", 200.0);
        paths.update_path_latency("/test", 300.0);
        paths.update_path_latency("/test", 400.0);
        paths.update_path_latency("/test", 500.0);
        paths.update_path_latency("/test", 600.0);

        assert_eq!(
            paths.paths.get("/test").unwrap(),
            &vec![100.0, 200.0, 300.0, 400.0, 500.0, 600.0]
        );

        let avg_latency = paths.get_path_average_latency("/test");
        assert_eq!(avg_latency, 350.0);
    }

    #[test]
    fn test_currently_requested_paths() {
        let currently_requested_paths = PathfinderCurrentlyRequestedPaths::new();

        currently_requested_paths.add_path("/test1");
        currently_requested_paths.add_path("/test1");
        currently_requested_paths.add_path("/test2");

        let mut paths = currently_requested_paths.get_currently_requested_paths();
        paths.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            paths,
            vec![("/test1".to_string(), 2), ("/test2".to_string(), 1)]
        );

        currently_requested_paths.remove_path("/test1");
        let mut paths = currently_requested_paths.get_currently_requested_paths();
        paths.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            paths,
            vec![("/test1".to_string(), 1), ("/test2".to_string(), 1)]
        );

        currently_requested_paths.remove_path("/test1");
        let mut paths = currently_requested_paths.get_currently_requested_paths();
        paths.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(paths, vec![("/test2".to_string(), 1)]);

        currently_requested_paths.remove_path("/test2");
        let mut paths = currently_requested_paths.get_currently_requested_paths();
        paths.sort_by(|a, b| a.0.cmp(&b.0));
        assert!(paths.is_empty());
    }

    #[test]
    fn test_total_latency_of_currently_requested_paths() {
        let mut paths = PathfinderPaths {
            paths: HashMap::new(),
        };
        paths.update_path_latency("/test1", 100.0);
        paths.update_path_latency("/test1", 200.0);
        paths.update_path_latency("/test2", 300.0);

        let currently_requested_paths = PathfinderCurrentlyRequestedPaths::new();
        currently_requested_paths.add_path("/test1");
        currently_requested_paths.add_path("/test2");

        let total_latency =
            currently_requested_paths.get_total_latency_of_currently_requested_paths(&paths);
        assert_eq!(total_latency, 450.0); // 150.0 * 1 + 300.0 * 1
    }

    #[tokio::test]
    async fn test_load_shedding_normal_operation() {
        let service = service_fn(|_req: Request<Body>| async {
            Ok::<_, hyper::Error>(Response::new(Body::from("Hello, World!")))
        });

        let layer = PathfinderLoadShedderLayer::new(500.0, vec!["/test/*"], 10, 1000.0);
        let mut load_shedder = layer.layer(service);

        let request = Request::builder()
            .uri("/test/path")
            .body(Body::empty())
            .unwrap();

        let response = load_shedder.call(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_load_shedding_concurrency_handling() {
        let service = service_fn(|_req: Request<Body>| async {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            Ok::<_, hyper::Error>(Response::new(Body::from("Hello, World!")))
        });

        let layer = PathfinderLoadShedderLayer::new(240.0, vec!["/test/*"], 2, 10000.0);
        let mut load_shedder = layer.layer(service);

        let request1 = Request::builder()
            .uri("/test/path")
            .body(Body::empty())
            .unwrap();

        let request2 = Request::builder()
            .uri("/test/path")
            .body(Body::empty())
            .unwrap();

        let request3 = Request::builder()
            .uri("/test/path")
            .body(Body::empty())
            .unwrap();

        let request4 = Request::builder()
            .uri("/test/path")
            .body(Body::empty())
            .unwrap();

        let request5 = Request::builder()
            .uri("/test/path")
            .body(Body::empty())
            .unwrap();

        let request6 = Request::builder()
            .uri("/test/path")
            .body(Body::empty())
            .unwrap();

        // call them all at once

        let response1 = load_shedder.call(request1);
        let response2 = load_shedder.call(request2);
        let response3 = load_shedder.call(request3);
        let response4 = load_shedder.call(request4);
        let response5 = load_shedder.call(request5);
        let response6 = load_shedder.call(request6);

        let (response1, response2, response3, response4, response5, response6) =
            tokio::try_join!(response1, response2, response3, response4, response5, response6)
                .unwrap();

        println!("Response 1: {:?}", response1);
        println!("Response 2: {:?}", response2);
        println!("Response 3: {:?}", response3);
        println!("Response 4: {:?}", response4);
        println!("Response 5: {:?}", response5);
        println!("Response 6: {:?}", response6);

        assert_eq!(response1.status(), StatusCode::OK);
        assert_eq!(response2.status(), StatusCode::OK);
        assert_eq!(response2.status(), StatusCode::OK);
        assert_eq!(response2.status(), StatusCode::OK);
        assert_eq!(response5.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response6.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn test_latency_capping() {
        let service = service_fn(|_req: Request<Body>| async {
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
            Ok::<_, hyper::Error>(Response::new(Body::from("Hello, World!")))
        });

        let layer = PathfinderLoadShedderLayer::new(240.0, vec!["/test/*"], 2, 1000.0);

        let request = Request::builder()
            .uri("/test/path")
            .body(Body::empty())
            .unwrap();

        // latency registered should 240 ms - 1 ms, because the longest a request's latency can be is the max time allowed minus 1 ms.
        // This avoids lock ups where no requests can be made as the average latency to load the page is already over the max allowed.

        let mut load_shedder = layer.layer(service);

        let response = load_shedder.call(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let paths = load_shedder.paths.read().unwrap();

        let latency = paths.get_path_average_latency("/test/*");

        assert_eq!(latency, 239.0);
    }

    #[tokio::test]
    async fn test_load_shedding_latency_calculation() {
        let service = service_fn(|_req: Request<Body>| async {
            Ok::<_, hyper::Error>(Response::new(Body::from("Hello, World!")))
        });

        let layer = PathfinderLoadShedderLayer::new(500.0, vec!["/test/*"], 1, 1000.0);
        let mut load_shedder = layer.layer(service);

        let request = Request::builder()
            .uri("/test/path")
            .body(Body::empty())
            .unwrap();

        let response = load_shedder.call(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let paths = load_shedder.paths.read().unwrap();
        let latency = paths.get_path_average_latency("/test/*");
        assert!(latency > 0.0);
    }

    #[tokio::test]
    async fn test_load_shedding_request_tracking() {
        let service = service_fn(|_req: Request<Body>| async {
            Ok::<_, hyper::Error>(Response::new(Body::from("Hello, World!")))
        });

        let layer = PathfinderLoadShedderLayer::new(500.0, vec!["/test/*"], 1, 1000.0);
        let mut load_shedder = layer.layer(service);

        let request = Request::builder()
            .uri("/test/path")
            .body(Body::empty())
            .unwrap();

        {
            let currently_requested_paths = load_shedder.currently_requested_paths.read().unwrap();
            assert!(currently_requested_paths
                .get_currently_requested_paths()
                .is_empty());
        }

        let response = load_shedder.call(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        {
            let currently_requested_paths = load_shedder.currently_requested_paths.read().unwrap();
            assert!(currently_requested_paths
                .get_currently_requested_paths()
                .is_empty());
        }
    }

    #[tokio::test]
    async fn test_timeout() {
        let service = service_fn(|_req: Request<Body>| async {
            tokio::time::sleep(tokio::time::Duration::from_millis(2000)).await;
            Ok::<_, hyper::Error>(Response::new(Body::from("Hello, World!")))
        });

        let layer = PathfinderLoadShedderLayer::new(500.0, vec!["/test/*"], 10, 1000.0);
        let mut load_shedder = layer.layer(service);

        let request = Request::builder()
            .uri("/test/path")
            .body(Body::empty())
            .unwrap();

        let response = load_shedder.call(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn test_refresh_burst() {
        let service = service_fn(|_req: Request<Body>| async {
            // Simulate some processing time
            tokio::time::sleep(tokio::time::Duration::from_millis(4000)).await;
            Ok::<_, hyper::Error>(Response::new(Body::from("Hello, World!")))
        });

        let layer = PathfinderLoadShedderLayer::new(10000.0, vec!["/test/*"], 10, 20000.0);
        let load_shedder = layer.layer(service);

        let mut handles = vec![];

        for i in 0..30 {
            let request = Request::builder()
                .uri("/test/path")
                .body(Body::empty())
                .unwrap();

            let mut shedder_clone = load_shedder.clone();

            let handle = tokio::spawn(async move {
                let response = shedder_clone.call(request).await;
                println!("Request {}: {:?}", i, response);
                response
            });

            handles.push(handle);
        }

        // Await all the handles
        let results = futures::future::join_all(handles).await;

        // Filter down to unique results, there should be two, 200 and 503
        let mut response_types: Vec<_> = results
            .into_iter()
            .map(|result| match result {
                Ok(Ok(response)) => {
                    if response.status() == StatusCode::OK {
                        "OK"
                    } else {
                        "ERROR"
                    }
                }
                Ok(Err(_)) => "ERROR",
                Err(_) => "ERROR",
            })
            .collect();

        println!("Response types: {:?}", response_types);

        // Sort and deduplicate the vector
        response_types.sort();
        response_types.dedup();

        assert!(
            response_types.contains(&"OK") && response_types.contains(&"ERROR"),
            "Expected both OK and ERROR responses, got: {:?}",
            response_types
        );

        // run the whole thing again to make sure its functional again after

        println!("Running the test again to make sure it works after the burst");

        let mut handles = vec![];

        for i in 0..30 {
            let request = Request::builder()
                .uri("/test/path")
                .body(Body::empty())
                .unwrap();

            let mut shedder_clone = load_shedder.clone();

            let handle = tokio::spawn(async move {
                let response = shedder_clone.call(request).await;
                println!("Request {}: {:?}", i, response);
                response
            });

            handles.push(handle);
        }

        // Await all the handles
        let results = futures::future::join_all(handles).await;

        // Filter down to unique results, there should be two, 200 and 503
        let mut response_types: Vec<_> = results
            .into_iter()
            .map(|result| match result {
                Ok(Ok(response)) => {
                    if response.status() == StatusCode::OK {
                        "OK"
                    } else {
                        "ERROR"
                    }
                }
                Ok(Err(_)) => "ERROR",
                Err(_) => "ERROR",
            })
            .collect();

        println!("Response types: {:?}", response_types);

        // Sort and deduplicate the vector
        response_types.sort();
        response_types.dedup();

        assert!(
            response_types.contains(&"OK") && response_types.contains(&"ERROR"),
            "Expected both OK and ERROR responses, got: {:?}",
            response_types
        );
    }
}
