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

use std::fs::File;
use std::io::Write;
use std::time::Duration;

use tokio::sync::Semaphore;
use tokio::time::timeout;
use tower::{Layer, Service};

#[derive(Debug, Default)]
struct Metrics {
    requests: Vec<MetricRequest>,
}

#[derive(Debug, PartialEq)]
enum RequestResult {
    Fulfilled,
    Dropped,
    TimedOut,
}

#[derive(Debug)]
struct MetricRequest {
    path: String,
    path_transformed: String,
    start_time: Instant,
    finish_time: Instant,
    result: RequestResult,
    average_path_latency: f64,
}

impl Metrics {
    fn new() -> Self {
        Self {
            requests: Vec::new(),
        }
    }

    fn add_request(
        &mut self,
        path: String,
        path_transformed: String,
        start_time: Instant,
        finish_time: Instant,
        result: RequestResult,
        average_path_latency: f64,
    ) {
        self.requests.push(MetricRequest {
            path,
            path_transformed,
            start_time,
            finish_time,
            result,
            average_path_latency,
        });
    }
}

async fn save_metrics_to_csv(metrics: &Metrics) {
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;
    let mut file = File::create("metrics.csv").unwrap();
    file.write_all(
        b"path, path_transformed, start_time, finish_time, fulfilled, average_path_latency\n",
    )
    .unwrap();
    for request in &metrics.requests {
        let start_time = SystemTime::now() - request.start_time.elapsed();
        let finish_time = SystemTime::now() - request.finish_time.elapsed();

        let start_time_ms = start_time.duration_since(UNIX_EPOCH).unwrap().as_millis();
        let finish_time_ms = finish_time.duration_since(UNIX_EPOCH).unwrap().as_millis();

        let _ = file.write_all(
            format!(
                "{}, {}, {}, {}, {:?}, {}\n",
                request.path,
                request.path_transformed,
                start_time_ms,
                finish_time_ms,
                request.result,
                request.average_path_latency
            )
            .as_bytes(),
        );
    }
}

enum MetricType {
    AveragePathLatency,
    FulfilledVsDroppedVsTimedOut,
}

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

        for (path, count) in currently_requested_paths {
            let avg_latency = paths.get_path_average_latency(&path);
            total_latency += avg_latency * count as f64;
        }

        println!(
            "Total latency of currently requested paths: {}",
            total_latency
        );
        total_latency
    }
}

use lazy_static::lazy_static;
use std::sync::Mutex;

lazy_static! {
    pub static ref PATHFINDER_503_BODY: Mutex<&'static str> = Mutex::new("");
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
                    // timeout should affect all routes, so it's here as well.
                    let res = timeout(timeout_duration, fut).await;

                    match res {
                        Ok(Ok(response)) => Ok(response),
                        Ok(Err(err)) => Err(err),
                        Err(_) => Ok(Response::builder()
                            .status(StatusCode::GATEWAY_TIMEOUT)
                            .body("Request timed out".into())
                            .unwrap()),
                    }
                });
            }
        };

        let custom_503_body: &str = &PATHFINDER_503_BODY.lock().expect("503 body not set");

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
                        .body(custom_503_body.into())
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
                    .body(custom_503_body.into())
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
                    .status(StatusCode::GATEWAY_TIMEOUT)
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

    pub fn set_pathfinder_503_body(&self, body: &'static str) -> Self {
        *PATHFINDER_503_BODY.lock().unwrap() = body;
        self.clone()
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
    use plotters::prelude::*;
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
        assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
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

    #[tokio::test]
    async fn test_load_shedding_with_work_simulation() {
        use rand::Rng;
        let service = service_fn(|_req: Request<Body>| async {
            // generate random amount of work
            let duration_ms = rand::thread_rng().gen_range(1000..2000);
            simulate_work(duration_ms).await;
            Ok::<_, hyper::Error>(Response::new(Body::from("Hello, World!")))
        });

        let layer = PathfinderLoadShedderLayer::new(2500.0, vec!["/test/*"], 10, 5000.0);
        let mut load_shedder = layer.layer(service);

        let request = Request::builder()
            .uri("/test/path")
            .body(Body::empty())
            .unwrap();

        let response = load_shedder.call(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    // #test1
    #[tokio::test]
    async fn test_load_shedding_with_random_requests() {
        use rand::Rng;
        use std::sync::Mutex;

        let service = service_fn(|_req: Request<Body>| async {
            let duration_ms = rand::thread_rng().gen_range(500..6000); // Increase the lower bound
            simulate_work(duration_ms).await;
            Ok::<_, hyper::Error>(Response::new(Body::from("Hello, World!")))
        });

        let layer =
            PathfinderLoadShedderLayer::new(2500.0, vec!["/test/*", "/random/*"], 2, 5000.0);
        let load_shedder = layer.layer(service);

        let pathfinder_paths = load_shedder.paths.clone();

        let paths = vec![
            "/test/path1",
            "/test/path2",
            "/test/path3",
            "/random/path1",
            "/random/path2",
        ];

        let metrics = Arc::new(Mutex::new(Metrics::new()));

        let mut handles = vec![];

        // do the next part 10 times but sleep a randomt ime in between

        for _ in 0..100 {
            for i in 0..2 {
                let pathfinder_paths = pathfinder_paths.clone();
                let path: &str = paths[rand::thread_rng().gen_range(0..paths.len())];
                let path_transformed = layer
                    .pathfinder_path_transforms
                    .normalize_path(path)
                    .unwrap();

                let request = Request::builder().uri(path).body(Body::empty()).unwrap();

                let mut shedder_clone = load_shedder.clone();
                let metrics_clone = metrics.clone();
                let path_clone = path.to_string();

                println!("Requesting path: {}", path);
                println!("Normalized path: {}", path_transformed);

                let average_path_latency = pathfinder_paths
                    .clone()
                    .read()
                    .unwrap()
                    .get_path_average_latency(&path_transformed);

                let handle = tokio::spawn(async move {
                    let start_time = Instant::now();
                    // println!("Start time: {:?}", start_time);
                    let response = shedder_clone.call(request).await;
                    let finish_time = Instant::now();
                    // println!("Finish time: {:?}", finish_time);

                    let result = response
                        .as_ref()
                        .map(|r| match r.status() {
                            StatusCode::OK => RequestResult::Fulfilled,
                            StatusCode::SERVICE_UNAVAILABLE => RequestResult::Dropped,
                            StatusCode::GATEWAY_TIMEOUT => RequestResult::TimedOut,
                            _ => RequestResult::TimedOut,
                        })
                        .unwrap_or(RequestResult::TimedOut);

                    println!("Updating metrics for path: {}", path_clone);
                    metrics_clone.lock().unwrap().add_request(
                        path_clone.clone(),
                        path_transformed,
                        start_time,
                        finish_time,
                        result,
                        average_path_latency,
                    );

                    println!("Request {}: {:?}", i, response);
                    response
                });

                handles.push(handle);
            }

            // sleep a random amount of time
            let duration_ms = rand::thread_rng().gen_range(0..1000); // Increase the lower bound
            simulate_work(duration_ms).await;
        }

        let results = futures::future::join_all(handles).await;

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

        response_types.sort();
        response_types.dedup();

        let metrics = metrics.lock().unwrap();
        save_metrics_to_csv(&metrics).await;
        plot_metrics(
            &metrics,
            MetricType::AveragePathLatency,
            "average_path_latency.png",
        )
        .unwrap();
        plot_metrics(
            &metrics,
            MetricType::FulfilledVsDroppedVsTimedOut,
            "fulfilled_vs_dropped.png",
        )
        .unwrap();

        assert!(
            response_types.contains(&"OK") && response_types.contains(&"ERROR"),
            "Expected both OK and ERROR responses, got: {:?}",
            response_types
        );
    }

    #[tokio::test]
    async fn test_setting_503_body() {
        // Create a service that simulates a high load condition by always returning 503
        let service = service_fn(|_req: Request<Body>| async {
            // sleep for ten seconds so it 503s every time
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            Ok::<_, hyper::Error>(
                Response::builder()
                    .status(StatusCode::SERVICE_UNAVAILABLE)
                    .body("Shouldn't see this, as it should load shed this response".into())
                    .unwrap(),
            )
        });

        let custom_html_503_body = r#"
        <html>
            <head>
                <title>Service Unavailable</title>
            </head>
            <body>
                <h1>503 Service Unavailable</h1>
                <p>Sorry, our server is currently under heavy load. Please try again later.</p>
            </body>
        </html>
        "#;

        let layer = PathfinderLoadShedderLayer::new(500.0, vec!["/test/*"], 1, 1000000.0)
            .set_pathfinder_503_body(custom_html_503_body);

        let mut load_shedder = layer.layer(service);

        let request = Request::builder()
            .uri("/test/path")
            .body(Body::empty())
            .unwrap();

        // call it twice so it knows average path latency to load shed

        let _ = load_shedder.call(request).await.unwrap();

        let request1 = Request::builder()
            .uri("/test/path")
            .body(Body::empty())
            .unwrap();

        let request2 = Request::builder()
            .uri("/test/path")
            .body(Body::empty())
            .unwrap();

        // call both at once so it load sheds

        let request_out_1 = load_shedder.call(request1);
        let request_out_2 = load_shedder.call(request2);

        let (_, response2) = tokio::try_join!(request_out_1, request_out_2).unwrap();

        // see if the body matches

        let body = axum::body::to_bytes(response2.into_body(), 10000000000)
            .await
            .unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();

        println!("Response body: {}", body);

        assert_eq!(body, custom_html_503_body);
    }

    fn plot_metrics(
        metrics: &Metrics,
        metric_type: MetricType,
        file_name: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        use std::time::SystemTime;
        use std::time::UNIX_EPOCH;
        let root_area = BitMapBackend::new(file_name, (800, 600)).into_drawing_area();
        root_area.fill(&WHITE)?;

        // Convert Instant to SystemTime for plotting
        let system_start_time = SystemTime::now();
        let min_start_time: SystemTime = metrics
            .requests
            .iter()
            .map(|r| system_start_time - r.start_time.elapsed())
            .min()
            .unwrap_or(SystemTime::now());
        let max_finish_time: SystemTime = metrics
            .requests
            .iter()
            .map(|r| system_start_time - r.finish_time.elapsed())
            .max()
            .unwrap_or(SystemTime::now());

        let min_start_time_sec = min_start_time
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();
        let max_finish_time_sec = max_finish_time
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();

        match metric_type {
            MetricType::AveragePathLatency => {
                fn x_label_formatter(unix_epoch_time: f64, start_time: f64) -> String {
                    let elapsed_time = unix_epoch_time - start_time;
                    format!("{:.2}", elapsed_time)
                }

                let mut path_latencies = HashMap::new();

                for request in metrics.requests.iter() {
                    if request.average_path_latency > 0.0 {
                        path_latencies
                            .entry(&request.path_transformed)
                            .or_insert_with(Vec::new)
                            .push((
                                system_start_time
                                    .duration_since(UNIX_EPOCH)
                                    .unwrap()
                                    .as_secs_f64()
                                    - request.finish_time.elapsed().as_secs_f64(),
                                request.average_path_latency,
                            ));
                    }
                }

                let max_latency = path_latencies
                    .iter()
                    .map(|(_, latencies)| {
                        latencies
                            .iter()
                            .map(|(_, latency)| *latency)
                            .max_by(|a, b| a.partial_cmp(b).unwrap())
                            .unwrap()
                    })
                    .max_by(|a, b| a.partial_cmp(b).unwrap())
                    .unwrap();

                let mut ctx = ChartBuilder::on(&root_area)
                    .x_label_area_size(40)
                    .y_label_area_size(80)
                    .caption("Average Path Latency", ("sans-serif", 40))
                    .build_cartesian_2d(
                        min_start_time_sec..max_finish_time_sec,
                        0.0..(max_latency * 1.05),
                    )?;

                ctx.configure_mesh()
                    .x_desc("Seconds")
                    .y_desc("Latency (ms)")
                    .axis_desc_style(("sans-serif", 20).into_font())
                    .x_label_formatter(&|x| x_label_formatter(*x, min_start_time_sec))
                    .draw()?;

                let colors = vec![
                    &RED, &BLUE, &GREEN, &YELLOW, &CYAN, &MAGENTA, &BLACK, &WHITE,
                ];

                for (i, (path, latencies)) in path_latencies.iter().enumerate() {
                    let color = colors[i % colors.len()];

                    let smoothed_latencies = latencies
                        .windows(10)
                        .map(|window| {
                            let sum: f64 = window.iter().map(|(_, latency)| *latency).sum();
                            let count = window.len();
                            let avg_time =
                                window.iter().map(|(time, _)| *time).sum::<f64>() / count as f64;
                            (avg_time, sum / count as f64)
                        })
                        .collect::<Vec<_>>();

                    ctx.draw_series(LineSeries::new(smoothed_latencies, color))?
                        .label(path.to_owned())
                        .legend(|(x, y)| {
                            PathElement::new(vec![(x, y), (x + 20, y)], color.clone())
                        });
                }

                ctx.configure_series_labels()
                    .background_style(&WHITE.mix(0.8))
                    .border_style(&BLACK)
                    .draw()?;
            }
            MetricType::FulfilledVsDroppedVsTimedOut => {
                fn x_label_formatter(unix_epoch_time: f64, start_time: f64) -> String {
                    let elapsed_time = unix_epoch_time - start_time;
                    format!("{:.2}", elapsed_time)
                }

                let mut fulfilled_count = 0;
                let mut dropped_count = 0;
                let mut timed_out_count = 0;

                let mut fulfilled_data = Vec::new();
                let mut dropped_data = Vec::new();
                let mut timed_out_data = Vec::new();

                for request in metrics.requests.iter() {
                    let elapsed_time = system_start_time
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs_f64()
                        - request.finish_time.elapsed().as_secs_f64();
                    match request.result {
                        RequestResult::Fulfilled => fulfilled_count += 1,
                        RequestResult::Dropped => dropped_count += 1,
                        RequestResult::TimedOut => timed_out_count += 1,
                    }
                    fulfilled_data.push((elapsed_time, fulfilled_count as f64));
                    dropped_data.push((elapsed_time, dropped_count as f64));
                    timed_out_data.push((elapsed_time, timed_out_count as f64));
                }

                let mut ctx = ChartBuilder::on(&root_area)
                    .x_label_area_size(80)
                    .y_label_area_size(80)
                    .caption("Fulfilled vs Dropped vs Timed Out", ("sans-serif", 40))
                    .build_cartesian_2d(
                        min_start_time_sec..max_finish_time_sec,
                        0.0..(metrics.requests.len() + 10) as f64,
                    )?;

                ctx.configure_mesh()
                    .x_desc("Seconds")
                    .y_desc("Requests")
                    .axis_desc_style(("sans-serif", 20).into_font()) // Adjust the font size here
                    .x_label_formatter(&|x| x_label_formatter(*x, min_start_time_sec))
                    .draw()?;

                ctx.draw_series(LineSeries::new(dropped_data, &BLUE))?
                    .label("Dropped")
                    .legend(|(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], &BLUE));

                ctx.draw_series(LineSeries::new(timed_out_data, &GREEN))?
                    .label("Timed Out")
                    .legend(|(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], &GREEN));

                ctx.draw_series(LineSeries::new(fulfilled_data, &RED))?
                    .label("Fulfilled")
                    .legend(|(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], &RED));

                ctx.configure_series_labels()
                    .background_style(&WHITE.mix(0.8))
                    .border_style(&BLACK)
                    .draw()?;
            }
        }

        root_area.present().expect("Unable to save the file");

        Ok(())
    }

    async fn simulate_work(duration_ms: u64) -> () {
        use tokio::time::sleep;
        sleep(Duration::from_millis(duration_ms)).await
    }
}
