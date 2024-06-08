use arrayvec::ArrayVec;
use axum::body::to_bytes;
use axum::body::Body;
use hyper::{Request, Response, StatusCode};
use std::path;
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Instant,
};
use tokio::sync::RwLock;
use uuid::Uuid;

use tokio::task::LocalSet;

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
    average_path_latency: f32,
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
        average_path_latency: f32,
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
pub struct PathfinderPath {
    pub path: &'static str,
    pub initialization_latency_ms: f32,
}

#[derive(Clone, Debug)]
struct PathfinderPathTransforms {
    path_latencies: Vec<PathfinderPath>,
}

impl PathfinderPathTransforms {
    fn new(path_latencies: Vec<PathfinderPath>) -> Self {
        Self { path_latencies }
    }

    fn normalize_path(&self, path: &str) -> Option<String> {
        for pathfinder_path in &self.path_latencies {
            if let Some(normalized_path) = self.match_to_pattern(path, pathfinder_path.path) {
                return Some(normalized_path);
            }
        }
        None
    }

    fn match_to_pattern(&self, path: &str, pattern: &str) -> Option<String> {
        let path_parts: Vec<&str> = path.split('/').collect();
        let pattern_parts: Vec<&str> = pattern.split('/').collect();

        if pattern_parts.len() != path_parts.len() && !pattern_parts.contains(&"*") {
            return None;
        }

        let mut normalized_path = String::new();

        for (i, (pattern_part, path_part)) in
            pattern_parts.iter().zip(path_parts.iter()).enumerate()
        {
            if i > 0 {
                normalized_path.push('/');
            }

            if *pattern_part == "*" {
                normalized_path.push('*');
            } else if pattern_part == path_part {
                normalized_path.push_str(path_part);
            } else {
                return None;
            }
        }

        Some(normalized_path)
    }
}

#[derive(Debug)]
struct PathfinderPathLatencies {
    path_latencies: HashMap<String, Vec<f32>>,
}

impl PathfinderPathLatencies {
    fn update_path_latency(&mut self, path: &str, latency: f32) {
        println!("Updating latency for path: {}", path);
        self.path_latencies
            .entry(path.to_string())
            .or_insert_with(Vec::new)
            .push(latency);

        // remove if over 50 latencies
        if let Some(latencies) = self.path_latencies.get_mut(path) {
            if latencies.len() > 50 {
                latencies.remove(0);
            }
        }

        println!(
            "Updated latencies for path {}: {:?}",
            path,
            self.path_latencies.get(path)
        );
    }

    fn get_path_average_latency(&self, path: &str) -> f32 {
        let average_latency = self.path_latencies.get(path).map_or(0.0, |latencies| {
            let sum: f32 = latencies.iter().sum();
            let count = latencies.len();
            sum / count as f32
        });

        println!("Average latency for path {}: {}", path, average_latency);
        average_latency
    }
}

use lazy_static::lazy_static;
use std::sync::Mutex;

lazy_static! {
    pub static ref PATHFINDER_503_BODY: Mutex<&'static str> = Mutex::new("");
}

use axum::body::Bytes;
use http::request::Parts;

#[derive(Debug, Clone)]
struct QueuedRequest {
    uuid: Uuid,
    body: Bytes,
    parts: Parts,
    expected_average_latency: f32,
    expected_time_until_processed: f32,
}

#[derive(Debug, Clone)]
pub struct PathfinderLoadShedder<S> {
    max_expected_time_until_processed: f32,
    semaphore: Arc<Semaphore>,
    path_latencies: Arc<RwLock<PathfinderPathLatencies>>,
    pathfinder_path_transforms: PathfinderPathTransforms,
    inner: S,
    timeout_duration_ms: f32,
    queue: Arc<RwLock<ArrayVec<QueuedRequest, 1000>>>,
}

impl<S> PathfinderLoadShedder<S> {
    fn new(
        max_expected_time_until_processed: f32,
        pathfinder_path_transforms: PathfinderPathTransforms,
        inner: S,
        max_concurrent_requests: usize,
        timeout_duration_ms: f32,
    ) -> Self {
        // initlaize the latencies by mapping path_latencies to the pathfinder_path_transforms

        let mut path_latencies = PathfinderPathLatencies {
            path_latencies: HashMap::new(),
        };

        for path in &pathfinder_path_transforms.path_latencies {
            path_latencies
                .path_latencies
                .insert(path.path.to_string(), vec![path.initialization_latency_ms]);
        }

        println!("Initialized latencies: {:?}", path_latencies);

        Self {
            max_expected_time_until_processed,
            path_latencies: Arc::new(RwLock::new(path_latencies)),
            pathfinder_path_transforms,
            inner,
            semaphore: Arc::new(Semaphore::new(max_concurrent_requests)),
            timeout_duration_ms,
            queue: Arc::new(RwLock::new(ArrayVec::new())),
        }
    }
}

#[derive(Clone)]
pub struct PathfinderLoadShedderLayer {
    max_expected_time_until_processed: f32,
    pathfinder_path_transforms: PathfinderPathTransforms,
    max_concurrent_requests: usize,
    timeout_duration_ms: f32,
}

impl PathfinderLoadShedderLayer {
    pub fn new(
        max_expected_time_until_processed: f32,
        pathfinder_path_transforms: Vec<PathfinderPath>,
        max_concurrent_requests: usize,
        timeout_duration_ms: f32,
    ) -> Self {
        Self {
            max_expected_time_until_processed,
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
            self.max_expected_time_until_processed,
            self.pathfinder_path_transforms.clone(),
            inner,
            self.max_concurrent_requests,
            self.timeout_duration_ms,
        )
    }
}

impl<S> Service<Request<Body>> for PathfinderLoadShedder<S>
where
    // I added the ability to + clone the service here... not sure if that is wise.
    S: Service<Request<Body>, Response = Response<Body>> + Clone + Send + Sync + 'static,
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
        // determine whether or not we skip the pathfinder loadshedder completely - if no paths match, we skip
        let path = req.uri().path().to_string();
        let timeout_duration: Duration = Duration::from_millis(self.timeout_duration_ms as u64);

        let normalized_path = match self.pathfinder_path_transforms.normalize_path(&path) {
            Some(p) => p,
            None => {
                let fut = self.inner.call(req);
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

        // the semaphore set to 1 keeps it basically sync from here on out
        // turn the body into Vec<u8> or prepare to meet hell since Axum's body isn't clone (since it's a socket or something, I dunno)

        let semaphore = self.semaphore.clone();
        let queue_arc = self.queue.clone();
        let path_latencies = self.path_latencies.clone();
        let custom_503_body: &str = &PATHFINDER_503_BODY.lock().expect("503 body not set");
        let max_expected_time_until_processed = self.max_expected_time_until_processed;
        let inner = self.inner.clone();

        Box::pin(async move {
            let mut inner = inner.clone();
            let uuid = Uuid::new_v4();
            let path_latencies = path_latencies.clone();

            let (parts, body) = req.into_parts();
            let body_bytes = to_bytes(body, 0).await;
            let body_bytes = match body_bytes {
                Ok(bytes) => bytes,
                Err(_) => {
                    return Ok(Response::builder()
                        .status(StatusCode::PAYLOAD_TOO_LARGE)
                        .body("".into())
                        .unwrap());
                }
            };

            // Initial request comes in, and basically only affects the queue

            {
                let mut queue = queue_arc.write().await;
                if queue.len() >= 1000 {
                    queue.pop();
                    return Ok(Response::builder()
                        .status(StatusCode::SERVICE_UNAVAILABLE)
                        .body(custom_503_body.into())
                        .unwrap());
                }

                let expected_path_latency = path_latencies
                    .read()
                    .await
                    .get_path_average_latency(&normalized_path);

                queue.push(QueuedRequest {
                    uuid,
                    body: body_bytes.clone(),
                    parts: parts.clone(),
                    expected_average_latency: expected_path_latency,
                    expected_time_until_processed: expected_path_latency, // this is the default initial value
                });

                println!("Queue length: {}", queue.len());

                let mut expected_time_until_processed = 0.0;
                for request in queue.iter_mut() {
                    // println!(
                    //     "Uuid: {}: Expected time until processing: {}, expected_average_latency: {}",
                    //     request.uuid,
                    //     expected_time_until_processed, request.expected_average_latency
                    // );
                    expected_time_until_processed += request.expected_average_latency;
                    request.expected_time_until_processed = expected_time_until_processed;
                }

                // I think we should also just drop requests right here as well if the expected time is too high, as well as AFTER the semaphore.
            }

            // then when we have the semaphore, we can acquire it and then process the actual request

            let mut this_request: Option<QueuedRequest> = None;

            {
                let queue = queue_arc.write().await;

                this_request = queue.iter().find(|r| r.uuid == uuid).cloned();
            }

            let this_request = match this_request {
                Some(r) => r,
                None => {
                    return Ok(Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body("Request not found".into())
                        .unwrap());
                }
            };

            // THIS LOGIC EXISTS TWICE, ONCE BEFORE THE SEMAPHORE IS ACQUIRED, AND ONCE AFTER. THIS ALLOWS INSTANT 503.

            if this_request.expected_time_until_processed > max_expected_time_until_processed {
                let mut queue = queue_arc.write().await;
                let position = queue.iter().position(|r| r.uuid == uuid);

                println!("EXPECTED TIME TOO HIGH, RETURNING 503");

                if let Some(pos) = position {
                    queue.remove(pos);
                }

                // put the latency as 1 to avoid to avoid forever lockups

                // let average_path_latency = path_latencies
                //     .read()
                //     .await
                //     .get_path_average_latency(&normalized_path);

                // let average_path_latency_minus_10_percent =
                //     average_path_latency - (average_path_latency * 0.1);

                // path_latencies
                //     .write()
                //     .await
                //     .update_path_latency(&normalized_path, 1.0);

                return Ok(Response::builder()
                    .status(StatusCode::SERVICE_UNAVAILABLE)
                    .body(custom_503_body.into())
                    .unwrap());
            }

            let _ = semaphore.acquire().await.unwrap();

            // THIS LOGIC EXISTS TWICE, ONCE BEFORE THE SEMAPHORE IS ACQUIRED, AND ONCE AFTER. THIS ALLOWS FOR 503'ING IF CONDITIONS HAVE CHANGED WHILE WAITING FOR SEMAPHORE.

            if this_request.expected_time_until_processed > max_expected_time_until_processed {
                let mut queue = queue_arc.write().await;
                let position = queue.iter().position(|r| r.uuid == uuid);

                println!("EXPECTED TIME TOO HIGH, RETURNING 503");

                if let Some(pos) = position {
                    queue.remove(pos);
                }

                // get the average and then minus 10% to avoid forever lockups

                let average_path_latency = path_latencies
                    .read()
                    .await
                    .get_path_average_latency(&normalized_path);

                let average_path_latency_minus_10_percent =
                    average_path_latency - (average_path_latency * 0.1);

                path_latencies
                    .write()
                    .await
                    .update_path_latency(&normalized_path, average_path_latency_minus_10_percent);

                return Ok(Response::builder()
                    .status(StatusCode::SERVICE_UNAVAILABLE)
                    .body(custom_503_body.into())
                    .unwrap());
            }

            let new_req = Request::from_parts(
                this_request.parts.clone(),
                Body::from(this_request.body.clone()),
            );

            let start_time = Instant::now();

            let mut queue = queue_arc.write().await;

            let mut path_latencies = path_latencies.write().await;

            let res = timeout(timeout_duration, inner.call(new_req)).await;

            let position = queue.iter().position(|r| r.uuid == uuid);

            println!("Position: {:?}", position);

            if let Some(pos) = position {
                queue.remove(pos);
            }

            match res {
                Ok(Ok(response)) => {
                    path_latencies.update_path_latency(&normalized_path, {
                        // cap latency to avoid total system lockup if request takes super long time
                        let latency = start_time.elapsed().as_millis() as f32;
                        if latency >= max_expected_time_until_processed - 1.0 {
                            max_expected_time_until_processed - 1.0
                        } else {
                            latency
                        }
                    });

                    return Ok(response);
                }
                Ok(Err(err)) => {
                    println!(
                        "Pathfinder load shedder error in response occured: {:?}",
                        err.into()
                    );
                    // return internal server error
                    return Ok(Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body("Internal server error".into())
                        .unwrap());
                }
                Err(err) => {
                    println!("GATEWAY_TIMEOUT: {:?}", err);
                    return Ok(Response::builder()
                        .status(StatusCode::GATEWAY_TIMEOUT)
                        .body("Request timed out".into())
                        .unwrap());
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use hyper::{Request, Response, StatusCode};
    use plotters::prelude::*;
    use rand::Rng;
    use tower::{service_fn, Service};

    #[tokio::test]
    async fn test_normalize_path() {
        let paths = vec![
            "/location/average/*/*/*/*/*",
            "/location/*/*/*/*",
            "/property/*/*/*/*/*",
            "/search/*",
        ];

        let transformer = PathfinderPathTransforms::new(
            paths
                .iter()
                .map(|p| PathfinderPath {
                    path: p,
                    initialization_latency_ms: 100.0,
                })
                .collect(),
        );

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
            assert_eq!(
                transformer.normalize_path(&path.to_string()).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn test_update_path_latency() {
        let mut paths = PathfinderPathLatencies {
            path_latencies: HashMap::new(),
        };

        paths.update_path_latency("/test", 100.0);
        paths.update_path_latency("/test", 200.0);
        paths.update_path_latency("/test", 300.0);
        paths.update_path_latency("/test", 400.0);
        paths.update_path_latency("/test", 500.0);
        paths.update_path_latency("/test", 600.0);

        assert_eq!(
            paths.path_latencies.get("/test").unwrap(),
            &vec![100.0, 200.0, 300.0, 400.0, 500.0, 600.0]
        );

        let avg_latency = paths.get_path_average_latency("/test");
        assert_eq!(avg_latency, 350.0);
    }

    #[tokio::test]
    async fn test_load_shedding_normal_operation() {
        let service = service_fn(|_req: Request<Body>| async {
            Ok::<_, hyper::Error>(Response::new(Body::from("Hello, World!")))
        });

        let layer = PathfinderLoadShedderLayer::new(
            500.0,
            vec![PathfinderPath {
                path: "/test/*",
                initialization_latency_ms: 100.0,
            }],
            1,
            1000.0,
        );

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
            simulate_work(100).await;
            Ok::<_, hyper::Error>(Response::new(Body::from("Hello, World!")))
        });

        let layer = PathfinderLoadShedderLayer::new(
            240.0,
            vec![PathfinderPath {
                path: "/test/*",
                initialization_latency_ms: 100.0,
            }],
            2,
            10000.0,
        );

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
        assert_eq!(response3.status(), StatusCode::OK);
        assert_eq!(response4.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response5.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response6.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    // #[tokio::test]
    // async fn test_latency_capping() {
    //     let service = service_fn(|_req: Request<Body>| async {
    //         tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    //         Ok::<_, hyper::Error>(Response::new(Body::from("Hello, World!")))
    //     });

    //     let layer = PathfinderLoadShedderLayer::new(
    //         240.0,
    //         vec![PathfinderPath {
    //             path: "/test/*",
    //             initialization_latency_ms: 239.0,
    //         }],
    //         1,
    //         1000.0,
    //     );

    //     let request = Request::builder()
    //         .uri("/test/path")
    //         .body(Body::empty())
    //         .unwrap();

    //     // latency registered should 240 ms - 1 ms, because the longest a request's latency can be is the max time allowed minus 1 ms.
    //     // This avoids lock ups where no requests can be made as the average latency to load the page is already over the max allowed.

    //     let mut load_shedder = layer.layer(service);

    //     let response = load_shedder.call(request).await.unwrap();

    //     assert_eq!(response.status(), StatusCode::OK);

    //     let paths = load_shedder.path_latencies.read().await;

    //     let latency = paths.get_path_average_latency("/test/*");

    //     assert_eq!(latency, 239.0);
    // }

    #[tokio::test]
    async fn test_load_shedding_latency_calculation() {
        let service = service_fn(|_req: Request<Body>| async {
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            Ok::<_, hyper::Error>(Response::new(Body::from("Hello, World!")))
        });

        let layer = PathfinderLoadShedderLayer::new(
            500.0,
            vec![PathfinderPath {
                path: "/test/*",
                initialization_latency_ms: 100.0,
            }],
            1,
            1000.0,
        );
        let mut load_shedder = layer.layer(service);

        let request = Request::builder()
            .uri("/test/path")
            .body(Body::empty())
            .unwrap();

        let response = load_shedder.call(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let paths = load_shedder.path_latencies.read().await;
        let latency = paths.get_path_average_latency("/test/*");
        assert!(latency > 0.0);
    }

    #[tokio::test]
    async fn test_timeout() {
        let service = service_fn(|_req: Request<Body>| async {
            tokio::time::sleep(tokio::time::Duration::from_millis(2000)).await;
            Ok::<_, hyper::Error>(Response::new(Body::from("Hello, World!")))
        });

        let layer = PathfinderLoadShedderLayer::new(
            500.0,
            vec![PathfinderPath {
                path: "/test/*",
                initialization_latency_ms: 100.0,
            }],
            10,
            1000.0,
        );
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

        let layer = PathfinderLoadShedderLayer::new(
            10000.0,
            vec![PathfinderPath {
                path: "/test/*",
                initialization_latency_ms: 100.0,
            }],
            10,
            20000.0,
        );
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
                    println!("RETURNED STATUS: {}", response.status());
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
    async fn test_body_size_limit_exceeded() {
        let service = service_fn(|_req: Request<Body>| async {
            Ok::<_, hyper::Error>(Response::new(Body::from("Hello, World!")))
        });

        let layer = PathfinderLoadShedderLayer::new(
            500.0,
            vec![PathfinderPath {
                path: "/test/*",
                initialization_latency_ms: 100.0,
            }],
            1,
            1000.0,
        );

        let mut load_shedder = layer.layer(service);

        let large_body = vec![0; 1_000_000]; // 1 MB body

        let request = Request::builder()
            .uri("/test/path")
            .body(Body::from(large_body))
            .unwrap();

        let response = load_shedder.call(request).await.unwrap();

        // Verify that the response is a 413 Payload Too Large
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn test_load_shedding_with_real_world_conditions() {
        use axum::routing::get;
        use axum::Router;
        use reqwest::Client;
        use std::net::SocketAddr;
        use tokio::task::JoinHandle;
        use tokio::time::sleep;

        let layer = PathfinderLoadShedderLayer::new(
            1600.0,
            vec![
                PathfinderPath {
                    path: "/test/*",
                    initialization_latency_ms: 490.0,
                },
                PathfinderPath {
                    path: "/random/*",
                    initialization_latency_ms: 200.0,
                },
            ],
            2,
            1700.0,
        );

        // Start the Axum server
        let app = Router::new()
            .route(
                "/test/path1",
                get(|| async {
                    let duration = Duration::from_millis(rand::thread_rng().gen_range(1000..2000));
                    sleep(duration).await;
                    "Done with /test/path1"
                }),
            )
            .route(
                "/test/path2",
                get(|| async {
                    let duration = Duration::from_millis(rand::thread_rng().gen_range(500..1000));
                    sleep(duration).await;
                    "Done with /test/path2"
                }),
            )
            .route(
                "/random/path1",
                get(|| async {
                    let duration = Duration::from_millis(rand::thread_rng().gen_range(200..2000));
                    sleep(duration).await;
                    "Done with /random/path1"
                }),
            )
            .layer(layer.clone());

        let addr = SocketAddr::from(([127, 0, 0, 1], 12000));
        let tcp_listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
        let server = axum::serve(tcp_listener, app.into_make_service());

        let server_handle: JoinHandle<()> = tokio::spawn(async move {
            server.await.unwrap();
        });

        // Allow some time for the server to start
        sleep(Duration::from_secs(1)).await;

        // Prepare to make requests
        let client = Client::new();
        let paths = vec![
            "http://localhost:12000/test/path1",
            "http://localhost:12000/test/path2",
            "http://localhost:12000/random/path1",
        ];

        let results = Arc::new(Mutex::new(Vec::new()));

        let metrics = Arc::new(RwLock::new(Metrics::new()));
        let metrics_clone = metrics.clone();

        let request_number = 200;

        for _i in 0..request_number as u32 {
            let metrics_clone = metrics_clone.clone();
            let client_clone = client.clone();
            let request_delay = rand::thread_rng().gen_range(100..300);
            sleep(Duration::from_millis(request_delay)).await;

            let path = paths[rand::thread_rng().gen_range(0..paths.len())].to_string();

            let results_clone = Arc::clone(&results);

            tokio::spawn(async move {
                let path_clone = path.to_string();
                let start_time = Instant::now();
                let response = client_clone.get(&path).send().await;
                let finish_time = Instant::now();

                // do the metrics

                let result = response
                    .as_ref()
                    .map(|r| match r.status() {
                        StatusCode::OK => RequestResult::Fulfilled,
                        StatusCode::SERVICE_UNAVAILABLE => RequestResult::Dropped,
                        StatusCode::GATEWAY_TIMEOUT => RequestResult::TimedOut,
                        _ => panic!("Unexpected status code"),
                    })
                    .unwrap_or(RequestResult::TimedOut);

                println!("Updating metrics for path: {}", path_clone);

                metrics_clone.write().await.add_request(
                    path_clone.clone(),
                    "".to_string(),
                    start_time,
                    finish_time,
                    result,
                    0.0,
                );

                let mut results_lock = results_clone.lock().unwrap();
                results_lock.push(response);
            });
        }

        // wait till results is 100 in length
        while results.lock().unwrap().len() < request_number {
            sleep(Duration::from_millis(100)).await;
        }

        // Collect and print the results
        let response_types: Vec<_> = {
            let results_lock = results.lock().unwrap();
            results_lock
                .iter()
                .map(|result| match result {
                    Ok(response) => {
                        if response.status().is_success() {
                            "OK"
                        } else {
                            "ERROR"
                        }
                    }
                    Err(_) => "ERROR",
                })
                .collect()
        };

        println!("Response types: {:?}", response_types);

        // Sort and deduplicate response types
        let mut response_types = response_types;
        response_types.sort();
        response_types.dedup();

        println!("Unique Response types: {:?}", response_types);

        let metrics = metrics.read().await;
        save_metrics_to_csv(&metrics).await;

        plot_metrics(
            &metrics,
            MetricType::FulfilledVsDroppedVsTimedOut,
            "real_world_fulfilled_vs_dropped.png",
        )
        .unwrap();

        // Shutdown the server
        server_handle.abort();
    }

    #[tokio::test]
    async fn test_path_latency_initialization() {
        let paths = vec![
            PathfinderPath {
                path: "/location/average/*/*/*/*/*",
                initialization_latency_ms: 50.0,
            },
            PathfinderPath {
                path: "/location/*/*/*/*",
                initialization_latency_ms: 100.0,
            },
            PathfinderPath {
                path: "/property/*/*/*/*/*",
                initialization_latency_ms: 150.0,
            },
            PathfinderPath {
                path: "/search/*",
                initialization_latency_ms: 200.0,
            },
        ];

        let pathfinder_path_transforms = PathfinderPathTransforms::new(paths.clone());

        let load_shedder = PathfinderLoadShedder::new(
            500.0,
            pathfinder_path_transforms.clone(),
            service_fn(|_req: Request<Body>| async {
                Ok::<_, hyper::Error>(Response::new(Body::from("Hello, World!")))
            }),
            1,
            1000.0,
        );

        // Allow time for async initialization (if applicable)
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        let path_latencies = load_shedder.path_latencies.read().await;

        println!("Path latencies: {:?}", path_latencies);

        for path in paths {
            let latencies = path_latencies.path_latencies.get(path.path);
            assert!(
                latencies.is_some(),
                "Path {} not found in latencies",
                path.path
            );
            assert_eq!(
                latencies.unwrap(),
                &vec![path.initialization_latency_ms],
                "Incorrect initial latency for path {}",
                path.path
            );
        }
    }

    #[tokio::test]
    async fn test_latency_capping() {
        let service = service_fn(|_req: Request<Body>| async {
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
            Ok::<_, hyper::Error>(Response::new(Body::from("Hello, World!")))
        });

        let layer = PathfinderLoadShedderLayer::new(
            240.0,
            vec![PathfinderPath {
                path: "/test/*",
                initialization_latency_ms: 239.0,
            }],
            1,
            1000.0,
        );

        let request = Request::builder()
            .uri("/test/path")
            .body(Body::empty())
            .unwrap();

        // latency registered should 240 ms - 1 ms, because the longest a request's latency can be is the max time allowed minus 1 ms.
        // This avoids lock ups where no requests can be made as the average latency to load the page is already over the max allowed.

        let mut load_shedder = layer.layer(service);

        let response = load_shedder.call(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let paths = load_shedder.path_latencies.read().await;

        let latency = paths.get_path_average_latency("/test/*");

        assert_eq!(latency, 239.0);
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

        let layer = PathfinderLoadShedderLayer::new(
            250.0,
            vec![PathfinderPath {
                path: "/test/*",
                initialization_latency_ms: 200.0,
            }],
            1,
            1000000.0,
        )
        .set_pathfinder_503_body(custom_html_503_body);

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

        // call both at once so it load sheds

        let request_out_1 = load_shedder.call(request1);
        let request_out_2 = load_shedder.call(request2);
        let request_out_3 = load_shedder.call(request3);

        let (response1, response2, response3) =
            tokio::try_join!(request_out_1, request_out_2, request_out_3).unwrap();

        // see if the body matches

        let body = axum::body::to_bytes(response3.into_body(), 10000000000)
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
                fn x_label_formatter(unix_epoch_time: f32, start_time: f32) -> String {
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
                    // .x_label_formatter(&|x| x_label_formatter(*x as f32, min_start_time_sec as f32))
                    .draw()?;

                let colors = vec![
                    &RED, &BLUE, &GREEN, &YELLOW, &CYAN, &MAGENTA, &BLACK, &WHITE,
                ];

                for (i, (path, latencies)) in path_latencies.iter().enumerate() {
                    let color = colors[i % colors.len()];

                    let smoothed_latencies = latencies
                        .windows(10)
                        .map(|window| {
                            let sum: f32 = window.iter().map(|(_, latency)| *latency).sum();
                            let count = window.len();
                            let avg_time =
                                window.iter().map(|(time, _)| *time).sum::<f64>() / count as f64;
                            (avg_time, sum / count as f32)
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
                fn x_label_formatter(unix_epoch_time: f32, start_time: f32) -> String {
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
                    fulfilled_data.push((elapsed_time, fulfilled_count as f32));
                    dropped_data.push((elapsed_time, dropped_count as f32));
                    timed_out_data.push((elapsed_time, timed_out_count as f32));
                }

                let mut ctx = ChartBuilder::on(&root_area)
                    .x_label_area_size(80)
                    .y_label_area_size(80)
                    .caption("Fulfilled vs Dropped vs Timed Out", ("sans-serif", 40))
                    .build_cartesian_2d(
                        min_start_time_sec..max_finish_time_sec,
                        0.0..(metrics.requests.len() + 10) as f32,
                    )?;

                ctx.configure_mesh()
                    .x_desc("Seconds")
                    .y_desc("Requests")
                    .axis_desc_style(("sans-serif", 20).into_font()) // Adjust the font size here
                    // .x_label_formatter(&|x| x_label_formatter(*x as f32, min_start_time_sec as f32))
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

    pub async fn simulate_work(duration_ms: u64) -> () {
        use tokio::time::sleep;
        sleep(Duration::from_millis(duration_ms)).await
    }
}
