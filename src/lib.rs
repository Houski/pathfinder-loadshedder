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
use tokio::sync::Semaphore;
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
}

impl<S> PathfinderLoadShedder<S> {
    fn new(
        max_allowable_system_average_latency_ms: f64,
        pathfinder_path_transforms: PathfinderPathTransforms,
        inner: S,
        max_concurrent_requests: usize,
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
        let max_allowable_system_average_latency_ms = self.max_allowable_system_average_latency_ms;
        let fut = self.inner.call(req);
        let paths = self.paths.clone();
        let currently_requested_paths = self.currently_requested_paths.clone();
        let semaphore = self.semaphore.clone();

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
            println!("Handling request for path: {}", normalized_path);

            // Acquire a permit from the semaphore
            let permit = semaphore.acquire().await.unwrap();

            // Calculate the system average latency including the current request
            let system_average_latency;
            {
                let paths = paths.read().unwrap();
                let currently_requested_paths = currently_requested_paths.read().unwrap();
                system_average_latency = currently_requested_paths
                    .get_total_latency_of_currently_requested_paths(&*paths);
            }

            let current_path_average_latency;
            {
                let paths = paths.read().unwrap();
                current_path_average_latency = paths.get_path_average_latency(&normalized_path);
            }

            println!(
                "System average latency: {}, Current path average latency: {}, Max allowable system average latency: {}",
                system_average_latency, current_path_average_latency, max_allowable_system_average_latency_ms
            );

            // Check if the system is under high load including the current request
            if system_average_latency + current_path_average_latency
                > max_allowable_system_average_latency_ms
            {
                println!(
                    "System under high load. Shedding request for path: {}",
                    normalized_path
                );

                // Release the permit back to the semaphore
                drop(permit);

                // Return 503 response
                return Ok(Response::builder()
                    .status(StatusCode::SERVICE_UNAVAILABLE)
                    .body(Body::empty())
                    .unwrap());
            }

            // Add the path to currently requested paths
            {
                let currently_requested_paths = currently_requested_paths.read().unwrap();
                currently_requested_paths.add_path(&normalized_path);
            }

            let res = fut.await?;

            let latency = start.elapsed().as_secs_f64() * 1000.0;
            println!("Request for path {} took {} ms", normalized_path, latency);

            // max out the latency at half the max capacity to avoid situations where the latency is too high causing it to lock up and never recover
            let latency = match latency >= max_allowable_system_average_latency_ms - 1.0 {
                true => {
                    let capped_latency = max_allowable_system_average_latency_ms - 1.0;
                    println!(
                        "Request latency is higher than max allowable system average latency. Capping latency at {} ms to avoid lockup",
                        capped_latency
                    );
                    capped_latency
                }
                false => latency,
            };

            {
                let mut paths = paths.write().unwrap();
                paths.update_path_latency(&normalized_path, latency);
            }

            // Remove the path from currently requested paths
            {
                let currently_requested_paths = currently_requested_paths.read().unwrap();
                currently_requested_paths.remove_path(&normalized_path);
            }

            // Release the permit back to the semaphore
            drop(permit);

            Ok(res)
        })
    }
}

#[derive(Clone)]
pub struct PathfinderLoadShedderLayer {
    max_allowable_system_average_latency_ms: f64,
    pathfinder_path_transforms: PathfinderPathTransforms,
    max_concurrent_requests: usize,
}

impl PathfinderLoadShedderLayer {
    pub fn new(
        max_allowable_system_average_latency_ms: f64,
        pathfinder_path_transforms: Vec<&'static str>,
        max_concurrent_requests: usize,
    ) -> Self {
        Self {
            max_allowable_system_average_latency_ms,
            pathfinder_path_transforms: PathfinderPathTransforms::new(pathfinder_path_transforms),
            max_concurrent_requests,
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

        let layer = PathfinderLoadShedderLayer::new(500.0, vec!["/test/*"], 10);
        let mut load_shedder = layer.layer(service);

        let request = Request::builder()
            .uri("/test/path")
            .body(Body::empty())
            .unwrap();

        let response = load_shedder.call(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_load_shedding_overload() {
        let service = service_fn(|_req: Request<Body>| async {
            Ok::<_, hyper::Error>(Response::new(Body::from("Hello, World!")))
        });

        let layer = PathfinderLoadShedderLayer::new(500.0, vec!["/test/*"], 1);
        let mut load_shedder = layer.layer(service);

        let request = Request::builder()
            .uri("/test/path")
            .body(Body::empty())
            .unwrap();

        let response = load_shedder.call(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        {
            let mut paths = load_shedder.paths.write().unwrap();
            paths.update_path_latency("/test/*", 1000.0);
        }

        let request = Request::builder()
            .uri("/test/path")
            .body(Body::empty())
            .unwrap();

        let response = load_shedder.call(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn test_load_shedding_concurrency_handling() {
        let service = service_fn(|_req: Request<Body>| async {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            Ok::<_, hyper::Error>(Response::new(Body::from("Hello, World!")))
        });

        let layer = PathfinderLoadShedderLayer::new(240.0, vec!["/test/*"], 2);
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

        let layer = PathfinderLoadShedderLayer::new(240.0, vec!["/test/*"], 2);

        let request = Request::builder()
            .uri("/test/path")
            .body(Body::empty())
            .unwrap();

        // latency registered should be half of 240, so 120 max.

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

        let layer = PathfinderLoadShedderLayer::new(500.0, vec!["/test/*"], 1);
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

        let layer = PathfinderLoadShedderLayer::new(500.0, vec!["/test/*"], 1);
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
}
