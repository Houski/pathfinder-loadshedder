use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
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
            if latencies.len() > 5 {
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

    fn get_average_latency_of_specified_paths(&self, paths: &[&str]) -> f64 {
        let mut sum = 0.0;
        let mut count = 0;
        for path in paths {
            if let Some(latencies) = self.paths.get(*path) {
                sum += latencies.iter().sum::<f64>();
                count += latencies.len();
            }
        }
        let average_latency = if count == 0 { 0.0 } else { sum / count as f64 };

        println!(
            "Average latency of specified paths {:?}: {}",
            paths, average_latency
        );
        average_latency
    }
}

#[derive(Debug, Clone)]
struct PathfinderCurrentlyRequestedPaths {
    currently_requested_paths: Arc<Mutex<HashSet<String>>>,
}

impl PathfinderCurrentlyRequestedPaths {
    fn new() -> Self {
        Self {
            currently_requested_paths: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    fn add_path(&self, path: &str) {
        println!("Adding path to currently requested paths: {}", path);
        self.currently_requested_paths
            .lock()
            .unwrap()
            .insert(path.to_string());
        println!(
            "Currently requested paths: {:?}",
            self.currently_requested_paths
        );
    }

    fn remove_path(&self, path: &str) {
        println!("Removing path from currently requested paths: {}", path);
        self.currently_requested_paths.lock().unwrap().remove(path);
        println!(
            "Currently requested paths: {:?}",
            self.currently_requested_paths
        );
    }

    fn get_currently_requested_paths(&self) -> Vec<String> {
        let paths = self
            .currently_requested_paths
            .lock()
            .unwrap()
            .iter()
            .cloned()
            .collect();
        println!("Getting currently requested paths: {:?}", paths);
        paths
    }

    fn get_average_latency_of_currently_requested_paths(&self, paths: &PathfinderPaths) -> f64 {
        let currently_requested_paths = self.get_currently_requested_paths();
        let average_latency = paths.get_average_latency_of_specified_paths(
            &currently_requested_paths
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<&str>>(),
        );

        println!(
            "Average latency of currently requested paths {:?}: {}",
            currently_requested_paths, average_latency
        );
        average_latency
    }
}

#[derive(Debug, Clone)]
pub struct PathfinderLoadShedder<S> {
    max_allowable_system_average_latency_ms: f64,
    currently_requested_paths: Arc<Mutex<PathfinderCurrentlyRequestedPaths>>,
    paths: Arc<Mutex<PathfinderPaths>>,
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
            paths: Arc::new(Mutex::new(PathfinderPaths {
                paths: HashMap::new(),
            })),
            currently_requested_paths: Arc::new(Mutex::new(
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

            // Add the path to currently requested paths
            currently_requested_paths
                .lock()
                .unwrap()
                .add_path(&normalized_path);

            let res = fut.await?;

            let latency = start.elapsed().as_secs_f64() * 1000.0;

            println!("Request for path {} took {} ms", normalized_path, latency);

            // Update path latency
            paths.lock().unwrap().update_path_latency(
                &normalized_path,
                // cap latency at half of max to avoid lockouts on overload where it is already over the max, so it always 503's. This allows for recovery.
                if latency > max_allowable_system_average_latency_ms / 2.0 {
                    max_allowable_system_average_latency_ms / 2.0
                } else {
                    latency
                },
            );

            // Get the average latency of currently requested paths
            let system_average_latency = currently_requested_paths
                .lock()
                .unwrap()
                .get_average_latency_of_currently_requested_paths(&*paths.lock().unwrap());

            // Get the average latency for this path
            let current_path_average_latency = paths
                .lock()
                .unwrap()
                .get_path_average_latency(&normalized_path);

            println!(
                "System average latency: {} ms, Current path average latency: {} ms",
                system_average_latency, current_path_average_latency
            );

            if (system_average_latency + current_path_average_latency)
                > max_allowable_system_average_latency_ms
            {
                println!(
                    "System under high load. Shedding request for path: {}",
                    normalized_path
                );
                // Remove the path from currently requested paths
                currently_requested_paths
                    .lock()
                    .unwrap()
                    .remove_path(&normalized_path);

                // Release the permit back to the semaphore
                drop(permit);

                // Return 503 response
                return Ok(Response::builder()
                    .status(StatusCode::SERVICE_UNAVAILABLE)
                    .body(Body::empty())
                    .unwrap());
            }

            // Remove the path from currently requested paths
            currently_requested_paths
                .lock()
                .unwrap()
                .remove_path(&normalized_path);

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

    #[test]
    fn test_normalize_path() {
        let paths = vec![
            "/location/average/*/*/*/*/*",
            "/location/*/*/*/*",
            "/property/*/*/*/*/*",
            "/search/*",
            // "/map",
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
            // ("/map?bbox_ne_lat=45.499623216059&bbox_ne_lng=-79.701366783941&bbox_sw_lat=45.481636783941006&bbox_sw_lng=-79.71935321605899&zoom=13", "/map"),
        ];

        for (path, expected) in test_paths {
            assert_eq!(transformer.normalize_path(path).unwrap(), expected);
        }
    }
}
