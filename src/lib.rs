use axum::body::Body;
use hyper::{Request, Response, StatusCode};
use lazy_static::lazy_static;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::{
    sync::Arc,
    task::{Context, Poll},
};
use tokio::sync::Semaphore;
use tokio::time::timeout;
use tokio::time::Duration;
use tower::{Layer, Service};

lazy_static! {
    pub static ref PATHFINDER_503_BODY: Mutex<&'static str> = Mutex::new("");
}

#[derive(Clone, Debug)]
pub struct PathfinderPath {
    pub path: &'static str,
}

#[derive(Clone, Debug)]
struct PathfinderPathTransforms {
    paths: Vec<PathfinderPath>,
}

impl PathfinderPathTransforms {
    fn new(mut paths: Vec<PathfinderPath>) -> Self {
        paths.sort_by(|a, b| {
            b.path
                .matches('/')
                .count()
                .cmp(&a.path.matches('/').count())
        });
        Self { paths }
    }

    fn check_path_starts_with(&self, path: &str) -> Option<String> {
        let path_segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

        for pathfinder_path in &self.paths {
            let pathfinder_segments: Vec<&str> = pathfinder_path
                .path
                .split('/')
                .filter(|s| !s.is_empty())
                .collect();

            if pathfinder_segments.len() > path_segments.len() {
                continue;
            }

            let mut matched = true;
            for (ps, pps) in pathfinder_segments.iter().zip(path_segments.iter()) {
                if ps != pps {
                    matched = false;
                    break;
                }
            }

            if matched {
                return Some(pathfinder_path.path.to_string());
            }
        }

        None
    }
}

#[derive(Clone)]
pub struct PathfinderLoadShedder<S> {
    semaphore: Arc<Semaphore>,
    pathfinder_path_transforms: PathfinderPathTransforms,
    inner: S,
    timeout_ms: u64,
}

impl<S> PathfinderLoadShedder<S> {
    fn new(
        pathfinder_path_transforms: PathfinderPathTransforms,
        inner: S,
        max_concurrent_requests: usize,
        timeout_ms: u64,
    ) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_concurrent_requests)),
            pathfinder_path_transforms,
            inner,
            timeout_ms,
        }
    }
}

impl<S> Service<Request<Body>> for PathfinderLoadShedder<S>
where
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
        let path = req.uri().path().to_string();
        let mut inner = self.inner.clone();

        if self
            .pathfinder_path_transforms
            .check_path_starts_with(&path)
            .is_none()
        {
            return Box::pin(async move { inner.call(req).await });
        }

        let semaphore = self.semaphore.clone();
        let custom_503_body: &str = &PATHFINDER_503_BODY.lock().expect("503 body not set");
        let timeout_ms = self.timeout_ms;

        println!("PATHFINDER LOAD SHEDDER - Path: {}", path);

        println!(
            "PATHFINDER LOAD SHEDDER - Semaphores available pre-execution: {}",
            semaphore.available_permits()
        );

        Box::pin(async move {
            let permit = match semaphore.try_acquire() {
                Ok(permit) => permit,
                Err(_) => {
                    println!(
                        "PATHFINDER LOAD SHEDDER - No semaphores available, shedding load 503"
                    );
                    return Ok(Response::builder()
                        .status(StatusCode::SERVICE_UNAVAILABLE)
                        .body(custom_503_body.into())
                        .unwrap());
                }
            };

            println!(
                "PATHFINDER LOAD SHEDDER - Semaphores available post-acquisition: {}",
                semaphore.available_permits()
            );

            let response = match timeout(Duration::from_millis(timeout_ms), inner.call(req)).await {
                Ok(res) => res,
                Err(_) => {
                    println!("PATHFINDER LOAD SHEDDER - Request timed out");
                    Ok(Response::builder()
                        .status(StatusCode::GATEWAY_TIMEOUT)
                        .body(Body::empty())
                        .unwrap())
                }
            };

            drop(permit);

            println!(
                "PATHFINDER LOAD SHEDDER - Semaphores available post-release: {}",
                semaphore.available_permits()
            );

            response
        })
    }
}

#[derive(Clone)]
pub struct PathfinderLoadShedderLayer {
    pathfinder_path_transforms: PathfinderPathTransforms,
    max_concurrent_requests: usize,
    timeout_ms: u64,
}

impl PathfinderLoadShedderLayer {
    pub fn new(
        pathfinder_path_transforms: Vec<PathfinderPath>,
        max_concurrent_requests: usize,
        timeout_ms: u64,
    ) -> Self {
        Self {
            pathfinder_path_transforms: PathfinderPathTransforms::new(pathfinder_path_transforms),
            max_concurrent_requests,
            timeout_ms,
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
            self.pathfinder_path_transforms.clone(),
            inner,
            self.max_concurrent_requests,
            self.timeout_ms,
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
    async fn test_path_starts_with_logic() {
        let pathfinder_path_transforms = PathfinderPathTransforms::new(vec![
            PathfinderPath { path: "/test" },
            PathfinderPath { path: "/test/bees" },
            PathfinderPath { path: "/test2" },
            PathfinderPath { path: "/test/nest" },
            PathfinderPath {
                path: "/test/bee/flies",
            },
        ]);

        let path = "/test/path";
        let result = pathfinder_path_transforms.check_path_starts_with(path);
        assert_eq!(result, Some("/test".to_string()));

        let path = "/test/bees/presbytarian?query=test";
        let result = pathfinder_path_transforms.check_path_starts_with(path);
        assert_eq!(result, Some("/test/bees".to_string()));

        let path = "/test2/path";
        let result = pathfinder_path_transforms.check_path_starts_with(path);
        assert_eq!(result, Some("/test2".to_string()));

        let path = "/test3/path";
        let result = pathfinder_path_transforms.check_path_starts_with(path);
        assert_eq!(result, None);

        let path = "/bees/path";
        let result = pathfinder_path_transforms.check_path_starts_with(path);
        assert_eq!(result, None);

        let path = "/test/nest/egg";
        let result = pathfinder_path_transforms.check_path_starts_with(path);
        assert_eq!(result, Some("/test/nest".to_string()));

        let path = "/test/bee/flies/fast";
        let result = pathfinder_path_transforms.check_path_starts_with(path);
        assert_eq!(result, Some("/test/bee/flies".to_string()));

        let path = "/test/nest/";
        let result = pathfinder_path_transforms.check_path_starts_with(path);
        assert_eq!(result, Some("/test/nest".to_string()));

        let path = "/test/bee/flies";
        let result = pathfinder_path_transforms.check_path_starts_with(path);
        assert_eq!(result, Some("/test/bee/flies".to_string()));

        let path = "/test/nest/egg?query=test#fragment";
        let result = pathfinder_path_transforms.check_path_starts_with(path);
        assert_eq!(result, Some("/test/nest".to_string()));

        let path = "/";
        let result = pathfinder_path_transforms.check_path_starts_with(path);
        assert_eq!(result, None);

        let path = "///";
        let result = pathfinder_path_transforms.check_path_starts_with(path);
        assert_eq!(result, None);

        let path = "/test/special!@#$%^&*()";
        let result = pathfinder_path_transforms.check_path_starts_with(path);
        assert_eq!(result, Some("/test".to_string()));
    }

    #[tokio::test]
    async fn test_load_shedding_normal_operation() {
        let service = service_fn(|_req: Request<Body>| async {
            Ok::<_, hyper::Error>(Response::new(Body::from("Hello, World!")))
        });

        let layer =
            PathfinderLoadShedderLayer::new(vec![PathfinderPath { path: "/test/*" }], 1, 100000);

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
            tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
            Ok::<_, hyper::Error>(Response::new(Body::from("Hello, World!")))
        });

        let layer =
            PathfinderLoadShedderLayer::new(vec![PathfinderPath { path: "/test" }], 2, 100000);

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

        let response1 = load_shedder.call(request1);
        let response2 = load_shedder.call(request2);
        let response3 = load_shedder.call(request3);

        let (response1, response2, response3) =
            tokio::try_join!(response1, response2, response3).unwrap();

        println!("Response 1: {:?}", response1);
        println!("Response 2: {:?}", response2);
        println!("Response 3: {:?}", response3);

        assert_eq!(response1.status(), StatusCode::OK);
        assert_eq!(response2.status(), StatusCode::OK);
        assert_eq!(response3.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn test_custom_503_body() {
        let service = service_fn(|_req: Request<Body>| async {
            tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
            Ok::<_, hyper::Error>(Response::new(Body::from("Hello, World!")))
        });

        let layer =
            PathfinderLoadShedderLayer::new(vec![PathfinderPath { path: "/test" }], 1, 100000);
        let layer = layer.set_pathfinder_503_body("Custom 503 Body");

        let mut load_shedder = layer.layer(service);

        let request1 = Request::builder()
            .uri("/test/path")
            .body(Body::empty())
            .unwrap();

        let request2 = Request::builder()
            .uri("/test/path")
            .body(Body::empty())
            .unwrap();

        let response1 = load_shedder.call(request1);
        let response2 = load_shedder.call(request2);

        let (response1, response2) = tokio::try_join!(response1, response2).unwrap();

        assert_eq!(response1.status(), StatusCode::OK);
        assert_eq!(response2.status(), StatusCode::SERVICE_UNAVAILABLE);

        use axum::body::to_bytes;
        let body_bytes = to_bytes(response2.into_body(), 1000000).await.unwrap();
        let body_str = std::str::from_utf8(&body_bytes).unwrap();
        assert_eq!(body_str, "Custom 503 Body");
    }

    #[tokio::test]
    async fn test_load_shedding_timeout_handling() {
        let service = service_fn(|_req: Request<Body>| async {
            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
            Ok::<_, hyper::Error>(Response::new(Body::from("Hello, World!")))
        });

        let layer = PathfinderLoadShedderLayer::new(vec![PathfinderPath { path: "/test" }], 2, 100);

        let mut load_shedder = layer.layer(service);

        let request = Request::builder()
            .uri("/test/path")
            .body(Body::empty())
            .unwrap();

        let response = load_shedder.call(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    }
}
