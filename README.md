# Pathfinder Loadshedder

This is a URL path-based load-shedding solution for Axum.

It calculates the average load times for various URL paths on your website, prioritizing the fastest loading pages to be served first. This approach allows your website to handle more traffic by serving slow loading pages only if the server can manage the load.

We developed this solution because our site, houski.ca, which has over 9 million pages, frequently experiences heavy spidering that can max out our server's usage.

Pathfinder Loadshedder helps keep our site up, fast for most requests, and running smoothly even under heavy traffic.

If a user requests a slow loading page when the server is maxed out, they will be informed immediately that the page is temporarily unavailable, and they can try again later.

Requests that exceed the timeout threshold will not be queued, preventing the server from becoming overloaded by avoiding a backlog of pending requests.

Inspired by the little-loadshedder project that didn't do quite what we needed.

## License

Licensed under either of

- Apache License, Version 2.0
  ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license
  ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Contribution

This project welcomes contributions and suggestions, just open an issue or pull request!

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
