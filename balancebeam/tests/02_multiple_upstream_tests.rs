mod common;

use common::{init_logging, BalanceBeam, EchoServer, ErrorServer, Server};

use std::time::Duration;
use tokio::time::sleep;

async fn setup_with_params(
    n_upstreams: u64,
    active_health_check_interval: Option<u64>,
    rate_limit_window_size: Option<u64>,
    max_requests_per_window: Option<u64>,
) -> (BalanceBeam, Vec<Box<dyn Server>>) {
    init_logging();
    let mut upstreams: Vec<Box<dyn Server>> = Vec::new();
    for _ in 0..n_upstreams {
        upstreams.push(Box::new(EchoServer::new().await));
    }
    let upstream_addresses: Vec<String> = upstreams
        .iter()
        .map(|upstream| upstream.address())
        .collect();
    let upstream_addresses: Vec<&str> = upstream_addresses
        .iter()
        .map(|addr| addr.as_str())
        .collect();
    let balancebeam = BalanceBeam::new(
        &upstream_addresses,
        active_health_check_interval,
        rate_limit_window_size,
        max_requests_per_window,
    )
    .await;
    (balancebeam, upstreams)
}

async fn setup(n_upstreams: u64) -> (BalanceBeam, Vec<Box<dyn Server>>) {
    setup_with_params(n_upstreams, None, None, None).await
}

/// Send a bunch of requests to the load balancer, and ensure they are evenly distributed across the
/// upstream servers
#[tokio::test]
async fn test_load_distribution() {
    let n_upstreams = 3;
    let n_requests = 90;
    let (balancebeam, mut upstreams) = setup(n_upstreams).await;

    for i in 0..n_requests {
        let path = format!("/request-{}", i);
        let response_text = balancebeam
            .get(&path)
            .await
            .expect("Error sending request to balancebeam");
        assert!(response_text.contains(&format!("GET {} HTTP/1.1", path)));
        assert!(response_text.contains("x-sent-by: balancebeam-tests"));
        assert!(response_text.contains("x-forwarded-for: 127.0.0.1"));
    }

    let mut request_counters = Vec::new();
    while let Some(upstream) = upstreams.pop() {
        request_counters.insert(0, upstream.stop().await);
    }
    log::info!(
        "Number of requests received by each upstream: {:?}",
        request_counters
    );
    let avg_req_count =
        request_counters.iter().sum::<u64>() as f64 / request_counters.len() as f64;
    log::info!("Average number of requests per upstream: {}", avg_req_count);
    for upstream_req_count in request_counters {
        if (upstream_req_count as f64 - avg_req_count).abs() > 0.4 * avg_req_count {
            log::error!(
                "Upstream request count {} differs too much from the average! Load doesn't seem \
                evenly distributed.",
                upstream_req_count
            );
            panic!("Upstream request count differs too much");
        }
    }

    log::info!("All done :)");
}

async fn try_failover(balancebeam: &BalanceBeam, upstreams: &mut Vec<Box<dyn Server>>) {
    // Send some initial requests. Everything should work
    log::info!("Sending some initial requests. These should definitely work.");
    for i in 0..5 {
        let path = format!("/request-{}", i);
        let response_text = balancebeam
            .get(&path)
            .await
            .expect("Error sending request to balancebeam");
        assert!(response_text.contains(&format!("GET {} HTTP/1.1", path)));
        assert!(response_text.contains("x-sent-by: balancebeam-tests"));
        assert!(response_text.contains("x-forwarded-for: 127.0.0.1"));
    }

    // Kill one of the upstreams
    log::info!("Killing one of the upstream servers");
    upstreams.pop().unwrap().stop().await;

    // Make sure requests continue to work
    for i in 0..6 {
        log::info!("Sending request #{} after killing an upstream server", i);
        let path = format!("/failover-{}", i);
        let response_text = balancebeam
            .get(&path)
            .await
            .expect("Error sending request to balancebeam. Passive failover may not be working");
        assert!(
            response_text.contains(&format!("GET {} HTTP/1.1", path)),
            "balancebeam returned unexpected response. Failover may not be working."
        );
        assert!(
            response_text.contains("x-sent-by: balancebeam-tests"),
            "balancebeam returned unexpected response. Failover may not be working."
        );
        assert!(
            response_text.contains("x-forwarded-for: 127.0.0.1"),
            "balancebeam returned unexpected response. Failover may not be working."
        );
    }
}

/// Make sure passive health checks work. Send a few requests, then kill one of the upstreams and
/// make sure requests continue to work
#[tokio::test]
async fn test_passive_health_checks() {
    let n_upstreams = 2;
    let (balancebeam, mut upstreams) = setup(n_upstreams).await;
    try_failover(&balancebeam, &mut upstreams).await;
    log::info!("All done :)");
}

/// Verify that the active health checks are monitoring HTTP status, rather than simply depending
/// on whether connections can be established to determine whether an upstream is up:
///
/// * Send a few requests
/// * Replace one of the upstreams with a server that only returns HTTP error 500s
/// * Send some more requests. Make sure all the requests succeed
#[tokio::test]
async fn test_active_health_checks_check_http_status() {
    let n_upstreams = 2;
    let (balancebeam, mut upstreams) = setup_with_params(n_upstreams, Some(1), None, None).await;
    let failed_ip = upstreams[upstreams.len() - 1].address();

    // Send some initial requests. Everything should work
    log::info!("Sending some initial requests. These should definitely work.");
    for i in 0..4 {
        let path = format!("/request-{}", i);
        let response_text = balancebeam
            .get(&path)
            .await
            .expect("Error sending request to balancebeam");
        assert!(response_text.contains(&format!("GET {} HTTP/1.1", path)));
        assert!(response_text.contains("x-sent-by: balancebeam-tests"));
        assert!(response_text.contains("x-forwarded-for: 127.0.0.1"));
    }

    // Do a switcharoo with an upstream
    log::info!("Replacing one of the upstreams with a server that returns Error 500s...");
    upstreams.pop().unwrap().stop().await;
    upstreams.push(Box::new(ErrorServer::new_at_address(failed_ip).await));

    log::info!("Waiting for health checks to realize server is dead...");
    sleep(Duration::from_secs(3)).await;

    // Make sure we get back successful requests
    for i in 0..8 {
        log::info!(
            "Sending request #{} after swapping server for one that returns Error 500. We should \
            get a successful response from the other upstream",
            i
        );
        let path = format!("/failover-{}", i);
        let response_text = balancebeam.get(&path).await.expect(
            "Error sending request to balancebeam. Active health checks may not be working",
        );
        assert!(
            response_text.contains(&format!("GET {} HTTP/1.1", path)),
            "balancebeam returned unexpected response. Active health checks may not be working."
        );
        assert!(
            response_text.contains("x-sent-by: balancebeam-tests"),
            "balancebeam returned unexpected response. Active health checks may not be working."
        );
        assert!(
            response_text.contains("x-forwarded-for: 127.0.0.1"),
            "balancebeam returned unexpected response. Active health checks may not be working."
        );
    }
}

/// Make sure active health checks restore upstreams that were previously failed but are now
/// working again:
///
/// * Send a few requests
/// * Kill one of the upstreams
/// * Send some more requests
/// * Bring the upstream back
/// * Ensure requests are delivered again
#[tokio::test]
async fn test_active_health_checks_restore_failed_upstream() {
    let n_upstreams = 2;
    let (balancebeam, mut upstreams) = setup_with_params(n_upstreams, Some(1), None, None).await;
    let failed_ip = upstreams[upstreams.len() - 1].address();
    try_failover(&balancebeam, &mut upstreams).await;

    log::info!("Re-starting the \"failed\" upstream server...");
    upstreams.push(Box::new(EchoServer::new_at_address(failed_ip).await));

    log::info!("Waiting a few seconds for the active health check to run...");
    sleep(Duration::from_secs(3)).await;

    log::info!("Sending some more requests");
    for i in 0..5 {
        let path = format!("/after-restore-{}", i);
        let response_text = balancebeam
            .get(&path)
            .await
            .expect("Error sending request to balancebeam");
        assert!(response_text.contains(&format!("GET {} HTTP/1.1", path)));
        assert!(response_text.contains("x-sent-by: balancebeam-tests"));
        assert!(response_text.contains("x-forwarded-for: 127.0.0.1"));
    }

    log::info!(
        "Verifying that the previously-dead upstream got some requests after being restored"
    );
    let last_upstream_req_count = upstreams.pop().unwrap().stop().await;
    assert!(
        last_upstream_req_count > 0,
        "We killed an upstream, then brought it back, but it never got any more requests!"
    );

    // Shut down
    while let Some(upstream) = upstreams.pop() {
        upstream.stop().await;
    }

    log::info!("All done :)");
}

/// Enable rate limiting and ensure that requests fail after sending more than the threshold
#[tokio::test]
async fn test_rate_limiting() {
    let n_upstreams = 1;
    let rate_limit_window = 5; // seconds
    let rate_limit_threshold = 5; // requests/window
    // Number of requests to send beyond what's allowed:
    let num_requests_to_get_blocked = 3;
    // Number of requests to send after we've cooled off and should be allowed again:
    let num_requests_after_window = 3;
    let (balancebeam, mut upstreams) =
        setup_with_params(n_upstreams, None, Some(rate_limit_window), Some(rate_limit_threshold)).await;

    log::info!(
        "Sending some basic requests to the server, within the rate limit threshold. These \
        should succeed."
    );
    for i in 0..rate_limit_threshold {
        let path = format!("/request-{}", i);
        let response_text = balancebeam
            .get(&path)
            .await
            .expect("Error sending request to balancebeam");
        assert!(response_text.contains(&format!("GET {} HTTP/1.1", path)));
        assert!(response_text.contains("x-sent-by: balancebeam-tests"));
        assert!(response_text.contains("x-forwarded-for: 127.0.0.1"));
    }

    log::info!(
        "Sending more requests that exceed the rate limit threshold. The server should \
        respond to these with an HTTP 429 (too many requests) error."
    );
    for i in 0..num_requests_to_get_blocked {
        let client = reqwest::Client::new();
        let response = client
            .get(&format!("http://{}/overboard-{}", balancebeam.address, i))
            .header("x-sent-by", "balancebeam-tests")
            .send()
            .await
            .expect(
                "Error sending rate limited request to balancebeam. You should be \
                accepting the connection but sending back an HTTP error, rather than rejecting \
                the connection outright.",
            );
        log::info!("{:?}", response);
        log::info!("Checking to make sure the server responded with HTTP 429");
        assert_eq!(response.status().as_u16(), 429);
    }

    log::info!("Waiting for the rate limiting window to elapse so that we can send requests again");
    sleep(Duration::from_secs(rate_limit_window + 1)).await;

    log::info!("Sending more requests after the rate limit window has elapsed. These should work!");
    for i in 0..num_requests_after_window {
        let path = format!("/please-work-again-{}", i);
        let response_text = balancebeam
            .get(&path)
            .await
            .expect("Error sending request to balancebeam");
        assert!(response_text.contains(&format!("GET {} HTTP/1.1", path)));
        assert!(response_text.contains("x-sent-by: balancebeam-tests"));
        assert!(response_text.contains("x-forwarded-for: 127.0.0.1"));
    }

    log::info!("Ensuring the blocked requests didn't go through to the upstream servers");
    let mut total_request_count = 0;
    while let Some(upstream) = upstreams.pop() {
        total_request_count += upstream.stop().await;
    }
    assert_eq!(total_request_count, rate_limit_threshold + num_requests_after_window);

    log::info!("All done :)");
}
