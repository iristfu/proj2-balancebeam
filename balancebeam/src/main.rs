mod request;
mod response;

#[macro_use]
extern crate error_chain;

use clap::Parser;
use rand::{Rng, SeedableRng};
use std::net::{TcpListener, TcpStream};

// Defines a custom Error type that can either contain std::io::Error or one of our own custom
// errors. Then, defines a custom Result<T> = std::Result<T, Error> type that can return this
// custom Error.
//
// You aren't required to use this for anything, but if you want to return a custom error, you can
// do something like:
//     return Err(ErrorKind::NoUpstreamServers.into())
error_chain! {
    // Declare custom errors for use in this project
    errors {
        NoUpstreamServers
        BadResponse
    }
    // Automatically convert other errors, such as std::io::Error, into our custom Error type
    foreign_links {
        IoError(std::io::Error);
        ResponseError(response::Error);
    }
}

/// Contains information parsed from the command-line invocation of balancebeam. The Clap macros
/// provide a fancy way to automatically construct a command-line argument parser.
#[derive(Parser, Debug)]
#[clap(about = "Fun with load balancing")]
struct CmdOptions {
    #[clap(
        short,
        long,
        help = "IP/port to bind to",
        default_value = "0.0.0.0:1100"
    )]
    bind: String,
    #[clap(short, long, help = "Upstream host to forward requests to")]
    upstream: Vec<String>,
    #[clap(
        long,
        help = "Perform active health checks on this interval (in seconds)",
        default_value = "10"
    )]
    active_health_check_interval: u64,
    #[clap(
    long,
    help = "Path to send request to for active health checks",
    default_value = "/"
    )]
    active_health_check_path: String,
    #[clap(
        long,
        help = "Rate limit window size (in seconds)",
        default_value = "60"
    )]
    rate_limit_window_size: u64,
    #[clap(
        long,
        help = "Maximum number of requests to accept per IP per window (0 = unlimited)",
        default_value = "0"
    )]
    max_requests_per_window: u64,
}

/// Contains information about the state of balancebeam (e.g. what servers we are currently proxying
/// to, what servers have failed, rate limiting counts, etc.)
///
/// You should add fields to this struct throughout the project.
struct ProxyState {
    /// How frequently we check whether upstream servers are alive (Milestone 2)
    #[allow(dead_code)]
    active_health_check_interval: u64,
    /// Where we should send requests when doing active health checks (Milestone 2)
    #[allow(dead_code)]
    active_health_check_path: String,
    /// How big the rate limiting window should be, default is 1 minute (Milestone 3)
    #[allow(dead_code)]
    rate_limit_window_size: u64,
    /// Maximum number of requests an individual IP can make in a window (Milestone 3)
    #[allow(dead_code)]
    max_requests_per_window: u64,
    /// Addresses of servers that we are proxying to
    upstream_addresses: Vec<String>,
}

fn main() {
    // Initialize the logging library. You can print log messages using the `log` macros:
    // https://docs.rs/log/0.4.8/log/ You are welcome to continue using print! statements; this
    // just looks a little prettier.
    if let Err(_) = std::env::var("RUST_LOG") {
        std::env::set_var("RUST_LOG", "debug");
    }
    pretty_env_logger::init();

    // Parse the command line arguments passed to this program
    let options = CmdOptions::parse();
    if options.upstream.len() < 1 {
        log::error!("At least one upstream server must be specified using the --upstream option.");
        std::process::exit(1);
    }

    // Start listening for connections
    let listener = match TcpListener::bind(&options.bind) {
        Ok(listener) => listener,
        Err(err) => {
            log::error!("Could not bind to {}: {}", options.bind, err);
            std::process::exit(1);
        }
    };
    log::info!("Listening for requests on {}", options.bind);

    let mut state = ProxyState {
        upstream_addresses: options.upstream,
        active_health_check_interval: options.active_health_check_interval,
        active_health_check_path: options.active_health_check_path,
        rate_limit_window_size: options.rate_limit_window_size,
        max_requests_per_window: options.max_requests_per_window,
    };

    // Wait for incoming connections, and handle them as they come in
    for stream in listener.incoming() {
        if let Ok(stream) = stream {
            // Handle the connection!
            handle_connection(stream, &mut state);
        }
    }
}

/// Returns a random upstream IP, or None if there are no live upstreams
fn select_upstream(upstreams: &Vec<String>) -> Option<String> {
    if upstreams.len() == 0 {
        return None;
    }

    // Randomly pick an upstream server to connect to
    let mut rng = rand::rngs::StdRng::from_entropy();
    let upstream_idx = rng.gen_range(0..upstreams.len());
    // Return the selected upstream IP. We clone() here to return an owned String, because doing so
    // is not particularly expensive, and returning a reference into the vector of upstreams makes
    // life complicated once you start concurrently modifying that vector. (What if you take a
    // reference to an upstream, and then the health check code removes that string when marking
    // the upstream as dead? Sad times.)
    Some(upstreams[upstream_idx].clone())
}

/// Attempts to connect to an upstream server, returning the connection (TcpStream) if successful,
/// or an Error if not.
fn connect_to_upstream(state: &mut ProxyState) -> Result<TcpStream> {
   loop {
        let upstream_ip = select_upstream(&state.upstream_addresses).ok_or(ErrorKind::NoUpstreamServers)?;
    // Try establishing a connection. Return the Result (which either contains the established
    // connection, or an error)
       let result = TcpStream::connect(&upstream_ip).map_err(Error::from);
       if let Err(err) = &result {
          log::error!("Failed to connect to upstream {}: {}", upstream_ip, err);
          // TODO: implement failover (milestone 1)
          // Note: Vec::retain is helpful for removing a value from a vector:
          //     vec.retain(|val| *val != valueToRemove);
          state.upstream_addresses.retain(|val| *val != upstream_ip); // remove upstream_ip from upstream_addresses
          continue;
        
        }
        return result;
   }
}

/// Given a TcpStream that is connected to a client, sends an HTTP response back to that client.
fn send_response(client_conn: &mut TcpStream, response: &http::Response<Vec<u8>>) {
    let client_ip = client_conn.peer_addr().unwrap().ip().to_string();
    log::info!("{} <- {}", client_ip, response::format_response_line(&response));
    if let Err(error) = response::write_to_stream(&response, client_conn) {
        log::warn!("Failed to send response to client: {}", error);
        return;
    }
}

/// Facilitates a connection with a client. Opens a connection to an upstream server, then, for
/// each request the client sends us:
///   * Reads the request
///   * Forwards the request to the upstream server
///   * Reads the response from the upstream
///   * Sends the response back to the client
///
///   Rate limiting should be implemented here as well.
fn handle_connection(mut client_conn: TcpStream, state: &mut ProxyState) {
    let client_ip = client_conn.peer_addr().unwrap().ip().to_string();
    log::info!("Connection received from {}", client_ip);

    // Open a connection to a random upstream server
    let mut upstream_conn = match connect_to_upstream(state) {
        Ok(stream) => stream,
        Err(_error) => {
            // If we failed to connect, send HTTP 502 (Bad Gateway) back to the client
            let response = response::make_http_error(http::StatusCode::BAD_GATEWAY);
            send_response(&mut client_conn, &response);
            return;
        }
    };
    let upstream_ip = client_conn.peer_addr().unwrap().ip().to_string();

    // The client may now send us one or more requests. Keep trying to read requests until the
    // client hangs up or we get an error.
    loop {
        // Read a request from the client
        let mut request = match request::read_from_stream(&mut client_conn) {
            Ok(request) => request,
            // Handle case where client closed connection and is no longer sending requests
            Err(request::Error::IncompleteRequest(0)) => {
                log::debug!("Client finished sending requests. Shutting down connection");
                return;
            }
            // Handle I/O error in reading from the client
            Err(request::Error::ConnectionError(io_err)) => {
                log::info!("Error reading request from client stream: {}", io_err);
                return;
            }
            Err(error) => {
                log::debug!("Error parsing request: {:?}", error);
                let response = response::make_http_error(match error {
                    request::Error::IncompleteRequest(_)
                    | request::Error::MalformedRequest(_)
                    | request::Error::InvalidContentLength
                    | request::Error::ContentLengthMismatch => http::StatusCode::BAD_REQUEST,
                    request::Error::RequestBodyTooLarge => http::StatusCode::PAYLOAD_TOO_LARGE,
                    request::Error::ConnectionError(_) => http::StatusCode::SERVICE_UNAVAILABLE,
                });
                send_response(&mut client_conn, &response);
                continue;
            }
        };
        // Print out the request that was received
        log::info!(
            "{} -> {}: {}",
            client_ip,
            upstream_ip,
            request::format_request_line(&request)
        );

        // Add X-Forwarded-For header so that the upstream server knows the client's IP address.
        // (We're the ones connecting directly to the upstream server, so without this header, the
        // upstream server will only know our IP, not the client's.)
        request::extend_header_value(&mut request, "x-forwarded-for", &client_ip);

        // Forward the request to the server
        if let Err(error) = request::write_to_stream(&request, &mut upstream_conn) {
            log::error!("Failed to send request to upstream {}: {}", upstream_ip, error);
            let response = response::make_http_error(http::StatusCode::BAD_GATEWAY);
            send_response(&mut client_conn, &response);
            return;
        }
        log::debug!("Forwarded request to server");

        // Read the server's response
        let response = match response::read_from_stream(&mut upstream_conn, request.method()) {
            Ok(response) => response,
            Err(error) => {
                log::error!("Error reading response from server: {:?}", error);
                let response = response::make_http_error(http::StatusCode::BAD_GATEWAY);
                send_response(&mut client_conn, &response);
                return;
            }
        };
        // Forward the response to the client
        send_response(&mut client_conn, &response);
        log::debug!("Forwarded response to client");
    }
}
