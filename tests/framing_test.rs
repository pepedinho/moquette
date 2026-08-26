use std::net::SocketAddr;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{Duration, sleep},
};

use moquette::{broker::SharedBroker, network::connection::Connection};

/// Starts a test server listening on an ephemeral port (`127.0.0.1:0`).
async fn spawn_test_server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let broker = SharedBroker::new(1);

    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let broker = broker.clone();
            tokio::spawn(async move {
                let mut connection = Connection::new(stream);
                // Handle the client using the internal client handler loop
                let _ = moquette::network::server::handl_client(&mut connection, broker).await;
            });
        }
    });

    addr
}

#[tokio::test]
async fn test_concatenated_packets_in_single_write() {
    let addr = spawn_test_server().await;
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // Two PINGREQ packets concatenated in a single socket write buffer: [0xC0, 0x00, 0xC0, 0x00]
    let two_pingreqs = [0xC0, 0x00, 0xC0, 0x00];

    stream.write_all(&two_pingreqs).await.unwrap();
    stream.flush().await.unwrap();

    // Expect two PINGRESP packets [0xD0, 0x00, 0xD0, 0x00] back from the server
    let mut response = [0u8; 4];
    stream.read_exact(&mut response).await.unwrap();

    assert_eq!(response, [0xD0, 0x00, 0xD0, 0x00]);
}

#[tokio::test]
async fn test_fragmented_packet_across_multiple_writes() {
    let addr = spawn_test_server().await;
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // Send the first byte of a PINGREQ packet (Fixed Header)
    stream.write_all(&[0xC0]).await.unwrap();
    stream.flush().await.unwrap();

    // Pause briefly to force the server to receive an incomplete packet frame
    sleep(Duration::from_millis(50)).await;

    // Send the second byte (Remaining Length) to complete the packet
    stream.write_all(&[0x00]).await.unwrap();
    stream.flush().await.unwrap();

    // Expect a single PINGRESP response [0xD0, 0x00] once the frame is completed
    let mut response = [0u8; 2];
    stream.read_exact(&mut response).await.unwrap();

    assert_eq!(response, [0xD0, 0x00]);
}
