use anyhow::Result;
use tokio::net::TcpListener;
use tracing::{debug, error, info, warn};

use crate::{
    network::connection::Connection,
    packet::{
        encoder::ServerPacket,
        types::{ConnackReturnCode, ControlPacketType},
    },
};

pub async fn start(addr: &str) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!("MQTT Server is listening on {}", addr);

    loop {
        let (stream, socket_addr) = listener.accept().await?;
        info!("New client connected: {}", socket_addr);

        tokio::spawn(async move {
            let mut connection = Connection::new(stream);

            if let Err(e) = handl_client(&mut connection).await {
                error!("Error with client {} : {}", socket_addr, e);
            }

            info!("Connection close for : {}", socket_addr);
        });
    }
}

async fn handl_client(connection: &mut Connection) -> Result<()> {
    while let Some(packet) = connection.read_packet().await? {
        debug!("Packet received : {:#?}", packet);

        match packet.header.r#type {
            ControlPacketType::Connect => {
                info!("Connection request received. Sending CONNACK...");
                let connack = ServerPacket::Connack {
                    session_present: false,
                    return_code: ConnackReturnCode::Accepted,
                };
                connection.write_packet(&connack).await?;
            }
            _ => {
                warn!("Packet not implemented for now: {:?}", packet.header.r#type);
            }
        }
    }

    Ok(())
}
