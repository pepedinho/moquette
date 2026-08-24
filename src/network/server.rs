use anyhow::{Context, Result, bail};
use tokio::{net::TcpListener, sync::mpsc};
use tracing::{debug, error, info, warn};

use crate::{
    broker::state::SharedBroker,
    config::Config,
    network::connection::Connection,
    packet::{
        decoder::ControlPacket,
        encoder::ServerPacket,
        types::{ConnackReturnCode, ControlPacketType, Payload, SubackReturnCode, VariableHeader},
    },
};

pub async fn start(config: Config, broker: SharedBroker) -> Result<()> {
    let addr = &format!("{}:{}", config.server.host, config.server.port);
    let listener = TcpListener::bind(addr).await?;
    info!("MQTT Server is listening on {}", addr);

    loop {
        let (stream, socket_addr) = listener.accept().await?;
        info!("New client connected: {}", socket_addr);

        let broker = broker.clone();

        tokio::spawn(async move {
            let mut connection = Connection::new(stream);

            if let Err(e) = handl_client(&mut connection, broker).await {
                error!("Error with client {} : {}", socket_addr, e);
            }

            info!("Connection close for : {}", socket_addr);
        });
    }
}

async fn handl_client(connection: &mut Connection, broker: SharedBroker) -> Result<()> {
    let (tx, mut rx) = mpsc::channel::<ServerPacket>(32);
    let mut client_id: Option<String> = None;

    let result = run_client_loop(connection, &broker, &tx, &mut rx, &mut client_id).await;

    if let Some(id) = client_id {
        broker.unsubscribe_client(&id);
    }

    result
}

async fn run_client_loop(
    connection: &mut Connection,
    broker: &SharedBroker,
    tx: &mpsc::Sender<ServerPacket>,
    rx: &mut mpsc::Receiver<ServerPacket>,
    client_id: &mut Option<String>,
) -> Result<()> {
    loop {
        tokio::select! {
            Some(packet_to_send) = rx.recv() => {
                connection.write_packet(&packet_to_send).await?;
            }

            result = connection.read_packet() => {
                let packet = match result? {
                    Some(p) => p,
                    None => return Ok(()),
                };

                handl_packet(packet, connection, broker, tx, client_id).await?;
            }
        }
    }
}

async fn handl_packet(
    packet: ControlPacket,
    connection: &mut Connection,
    broker: &SharedBroker,
    tx: &mpsc::Sender<ServerPacket>,
    client_id: &mut Option<String>,
) -> Result<()> {
    debug!("Packet received : {:#?}", packet);

    match packet.header.r#type {
        ControlPacketType::Connect => {
            let received_id = match &packet.payload {
                Payload::Connect(payload) => payload.client_id.clone(),
                _ => String::new(),
            };

            let final_id = if received_id.is_empty() {
                broker.generate_client_id()
            } else {
                received_id
            };

            *client_id = Some(final_id.clone());
            info!("Connection request received. Sending CONNACK...");

            let connack = ServerPacket::Connack {
                session_present: false,
                return_code: ConnackReturnCode::Accepted,
            };
            connection.write_packet(&connack).await?;
        }

        ControlPacketType::Subscribe => {
            let id = client_id
                .as_ref()
                .context("SUBSCRIBE received before CONNECT")?;

            info!("Connection request received. Sending SUBACK...");
            if let Payload::Subscribe(payload) = packet.payload {
                for topic in payload {
                    broker.subscribe(id.clone(), topic.filter, tx.clone());
                }

                let suback = ServerPacket::Suback {
                    packet_id: 1,
                    return_code: vec![SubackReturnCode::SuccessQoS0],
                };
                connection.write_packet(&suback).await?;
            } else {
                bail!("Incorrect payload");
            }
        }

        ControlPacketType::Publish => {
            if let (
                VariableHeader::Publish {
                    topic_name,
                    packet_id,
                },
                Payload::Publish(payload_bytes),
            ) = (&packet.vheader, &packet.payload)
            {
                let flags = packet.header.flags;
                let dup = (flags & 0x08) != 0;
                let qos = (flags & 0x06) >> 1;
                let retain = (flags & 0x01) != 0;

                info!(
                    "Received message on topic ['{}'] ({} bytes)",
                    topic_name,
                    payload_bytes.len()
                );

                let outgoing_publish = ServerPacket::Publish {
                    topic_name: topic_name.clone(),
                    payload: payload_bytes.clone(),
                    qos,
                    retain,
                    dup,
                    packet_id: *packet_id,
                };

                broker.publish(topic_name, outgoing_publish).await;
            } else {
                bail!("Incorrect packet structure for PUBLISH")
            }
        }
        ControlPacketType::Pingreq => {
            debug!("Receive PINGREQ, sending PINGRESP");
            connection.write_packet(&ServerPacket::Pingresp).await?;
        }
        ControlPacketType::Disconnect => {
            info!("Client ask for disconnection.");
            return Ok(());
        }
        _ => {
            warn!("Packet not implemented for now: {:?}", packet.header.r#type);
        }
    }
    Ok(())
}
