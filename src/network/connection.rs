use anyhow::{Result, anyhow};
use tokio::{
    io::AsyncWriteExt,
    net::{
        TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
};
use tokio_stream::StreamExt;
use tokio_util::codec::FramedRead;

use crate::{
    network::codec::MqttCodec,
    packet::{decoder::ControlPacket, encoder::ServerPacket},
};

pub struct Connection {
    reader: FramedRead<OwnedReadHalf, MqttCodec>,
    writer: OwnedWriteHalf,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        let (read_half, write_half) = stream.into_split();
        Self {
            reader: FramedRead::new(read_half, MqttCodec),
            writer: write_half,
        }
    }

    pub async fn read_packet(&mut self) -> Result<Option<ControlPacket>> {
        match self.reader.next().await {
            Some(Ok(packet)) => Ok(Some(packet)),
            Some(Err(e)) => Err(anyhow!(e)),
            None => Ok(None),
        }
    }

    pub async fn write_packet(&mut self, packet: &ServerPacket) -> Result<()> {
        let bytes = packet.encode();
        self.writer
            .write_all(&bytes)
            .await
            .map_err(|e| anyhow!(e))?;
        self.writer.flush().await.map_err(|e| anyhow!(e))?;
        Ok(())
    }
}
