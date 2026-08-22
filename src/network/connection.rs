use anyhow::{Result, anyhow};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::packet::{decoder::ControlPacket, encoder::ServerPacket};

pub struct Connection {
    stream: TcpStream,
    buffer: [u8; 4096],
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            buffer: [0; 4096],
        }
    }

    pub async fn read_packet(&mut self) -> Result<Option<ControlPacket>> {
        let bytes_read = self
            .stream
            .read(&mut self.buffer)
            .await
            .map_err(|e| anyhow!(e))?;

        if bytes_read == 0 {
            return Ok(None);
        }

        match ControlPacket::parse(&self.buffer[..bytes_read]) {
            Ok(packet) => Ok(Some(packet)),
            Err(e) => Err(anyhow!(e)),
        }
    }

    pub async fn write_packet(&mut self, packet: &ServerPacket) -> Result<()> {
        let bytes = packet.encode();
        self.stream
            .write_all(&bytes)
            .await
            .map_err(|e| anyhow!(e))?;
        Ok(())
    }
}
