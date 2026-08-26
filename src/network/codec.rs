use tokio_util::{
    bytes::{Buf, BytesMut},
    codec::Decoder,
};

use crate::packet::{decoder::ControlPacket, error::ParseError};

pub struct MqttCodec;

impl Decoder for MqttCodec {
    type Item = ControlPacket;
    type Error = ParseError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }

        match ControlPacket::parse(src) {
            Ok((packet, bytes_consumed)) => {
                src.advance(bytes_consumed);
                Ok(Some(packet))
            }
            Err(ParseError::Incomplete) => Ok(None),
            Err(e) => Err(e),
        }
    }
}
