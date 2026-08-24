use crate::packet::types::{ConnackReturnCode, SubackReturnCode};

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ServerPacket {
    Connack {
        session_present: bool,
        return_code: ConnackReturnCode,
    },
    Suback {
        packet_id: u16,
        return_code: Vec<SubackReturnCode>,
    },
    Unsuback {
        packet_id: u16,
    },
    Publish {
        topic_name: String,
        packet_id: Option<u16>,
        payload: Vec<u8>,
        dup: bool,
        qos: u8,
        retain: bool,
    },
    Puback {
        packet_id: u16,
    },
    Pingresp,
}

impl ServerPacket {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.encode_into(&mut buf);
        buf
    }

    pub fn encode_into(&self, buf: &mut Vec<u8>) {
        match self {
            ServerPacket::Connack {
                session_present,
                return_code,
            } => {
                buf.push(0x20);
                encode_remaining_length(2, buf);

                let flags = if *session_present { 0x01 } else { 0x00 };
                buf.push(flags);
                buf.push(*return_code as u8);
            }

            ServerPacket::Pingresp => {
                buf.push(0xD0);
                encode_remaining_length(0, buf);
            }

            ServerPacket::Suback {
                packet_id,
                return_code,
            } => {
                buf.push(0x90);

                let remaining_len = 2 + return_code.len();
                encode_remaining_length(remaining_len, buf);

                buf.extend_from_slice(&packet_id.to_be_bytes());
                for code in return_code {
                    buf.push(*code as u8);
                }
            }

            ServerPacket::Unsuback { packet_id } => {
                buf.push(0xB0);
                encode_remaining_length(2, buf);
                buf.extend_from_slice(&packet_id.to_be_bytes());
            }

            ServerPacket::Puback { packet_id } => {
                buf.push(0x40);
                encode_remaining_length(2, buf);
                buf.extend_from_slice(&packet_id.to_be_bytes());
            }

            ServerPacket::Publish {
                topic_name,
                packet_id,
                payload,
                dup,
                qos,
                retain,
            } => {
                let mut header_byte = 0x30;
                if *dup {
                    header_byte |= 0x08;
                }
                header_byte |= (*qos & 0x03) << 1;
                if *retain {
                    header_byte |= 0x01;
                }
                buf.push(header_byte);

                let mut remaining_len = 2 + topic_name.len() + payload.len();
                if *qos > 0 {
                    remaining_len += 2;
                }
                encode_remaining_length(remaining_len, buf);
                encode_utf8_string(topic_name, buf);

                if *qos > 0
                    && let Some(pid) = packet_id
                {
                    buf.extend_from_slice(&pid.to_be_bytes());
                }

                buf.extend_from_slice(payload);
            }
        }
    }
}

pub fn encode_remaining_length(mut length: usize, buf: &mut Vec<u8>) {
    loop {
        let mut byte = (length % 128) as u8;
        length /= 128;
        if length > 0 {
            byte |= 0x80;
        }
        buf.push(byte);
        if length == 0 {
            break;
        }
    }
}

fn encode_utf8_string(s: &str, buf: &mut Vec<u8>) {
    let len = s.len() as u16;
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(s.as_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::packet::types::ConnackReturnCode;

    #[test]
    fn test_encode_connack() {
        let packet = ServerPacket::Connack {
            session_present: true,
            return_code: ConnackReturnCode::Accepted,
        };

        let bytes = packet.encode();
        assert_eq!(bytes, vec![0x20, 0x02, 0x01, 0x00]);
    }

    #[test]
    fn test_encode_pingresp() {
        let packet = ServerPacket::Pingresp;
        assert_eq!(packet.encode(), vec![0xD0, 0x00]);
    }

    #[test]
    fn test_encode_remaining_length() {
        let mut buf = Vec::new();

        encode_remaining_length(64, &mut buf);
        assert_eq!(buf, vec![0x40]);
        buf.clear();

        encode_remaining_length(321, &mut buf);
        assert_eq!(buf, vec![0xC1, 0x02]);
    }

    #[test]
    fn test_encode_publish() {
        let packet = ServerPacket::Publish {
            topic_name: "a/b".to_string(),
            packet_id: None,
            payload: vec![0x01, 0x02],
            dup: false,
            qos: 0,
            retain: false,
        };

        let expected = vec![
            0x30, 0x07, // Header (QoS 0) + Remaining Length (7 octets)
            0x00, 0x03, b'a', b'/', b'b', // Topic name: Length (3) + "a/b"
            0x01, 0x02, // Payload
        ];
        assert_eq!(packet.encode(), expected);
    }
}
