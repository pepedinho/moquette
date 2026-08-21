use crate::packet::{
    reader::BufferReader,
    types::{
        ConnectFlag, ConnectPayload, ControlPacketType, FixedHeader, Payload, RemainingLength,
        TopicFilter, VariableHeader,
    },
};

#[derive(Debug)]
pub struct ControlPacket {
    pub header: FixedHeader,
    pub vheader: VariableHeader,
    pub payload: Payload,
}

impl ControlPacket {
    pub fn parse(bytes: &[u8]) -> Result<Self, &'static str> {
        let (header, vheader, payload) = decode_client_packet(bytes)?;
        Ok(Self {
            header,
            vheader,
            payload,
        })
    }
}

pub fn decode_client_packet(
    bytes: &[u8],
) -> Result<(FixedHeader, VariableHeader, Payload), &'static str> {
    if bytes.is_empty() {
        return Err("Empty buffer");
    }

    let fixed_header = FixedHeader::from_bytes(bytes)?;

    let header_len = 1 + fixed_header.remaining_length.br;
    let total_len = header_len + fixed_header.remaining_length.l;

    if bytes.len() < total_len {
        return Err("Incomplete packet");
    }

    let body_bytes = &bytes[header_len..total_len];
    let mut reader = BufferReader::new(body_bytes);

    let (variable_header, payload) = match fixed_header.r#type {
        ControlPacketType::Connect => {
            let vheader = VariableHeader::parse_connect(&mut reader)?;
            let payload = Payload::parse_connect(&mut reader, &vheader)?;
            (vheader, payload)
        }
        ControlPacketType::Publish => {
            let vheader = VariableHeader::parse_publish(&mut reader, fixed_header.flags)?;
            let payload = Payload::parse_publish(&mut reader)?;
            (vheader, payload)
        }
        ControlPacketType::Subscribe => {
            let vheader = VariableHeader::parse_packet_id(&mut reader)?;
            let payload = Payload::parse_subscribe(&mut reader)?;
            (vheader, payload)
        }
        ControlPacketType::Unsubscribe => {
            let vheader = VariableHeader::parse_packet_id(&mut reader)?;
            let payload = Payload::parse_unsubscribe(&mut reader)?;
            (vheader, payload)
        }
        ControlPacketType::Puback
        | ControlPacketType::Pubrec
        | ControlPacketType::Pubrel
        | ControlPacketType::Pubcomp => {
            let vheader = VariableHeader::parse_packet_id(&mut reader)?;
            (vheader, Payload::None)
        }
        ControlPacketType::Pingreq | ControlPacketType::Disconnect => {
            (VariableHeader::None, Payload::None)
        }
        _ => return Err("Unexpected or unsupported control packet type"),
    };

    Ok((fixed_header, variable_header, payload))
}

impl FixedHeader {
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, &'static str> {
        if bytes.is_empty() {
            return Err("Empty buffer: cannot parse fixed header");
        }

        let byte0 = bytes[0];
        let type_u8 = byte0 >> 4;
        let flags = byte0 & 0x0F;

        let packet_type = ControlPacketType::try_from(type_u8)?;

        match packet_type {
            ControlPacketType::Publish => {}
            ControlPacketType::Pubrel
            | ControlPacketType::Subscribe
            | ControlPacketType::Unsubscribe => {
                if flags != 2 {
                    return Err("Invalid flag detected");
                }
            }
            _ => {
                if flags != 0 {
                    return Err("Invalid flag detected");
                }
            }
        }

        let remaining_length = parse_remaining_length(&bytes[1..])?;

        Ok(FixedHeader {
            r#type: packet_type,
            flags,
            remaining_length,
        })
    }
}

fn parse_remaining_length(bytes: &[u8]) -> Result<RemainingLength, &'static str> {
    let mut value: usize = 0;
    let mut multiplier = 1;
    let mut bytes_read = 0;
    loop {
        if bytes_read >= bytes.len() {
            return Err("Incomplete remaining length data");
        }

        if bytes_read >= 4 {
            return Err("Malformed remaining length: exceeds 4 bytes");
        }

        let encoded_byte = bytes[bytes_read];
        bytes_read += 1;

        value += (encoded_byte & 127) as usize * multiplier;
        if (encoded_byte & 0x80) == 0 {
            break;
        }

        multiplier *= 128;
    }

    Ok(RemainingLength {
        l: value,
        br: bytes_read,
    })
}

impl VariableHeader {
    pub fn parse_connect(reader: &mut BufferReader) -> Result<Self, &'static str> {
        let protocol_name = reader.read_string()?;
        let protocol_level = reader.read_u8()?;
        let connect_flag = ConnectFlag::try_from(reader.read_u8()?)?;
        let keep_alive = reader.read_u16()?;

        Ok(VariableHeader::Connect {
            protocol_name,
            protocol_level,
            connect_flag,
            keep_alive,
        })
    }

    pub fn parse_publish(reader: &mut BufferReader, flags: u8) -> Result<Self, &'static str> {
        let topic_name = reader.read_string()?;
        let qos = (flags & 0x06) >> 1;

        let packet_id = if qos > 0 {
            Some(reader.read_u16()?)
        } else {
            None
        };

        Ok(VariableHeader::Publish {
            topic_name,
            packet_id,
        })
    }

    pub fn parse_packet_id(reader: &mut BufferReader) -> Result<Self, &'static str> {
        let packet_id = reader.read_u16()?;
        Ok(VariableHeader::PacketId(packet_id))
    }
}

impl Payload {
    pub fn parse_connect(
        reader: &mut BufferReader,
        vheader: &VariableHeader,
    ) -> Result<Self, &'static str> {
        let connect_flag = match vheader {
            VariableHeader::Connect { connect_flag, .. } => connect_flag,
            _ => return Err("Invalid variable header for CONNECT payload"),
        };

        let client_id = reader.read_string()?;

        let (will_topic, will_message) = if connect_flag.will_flag {
            (Some(reader.read_string()?), Some(reader.read_bytes()?))
        } else {
            (None, None)
        };

        let username = if connect_flag.username_flag {
            Some(reader.read_string()?)
        } else {
            None
        };

        let password = if connect_flag.password_flag {
            Some(reader.read_bytes()?)
        } else {
            None
        };

        Ok(Payload::Connect(ConnectPayload {
            client_id,
            will_topic,
            will_message,
            username,
            password,
        }))
    }

    pub fn parse_publish(reader: &mut BufferReader) -> Result<Self, &'static str> {
        Ok(Payload::Publish(reader.read_remaining()))
    }

    pub fn parse_subscribe(reader: &mut BufferReader) -> Result<Self, &'static str> {
        let mut filters = Vec::new();

        while !reader.is_empty() {
            let filter = reader.read_string()?;
            let qos = reader.read_u8()?;

            if qos > 2 {
                return Err("Invalid QoS in Subscribe payload");
            }

            filters.push(TopicFilter { filter, qos });
        }

        Ok(Payload::Subscribe(filters))
    }

    pub fn parse_unsubscribe(reader: &mut BufferReader) -> Result<Self, &'static str> {
        let mut topics = Vec::new();

        while !reader.is_empty() {
            topics.push(reader.read_string()?);
        }

        Ok(Payload::Unsubscribe(topics))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn from_int_to_enum() {
        let x = 5;

        let cp_type = ControlPacketType::try_from(x);

        assert!(cp_type.is_ok());
        if let Ok(t) = cp_type {
            assert_eq!(t, ControlPacketType::Pubrec);
        } else {
            panic!("Failed to convert {:?}", cp_type.ok());
        }
    }

    #[test]
    fn from_int_to_enum_publish() {
        let x = 3;

        let cp_type = ControlPacketType::try_from(x);

        assert!(cp_type.is_ok());
        if let Ok(t) = cp_type {
            assert_eq!(t, ControlPacketType::Publish);
        } else {
            panic!("Failed to convert {:?}", cp_type.ok());
        }
    }

    #[test]
    fn decode_header_remaining_length() {
        // Length to 0
        let bytes = &[0x00];
        let res = parse_remaining_length(bytes).unwrap();
        assert_eq!(res.l, 0);
        assert_eq!(res.br, 1);

        // Length to 127 (max on 1 byte)
        let bytes = &[0x7F];
        let res = parse_remaining_length(bytes).unwrap();
        assert_eq!(res.l, 127);
        assert_eq!(res.br, 1);

        // Length to 128 (overlap on 2 byte)
        let bytes = &[0x80, 0x01];
        let res = parse_remaining_length(bytes).unwrap();
        assert_eq!(res.l, 128);
        assert_eq!(res.br, 2);

        // Incomptete byte (continue bit is on but no other bytes was provided)
        let bytes = &[0x80];
        let res = parse_remaining_length(bytes);
        assert!(res.is_err());
    }

    #[test]
    fn test_parse_connect_packet() {
        let raw_packet: &[u8] = &[
            0x10, 0x10, // Header: CONNECT (0x10), Remaining Len: 18 (0x12)
            0x00, 0x04, b'M', b'Q', b'T', b'T', // Protocol Name: MQTT
            0x04, // Protocol Level: 4 (MQTT v3.1.1)
            0x02, // Connect Flags: Clean Session = 1
            0x00, 0x3C, // Keep Alive: 60s
            0x00, 0x04, b't', b'e', b's', b't', // Client ID: "test"
        ];

        let packet = ControlPacket::parse(raw_packet).expect("Failed to parse CONNECT packet");

        assert_eq!(packet.header.r#type, ControlPacketType::Connect);

        if let VariableHeader::Connect {
            protocol_name,
            protocol_level,
            keep_alive,
            connect_flag,
        } = packet.vheader
        {
            assert_eq!(protocol_name, "MQTT");
            assert_eq!(protocol_level, 4);
            assert_eq!(keep_alive, 60);
            assert!(connect_flag.clean_session);
            assert!(!connect_flag.username_flag);
        } else {
            panic!("Expected VariableHeader::Connect");
        }

        if let Payload::Connect(connect_payload) = packet.payload {
            assert_eq!(connect_payload.client_id, "test");
            assert_eq!(connect_payload.username, None);
        } else {
            panic!("Expected Payload::Connect");
        }
    }

    #[test]
    fn test_parse_publish_qos0() {
        let raw_packet: &[u8] = &[
            0x30, 0x09, // Header: PUBLISH (QoS 0, Dup 0, Retain 0), Len 9
            0x00, 0x04, b't', b'e', b'm', b'p', // Topic Name: "temp" (6 octets)
            b'2', b'2', b'C', // Payload: "22C" (3 octets)
        ];

        let packet = ControlPacket::parse(raw_packet).expect("Failed to parse PUBLISH packet");

        if let VariableHeader::Publish {
            topic_name,
            packet_id,
        } = packet.vheader
        {
            assert_eq!(topic_name, "temp");
            assert_eq!(packet_id, None); // No Packet ID with QoS 0
        } else {
            panic!("Expected VariableHeader::Publish");
        }

        assert_eq!(packet.payload, Payload::Publish(b"22C".to_vec()));
    }

    #[test]
    fn test_parse_publish_qos1() {
        // PUBLISH (QoS 1) on  "a", Packet ID = 10, payload = "hi"
        let raw_packet: &[u8] = &[
            0x32, 0x07, // Header: PUBLISH QoS 1 (0x30 | 0x02)
            0x00, 0x01, b'a', // Topic Name: "a"
            0x00, 0x0A, // Packet ID: 10
            b'h', b'i', // Payload: "hi"
        ];

        let packet =
            ControlPacket::parse(raw_packet).expect("Failed to parse PUBLISH QoS 1 packet");

        if let VariableHeader::Publish {
            topic_name,
            packet_id,
        } = packet.vheader
        {
            assert_eq!(topic_name, "a");
            assert_eq!(packet_id, Some(10));
        } else {
            panic!("Expected VariableHeader::Publish");
        }

        assert_eq!(packet.payload, Payload::Publish(b"hi".to_vec()));
    }

    #[test]
    fn test_parse_incomplete_packet() {
        let raw_packet: &[u8] = &[0x10, 0x0A, 0x00, 0x04];
        let res = ControlPacket::parse(raw_packet);

        assert!(res.is_err());
        assert_eq!(res.unwrap_err(), "Incomplete packet");
    }

    #[test]
    fn test_invalid_fixed_header_flags() {
        let raw_packet: &[u8] = &[0x80, 0x00];
        let res = ControlPacket::parse(raw_packet);

        assert!(res.is_err());
        assert_eq!(res.unwrap_err(), "Invalid flag detected");
    }

    #[test]
    fn test_read_string_success() {
        let data: &[u8] = &[0x00, 0x04, b'T', b'e', b's', b't'];
        let mut reader = BufferReader::new(data);

        let res = reader.read_string().unwrap();
        assert_eq!(res, "Test");
        assert!(reader.is_empty());
    }

    #[test]
    fn test_read_string_too_short() {
        let data: &[u8] = &[0x00, 0x05, b'T', b'e', b's', b't'];
        let mut reader = BufferReader::new(data);

        assert!(reader.read_string().is_err());
    }

    #[test]
    fn test_parse_subscribe_payload() {
        let raw: &[u8] = &[
            0x00, 0x03, b'a', b'/', b'b', 0x01, 0x00, 0x03, b'c', b'/', b'd', 0x00,
        ];
        let mut reader = BufferReader::new(raw);

        let payload = Payload::parse_subscribe(&mut reader).unwrap();
        if let Payload::Subscribe(filters) = payload {
            assert_eq!(filters.len(), 2);
            assert_eq!(filters[0].filter, "a/b");
            assert_eq!(filters[0].qos, 1);
            assert_eq!(filters[1].filter, "c/d");
            assert_eq!(filters[1].qos, 0);
        } else {
            panic!("Expected Payload::Subscribe");
        }
    }

    #[test]
    fn test_parse_subscribe_invalid_qos() {
        let raw: &[u8] = &[0x00, 0x03, b'a', b'/', b'b', 0x03];
        let mut reader = BufferReader::new(raw);
        assert!(Payload::parse_subscribe(&mut reader).is_err());
    }

    #[test]
    fn test_parse_connect_payload_with_flags() {
        let flags = ConnectFlag {
            username_flag: true,
            password_flag: true,
            will_retain: false,
            will_qos: 0,
            will_flag: true,
            clean_session: true,
        };

        let vheader = VariableHeader::Connect {
            protocol_name: "MQTT".to_string(),
            protocol_level: 4,
            connect_flag: flags,
            keep_alive: 60,
        };

        // client_id = "client", will_topic = "will", will_msg = "bye", user = "user", pass = "pass"
        let raw: &[u8] = &[
            0x00, 0x06, b'c', b'l', b'i', b'e', b'n', b't', // Client ID
            0x00, 0x04, b'w', b'i', b'l', b'l', // Will Topic
            0x00, 0x03, b'b', b'y', b'e', // Will Message
            0x00, 0x04, b'u', b's', b'e', b'r', // Username
            0x00, 0x04, b'p', b'a', b's', b's', // Password
        ];
        let mut reader = BufferReader::new(raw);

        let payload = Payload::parse_connect(&mut reader, &vheader).unwrap();

        if let Payload::Connect(conn_payload) = payload {
            assert_eq!(conn_payload.client_id, "client");
            assert_eq!(conn_payload.will_topic.as_deref(), Some("will"));
            assert_eq!(conn_payload.will_message.as_deref(), Some(&b"bye"[..]));
            assert_eq!(conn_payload.username.as_deref(), Some("user"));
            assert_eq!(conn_payload.password.as_deref(), Some(&b"pass"[..]));
        } else {
            panic!("Expected Payload::Connect");
        }
    }
}
