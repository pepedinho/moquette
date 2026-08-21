#[derive(Debug, PartialEq)]
pub struct TopicFilter {
    pub filter: String,
    pub qos: u8,
}

#[derive(Debug, PartialEq)]
pub struct ConnectPayload {
    pub client_id: String,
    pub will_topic: Option<String>,
    pub will_message: Option<Vec<u8>>,
    pub username: Option<String>,
    pub password: Option<Vec<u8>>,
}

#[derive(Debug, PartialEq)]
pub enum Payload {
    Connect(ConnectPayload),
    Publish(Vec<u8>),
    Subscribe(Vec<TopicFilter>),
    Suback(Vec<u8>),
    Unsubscribe(Vec<String>),
    None,
}

/// Control Packet Type representation
/// they all got a value from 1 to 14
#[repr(u8)]
#[derive(PartialEq, Debug)]
pub enum ControlPacketType {
    Connect = 1,
    Connack,
    Publish(PublishFlag),
    Puback,
    Pubrec,
    Pubrel,
    Pubcomp,
    Subscribe,
    Suback,
    Unsubscribe,
    Unsuback,
    Pingreq,
    Pingresp,
    Disconnect,
}

/// Flag for Pusblish control packet
/// see: https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Table_3.1_-
#[derive(PartialEq, Debug, Default)]
pub struct PublishFlag {
    pub dup: bool,
    pub qos: u8,
    pub retain: bool,
}

#[derive(Debug)]
pub struct FixedHeader {
    pub r#type: ControlPacketType,
    pub remaining_length: RemainingLength,
}

#[derive(Debug)]
pub struct RemainingLength {
    /// Length found in header.
    pub l: usize,
    /// The Bytes on which the length was encoded.
    pub br: usize,
}

#[derive(Debug)]
pub enum VariableHeader {
    Connect {
        protocol_name: String,
        protocol_level: u8,
        connect_flag: ConnectFlag,
        keep_alive: u16,
    },
    Publish {
        topic_name: String,
        packet_id: Option<u16>,
    },
    PacketId(u16),
    None,
}

#[derive(Debug)]
pub struct ConnectFlag {
    pub username_flag: bool,
    pub password_flag: bool,
    pub will_retain: bool,
    pub will_qos: u8,
    pub will_flag: bool,
    pub clean_session: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ConnackReturnCode {
    Accepted = 0x00,
    UnacceptedProtocolVersion = 0x01,
    IdentifierRejected = 0x02,
    ServerUnavailable = 0x03,
    BadUsernameOrPassword = 0x04,
    NotAuthorized = 0x05,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SubackReturnCode {
    SuccessQoS0 = 0x00,
    SuccessQoS1 = 0x01,
    SuccessQoS2 = 0x02,
    Failure = 0x80,
}

fn read_string(bytes: &mut &[u8]) -> Result<String, &'static str> {
    if bytes.len() < 2 {
        return Err("Payload too short for string length");
    }
    let len = u16::from_be_bytes([bytes[0], bytes[1]]) as usize;
    *bytes = &bytes[2..];

    if bytes.len() < len {
        return Err("Payload too short for string content");
    }

    let s = std::str::from_utf8(&bytes[..len])
        .map_err(|_| "Invalid UTF-8 string in payload")?
        .to_string();
    *bytes = &bytes[len..];
    Ok(s)
}

fn read_bytes(bytes: &mut &[u8]) -> Result<Vec<u8>, &'static str> {
    if bytes.len() < 2 {
        return Err("Payload too short for byte array length");
    }
    let len = u16::from_be_bytes([bytes[0], bytes[1]]) as usize;
    *bytes = &bytes[2..];

    if bytes.len() < len {
        return Err("Payload too short for byte array content");
    }

    let data = bytes[..len].to_vec();
    *bytes = &bytes[len..];
    Ok(data)
}

impl ConnectPayload {
    pub fn from_raw(raw_payload: &mut &[u8], flags: &ConnectFlag) -> Result<Self, &'static str> {
        let client_id = read_string(raw_payload)?;

        let (will_topic, will_message) = if flags.will_flag {
            let topic = read_string(raw_payload)?;
            let msg = read_bytes(raw_payload)?;
            (Some(topic), Some(msg))
        } else {
            (None, None)
        };

        let username = if flags.username_flag {
            Some(read_string(raw_payload)?)
        } else {
            None
        };

        let password = if flags.password_flag {
            Some(read_bytes(raw_payload)?)
        } else {
            None
        };

        let payload = ConnectPayload {
            client_id,
            will_topic,
            will_message,
            username,
            password,
        };

        Ok(payload)
    }
}

impl Payload {
    pub fn parse_subscribe(mut raw: &[u8]) -> Result<Self, &'static str> {
        let mut filters = Vec::new();

        while !raw.is_empty() {
            let filter = read_string(&mut raw)?;
            if raw.is_empty() {
                return Err("SUBSCRIBE payload truncated before QoS byte");
            }
            let qos = raw[0];
            if qos > 2 {
                return Err("Invalid QoS level in SUBSCRIBE payload");
            }
            raw = &raw[1..];
            filters.push(TopicFilter { filter, qos });
        }

        if filters.is_empty() {
            return Err("SUBSCRIBE payload must contain at least one topic filter");
        }

        Ok(Payload::Subscribe(filters))
    }

    pub fn parse_suback(raw: &[u8]) -> Result<Self, &'static str> {
        if raw.is_empty() {
            return Err("SUBACK payload cannot be empty");
        }
        Ok(Payload::Suback(raw.to_vec()))
    }

    pub fn parse_unsubscribe(mut raw: &[u8]) -> Result<Self, &'static str> {
        let mut topics = Vec::new();

        while !raw.is_empty() {
            let topic = read_string(&mut raw)?;
            topics.push(topic);
        }

        if topics.is_empty() {
            return Err("UNSUBSCRIBE payload must contain at least one topic filter");
        }

        Ok(Payload::Unsubscribe(topics))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_string_success() {
        let mut data: &[u8] = &[0x00, 0x04, b'T', b'e', b's', b't'];
        let res = read_string(&mut data).unwrap();
        assert_eq!(res, "Test");
        assert!(data.is_empty());
    }

    #[test]
    fn test_read_string_too_short() {
        let mut data: &[u8] = &[0x00, 0x05, b'T', b'e', b's', b't'];
        assert!(read_string(&mut data).is_err());
    }

    #[test]
    fn test_parse_subscribe_payload() {
        let raw: &[u8] = &[
            0x00, 0x03, b'a', b'/', b'b', 0x01, 0x00, 0x03, b'c', b'/', b'd', 0x00,
        ];

        let payload = Payload::parse_subscribe(raw).unwrap();
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
        assert!(Payload::parse_subscribe(raw).is_err());
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

        // client_id = "client", will_topic = "will", will_msg = "bye", user = "user", pass = "pass"
        let mut raw: &[u8] = &[
            0x00, 0x06, b'c', b'l', b'i', b'e', b'n', b't', // Client ID
            0x00, 0x04, b'w', b'i', b'l', b'l', // Will Topic
            0x00, 0x03, b'b', b'y', b'e', // Will Message
            0x00, 0x04, b'u', b's', b'e', b'r', // Username
            0x00, 0x04, b'p', b'a', b's', b's', // Password
        ];

        let conn_payload = ConnectPayload::from_raw(&mut raw, &flags).unwrap();
        assert_eq!(conn_payload.client_id, "client");
        assert_eq!(conn_payload.will_topic.as_deref(), Some("will"));
        assert_eq!(conn_payload.will_message.as_deref(), Some(&b"bye"[..]));
        assert_eq!(conn_payload.username.as_deref(), Some("user"));
        assert_eq!(conn_payload.password.as_deref(), Some(&b"pass"[..]));
    }
}
