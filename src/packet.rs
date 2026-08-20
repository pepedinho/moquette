use std::mem::transmute;

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

impl TryFrom<u8> for ControlPacketType {
    type Error = &'static str;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        if value > 0 && value <= 14 {
            //BUG: can maybe cause arbitrary segfault so
            //rewrite it in safe way.
            let o: ControlPacketType = unsafe { transmute(value as i32) };
            Ok(o)
        } else {
            Err("Control packet type representation musty be between 1 and 14")
        }
    }
}

/// Flag for Pusblish control packet
/// see: https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Table_3.1_-
#[derive(PartialEq, Debug, Default)]
pub struct PublishFlag {
    dup: bool,
    qos: u8,
    retain: bool,
}

pub struct ControlPacket {
    header: FixedHeader,
    vheader: VariableHeader,
    payload: Option<Vec<u8>>,
}

pub struct FixedHeader {
    r#type: ControlPacketType,
    remaining_length: RemainingLength,
}

pub struct RemainingLength {
    /// Length found in header.
    pub l: usize,
    /// The Bytes on which the length was encoded.
    pub br: usize,
}

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

pub struct ConnectFlag {
    pub username_flag: bool,
    pub password_flag: bool,
    pub will_retain: bool,
    pub will_qos: u8,
    pub will_flag: bool,
    pub clean_session: bool,
}

impl TryFrom<u8> for ConnectFlag {
    type Error = &'static str;

    fn try_from(byte: u8) -> Result<Self, Self::Error> {
        if (byte & 0x01) != 0 {
            return Err("Bit of 0 of Connext flag must be 0");
        }

        let will_qos = (byte & 0x18) >> 3;
        if will_qos > 2 {
            return Err("Invalid Will QoS level (must be 0, 1 or 2)");
        }

        let will_flag = (byte & 0x04) != 0;

        if !will_flag && (will_qos != 0) || (byte & 0x20) != 0 {
            return Err("Will QoS and Will Retain must be 0 if Will Flag is 0");
        }

        Ok(ConnectFlag {
            username_flag: (byte & 0x80) != 0,
            password_flag: (byte & 0x40) != 0,
            will_retain: (byte & 0x20) != 0,
            will_qos,
            will_flag,
            clean_session: (byte & 0x02) != 0,
        })
    }
}

impl ControlPacket {
    pub fn parse(bytes: &[u8]) -> Result<Self, &'static str> {
        let header = FixedHeader::from_raw_bytes(bytes)?;
        let offset = 1 + header.remaining_length.br;
        let total_expected_len = offset + header.remaining_length.l;

        if bytes.len() < total_expected_len {
            return Err("Incomplete packet: byte slice is shorter than remaining length");
        }

        let body = &bytes[offset..total_expected_len];

        let (vheader, payload) = match header.r#type {
            ControlPacketType::Publish(ref flags) => Self::parse_publish_body(body, flags)?,
            ControlPacketType::Puback
            | ControlPacketType::Pubrec
            | ControlPacketType::Pubrel
            | ControlPacketType::Pubcomp
            | ControlPacketType::Unsuback => Self::parse_id(body)?,
            ControlPacketType::Subscribe
            | ControlPacketType::Suback
            | ControlPacketType::Unsubscribe => Self::parse_subscribe_body(body)?,
            ControlPacketType::Connect => Self::parse_connect_body(body)?,
            _ => todo!(),
        };

        Ok(Self {
            header,
            vheader,
            payload,
        })
    }

    fn parse_id(body: &[u8]) -> Result<(VariableHeader, Option<Vec<u8>>), &'static str> {
        if body.len() < 2 {
            return Err("Publish body too short for topic length");
        }

        let packet_id = u16::from_be_bytes([body[0], body[1]]);

        Ok((VariableHeader::PacketId(packet_id), None))
    }

    fn parse_subscribe_body(
        body: &[u8],
    ) -> Result<(VariableHeader, Option<Vec<u8>>), &'static str> {
        if body.len() < 2 {
            return Err("SUBSCRIBE body too short");
        }
        let packet_id = u16::from_be_bytes([body[0], body[1]]);
        let offset = 2;
        if offset >= body.len() {
            return Err("SUBSCRIBE payload must contain at least one topic filter");
        }

        let payload = body[offset..].to_vec();

        Ok((VariableHeader::PacketId(packet_id), Some(payload)))
    }

    fn parse_publish_body(
        body: &[u8],
        flags: &PublishFlag,
    ) -> Result<(VariableHeader, Option<Vec<u8>>), &'static str> {
        if body.len() < 2 {
            return Err("Publish body too short for topic length");
        }

        let str_len = u16::from_be_bytes([body[0], body[1]]) as usize;
        let mut offset = 2 + str_len;

        if body.len() < offset {
            return Err("Publish body too short for topic name");
        }

        let topic_name = std::str::from_utf8(&body[2..offset])
            .map_err(|_| "Non utf8 string detected in publish Variable Header")?;

        let packet_id = if flags.qos > 0 {
            if body.len() < offset + 2 {
                return Err("Publish body too short for packet id");
            }
            let id = &body[offset..offset + 2];
            let id = u16::from_be_bytes([id[0], id[1]]);
            offset += 2;
            Some(id)
        } else {
            None
        };

        let vheader = VariableHeader::Publish {
            topic_name: topic_name.to_string(),
            packet_id,
        };

        let payload = body[offset..].to_vec();

        Ok((vheader, Some(payload)))
    }

    fn parse_connect_body(body: &[u8]) -> Result<(VariableHeader, Option<Vec<u8>>), &'static str> {
        if body.len() < 2 {
            return Err("CONNECT body too short");
        }

        let str_len = u16::from_be_bytes([body[0], body[1]]) as usize;
        let mut offset = 2 + str_len;

        if body.len() < offset {
            return Err("CONNECT body to short for protocol name");
        }

        let protocol_name = std::str::from_utf8(&body[2..offset])
            .map_err(|_| "Non utf-8 struct detected in Connect protocol name")?;

        if body.len() < offset + 4 {
            return Err("CONNECT body too short for variable header fields");
        }

        let protocol_level = body[offset];
        offset += 1;

        let connect_flag = ConnectFlag::try_from(body[offset])?;
        offset += 1;

        let keep_alive = u16::from_be_bytes([body[offset], body[offset + 1]]);
        offset += 2;

        let payload = body[offset..].to_vec();

        Ok((
            VariableHeader::Connect {
                protocol_name: protocol_name.to_string(),
                protocol_level,
                connect_flag,
                keep_alive,
            },
            Some(payload),
        ))
    }
}

impl FixedHeader {
    pub fn from_raw_bytes(bytes: &[u8]) -> Result<Self, &'static str> {
        if bytes.is_empty() {
            return Err("Empty buffer: cannot parse fixed header");
        }

        let r#type = bytes[0] >> 4;
        let flag = bytes[0] & 0x0F;

        let mut r#type = ControlPacketType::try_from(r#type)?;
        if !Self::check_flag(&mut r#type, flag) {
            //TODO: Close connection here
            return Err("Invalid flag detected");
        }
        let remaining_length = Self::decoding_remainin_length(&bytes[1..])?;

        Ok(Self {
            r#type,
            remaining_length,
        })
    }

    fn check_flag(packet_type: &mut ControlPacketType, flag: u8) -> bool {
        match packet_type {
            ControlPacketType::Publish(f) => {
                f.dup = (flag & 0x08) != 0;
                f.qos = (flag & 0x06) >> 1;
                f.retain = (flag & 0x01) != 0;
                if f.qos == 3 {
                    return false;
                }
                return true;
            }
            ControlPacketType::Pubrel
            | ControlPacketType::Subscribe
            | ControlPacketType::Unsubscribe => {
                if flag == 0x2 {
                    return true;
                }
            }
            _ => {
                if flag == 0x00 {
                    return true;
                }
            }
        }
        false
    }

    fn decoding_remainin_length(bytes: &[u8]) -> Result<RemainingLength, &'static str> {
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
            assert_eq!(t, ControlPacketType::Publish(PublishFlag::default()));
        } else {
            panic!("Failed to convert {:?}", cp_type.ok());
        }
    }

    #[test]
    fn decode_header_remaining_length() {
        // Length to 0
        let bytes = &[0x00];
        let res = FixedHeader::decoding_remainin_length(bytes).unwrap();
        assert_eq!(res.l, 0);
        assert_eq!(res.br, 1);

        // Length to 127 (max on 1 byte)
        let bytes = &[0x7F];
        let res = FixedHeader::decoding_remainin_length(bytes).unwrap();
        assert_eq!(res.l, 127);
        assert_eq!(res.br, 1);

        // Length to 128 (overlap on 2 byte)
        let bytes = &[0x80, 0x01];
        let res = FixedHeader::decoding_remainin_length(bytes).unwrap();
        assert_eq!(res.l, 128);
        assert_eq!(res.br, 2);

        // Incomptete byte (continue bit is on but no other bytes was provided)
        let bytes = &[0x80];
        let res = FixedHeader::decoding_remainin_length(bytes);
        assert!(res.is_err());
    }
}
