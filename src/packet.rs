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
    vheader: Option<Vec<u8>>,
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

impl FixedHeader {
    pub fn from_raw_bytes(bytes: &[u8]) -> Result<Self, &'static str> {
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
