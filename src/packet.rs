/// Control Packet Type representation
/// they all got a value from 1 to 14
pub enum ControlPacketType {
    Connect,
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
pub struct PublishFlag {
    dup: bool,
    qos: u8,
    retain: bool,
}

pub struct ControlPacket {
    header: FixedHeaderHeader,
    vheader: Option<FixedHeaderHeader>,
    payload: Option<Vec<u8>>,
}

pub struct FixedHeaderHeader {
    r#type: ControlPacketType,
    flag: u8,
    remaining_length: usize,
}

impl FixedHeaderHeader {
    pub fn from_raw_bytes(bytes: &[u8]) -> Self {
        let r#type = bytes[0] >> 4;
        let flag = bytes[0] & 0x0F;

        // let remaining_length = _; //TODO: parse remaining_lenght
        todo!()
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
}
