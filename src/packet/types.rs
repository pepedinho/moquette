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
    Publish,
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
        match value {
            1 => Ok(Self::Connect),
            2 => Ok(Self::Connack),
            3 => Ok(Self::Publish),
            4 => Ok(Self::Puback),
            5 => Ok(Self::Pubrec),
            6 => Ok(Self::Pubrel),
            7 => Ok(Self::Pubcomp),
            8 => Ok(Self::Subscribe),
            9 => Ok(Self::Suback),
            10 => Ok(Self::Unsubscribe),
            11 => Ok(Self::Unsuback),
            12 => Ok(Self::Pingreq),
            13 => Ok(Self::Pingresp),
            14 => Ok(Self::Disconnect),
            _ => Err("Control packet type representation must be between 1 and 14"),
        }
    }
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
    pub flags: u8,
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

impl TryFrom<u8> for ConnectFlag {
    type Error = &'static str;

    fn try_from(byte: u8) -> Result<Self, Self::Error> {
        if (byte & 0x01) != 0 {
            return Err("Bit 0 of Connect flag must be 0");
        }

        let will_qos = (byte & 0x18) >> 3;
        if will_qos > 2 {
            return Err("Invalid Will QoS level (must be 0, 1 or 2)");
        }

        let will_flag = (byte & 0x04) != 0;

        if (!will_flag && will_qos != 0) || (!will_flag && (byte & 0x20) != 0) {
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
