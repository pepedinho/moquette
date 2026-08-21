pub struct BufferReader<'a> {
    slice: &'a [u8],
}

impl<'a> BufferReader<'a> {
    pub fn new(slice: &'a [u8]) -> Self {
        Self { slice }
    }

    pub fn is_empty(&self) -> bool {
        self.slice.is_empty()
    }

    pub fn len(&self) -> usize {
        self.slice.len()
    }
    pub fn read_u8(&mut self) -> Result<u8, &'static str> {
        if self.slice.is_empty() {
            return Err("Buffer underflow while reading u8");
        }

        let byte = self.slice[0];
        self.slice = &self.slice[1..];
        Ok(byte)
    }

    pub fn read_u16(&mut self) -> Result<u16, &'static str> {
        if self.slice.len() < 2 {
            return Err("Buffer underflow while reading u16");
        }
        let value = u16::from_be_bytes([self.slice[0], self.slice[1]]);
        self.slice = &self.slice[2..];
        Ok(value)
    }

    pub fn read_string(&mut self) -> Result<String, &'static str> {
        let len = self.read_u16()? as usize;
        if self.slice.len() < len {
            return Err("Buffer underflow while reading string");
        }
        let s = std::str::from_utf8(&self.slice[..len])
            .map_err(|_| "Invalid UTF-8 string")?
            .to_string();
        self.slice = &self.slice[len..];
        Ok(s)
    }

    pub fn read_bytes(&mut self) -> Result<Vec<u8>, &'static str> {
        let len = self.read_u16()? as usize;
        if self.slice.len() < len {
            return Err("Buffer underflow while reading bytes");
        }
        let vec = self.slice[..len].to_vec();
        self.slice = &self.slice[len..];
        Ok(vec)
    }

    pub fn read_remaining(&mut self) -> Vec<u8> {
        let vec = self.slice.to_vec();
        self.slice = &[];
        vec
    }
}
