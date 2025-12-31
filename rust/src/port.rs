use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt, Stdin, Stdout};
use tokio::sync::mpsc;

/// Packet size configuration for Erlang port communication.
/// This should match the `{packet, N}` option in Erlang's `open_port/2`.
#[derive(Clone, Copy)]
pub enum PacketSize {
    /// 1-byte length prefix (max message size: 255 bytes)
    One,
    /// 2-byte length prefix (max message size: 65,535 bytes)
    Two,
    /// 4-byte length prefix (max message size: 4,294,967,295 bytes)
    Four,
}

impl PacketSize {
    fn header_len(&self) -> usize {
        match self {
            PacketSize::One => 1,
            PacketSize::Two => 2,
            PacketSize::Four => 4,
        }
    }

    fn read_length(&self, header: &[u8]) -> usize {
        match self {
            PacketSize::One => header[0] as usize,
            PacketSize::Two => u16::from_be_bytes([header[0], header[1]]) as usize,
            PacketSize::Four => {
                u32::from_be_bytes([header[0], header[1], header[2], header[3]]) as usize
            }
        }
    }

    fn write_length(&self, len: usize) -> Vec<u8> {
        match self {
            PacketSize::One => vec![len as u8],
            PacketSize::Two => (len as u16).to_be_bytes().to_vec(),
            PacketSize::Four => (len as u32).to_be_bytes().to_vec(),
        }
    }
}

/// A request received from Erlang, with its correlation ID.
pub struct Request {
    pub id: u32,
    pub data: Vec<u8>,
}

/// A response to send back to Erlang.
pub struct Response {
    pub id: u32,
    pub data: Vec<u8>,
}

/// Reader half of the port - receives requests from Erlang.
pub struct PortReader {
    packet_size: PacketSize,
    stdin: Stdin,
}

/// Writer half of the port - sends responses to Erlang.
pub struct PortWriter {
    packet_size: PacketSize,
    stdout: Stdout,
}

/// Sender for responses - clone this for each request handler task.
pub type ResponseTx = mpsc::Sender<Response>;

/// Receiver for responses - used by the writer task.
pub type ResponseRx = mpsc::Receiver<Response>;

/// Create a new port split into reader and writer halves, plus a response channel.
///
/// Returns (reader, writer, response_sender, response_receiver).
///
/// Typical usage:
/// - Spawn a task that uses `writer` to consume from `response_rx`
/// - In main loop, read from `reader` and spawn handler tasks with cloned `response_tx`
pub fn new_port(buffer_size: usize) -> (PortReader, PortWriter, ResponseTx, ResponseRx) {
    new_port_with_packet_size(PacketSize::Four, buffer_size)
}

/// Create a new port with a specific packet size.
pub fn new_port_with_packet_size(
    packet_size: PacketSize,
    buffer_size: usize,
) -> (PortReader, PortWriter, ResponseTx, ResponseRx) {
    let (tx, rx) = mpsc::channel(buffer_size);
    let reader = PortReader {
        packet_size,
        stdin: tokio::io::stdin(),
    };
    let writer = PortWriter {
        packet_size,
        stdout: tokio::io::stdout(),
    };
    (reader, writer, tx, rx)
}

impl PortReader {
    /// Read a request from Erlang.
    /// Returns `Ok(None)` on EOF (port closed).
    ///
    /// Message format: `<<RequestId:32/big, Payload/binary>>`
    pub async fn read_request(&mut self) -> io::Result<Option<Request>> {
        let header_len = self.packet_size.header_len();
        let mut header = vec![0u8; header_len];

        // Read the length header
        match self.stdin.read_exact(&mut header).await {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }

        let msg_len = self.packet_size.read_length(&header);
        if msg_len < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "message too short for request ID",
            ));
        }

        // Read the message body
        let mut body = vec![0u8; msg_len];
        self.stdin.read_exact(&mut body).await?;

        // Extract request ID (first 4 bytes, big-endian)
        let id = u32::from_be_bytes([body[0], body[1], body[2], body[3]]);
        let data = body[4..].to_vec();

        Ok(Some(Request { id, data }))
    }
}

impl PortWriter {
    /// Write a response to Erlang.
    ///
    /// Message format: `<<RequestId:32/big, Payload/binary>>`
    pub async fn write_response(&mut self, response: &Response) -> io::Result<()> {
        let id_bytes = response.id.to_be_bytes();
        let total_len = 4 + response.data.len();
        let header = self.packet_size.write_length(total_len);

        self.stdout.write_all(&header).await?;
        self.stdout.write_all(&id_bytes).await?;
        self.stdout.write_all(&response.data).await?;
        self.stdout.flush().await?;
        Ok(())
    }

    /// Run the writer loop, consuming responses from the channel.
    pub async fn run(mut self, mut rx: ResponseRx) -> io::Result<()> {
        while let Some(response) = rx.recv().await {
            self.write_response(&response).await?;
        }
        Ok(())
    }
}
