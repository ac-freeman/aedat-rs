use std::fs::File;
use std::io::{Read};
use std::net::{TcpStream, ToSocketAddrs};
use std::os::unix::net::UnixStream;
use num_derive::FromPrimitive;
use thiserror::Error;

#[allow(dead_code, unused_imports)]
#[path = "./ioheader_generated.rs"]
pub mod ioheader_generated;

#[allow(dead_code, unused_imports)]
#[path = "./events_generated.rs"]
mod events_generated;

#[allow(dead_code, unused_imports)]
#[path = "./frame_generated.rs"]
mod frame_generated;

#[allow(dead_code, unused_imports)]
#[path = "./imus_generated.rs"]
mod imus_generated;

#[allow(dead_code, unused_imports)]
#[path = "./triggers_generated.rs"]
mod triggers_generated;

const MAGIC_NUMBER: &str = "#!AER-DAT4.0\r\n";


#[allow(missing_docs)]
#[derive(Error, Debug)]
pub enum ParseError {
    #[error("Parse error: `{0}`")]
    General(String),

    #[error("Unsupported stream type: `{0}`")]
    UnsupportedStreamType(String),

    #[error("FlatBuffer error")]
    FlatBuffer(#[from] flatbuffers::InvalidFlatbuffer),

    #[error("Utf8 error")]
    Utf8(#[from] std::str::Utf8Error),

    #[error("RoxmlTree error")]
    RoxmlTree(#[from] roxmltree::Error),

    #[error("ParseIntError error")]
    ParseInt(#[from] std::num::ParseIntError),

    #[error("IO error")]
    Io(#[from] std::io::Error),
}

trait Source:std::io::Read {}
impl Source for File {}
impl Source for UnixStream {}
impl Source for TcpStream {}

#[derive(FromPrimitive, Copy, Clone)]
pub enum StreamContent {
    Events,
    Frame,
    Imus,
    Triggers,
}

impl StreamContent {
    fn from(identifier: &str) -> Result<Self, ParseError> {
        match identifier {
            "EVTS" => Ok(StreamContent::Events),
            "FRME" => Ok(StreamContent::Frame),
            "IMUS" => Ok(StreamContent::Imus),
            "TRIG" => Ok(StreamContent::Triggers),
            _ => Err(ParseError::UnsupportedStreamType("unsupported stream type".to_string())),
        }
    }
}

impl std::fmt::Display for StreamContent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            formatter,
            "{}",
            match self {
                StreamContent::Events => "EVTS",
                StreamContent::Frame => "FRME",
                StreamContent::Imus => "IMUS",
                StreamContent::Triggers => "TRIG",
            }
        )
    }
}

pub struct Stream {
    pub content: StreamContent,
    pub width: u16,
    pub height: u16,
}

pub struct Decoder {
    pub id_to_stream: std::collections::HashMap<u32, Stream>,
    file: Box<dyn Source>,
    position: i64,
    compression: ioheader_generated::Compression,
    file_data_position: i64,
}

unsafe impl Send for Decoder {}

impl Decoder {
    pub fn new_from_file<P: std::convert::AsRef<std::path::Path>>(path: P) -> Result<Self, ParseError> {
        let mut decoder = Decoder {
            id_to_stream: std::collections::HashMap::new(),
            file: Box::new(std::fs::File::open(path)?),
            position: 0i64,
            file_data_position: 0,
            compression: ioheader_generated::Compression::None,
        };
        {
            let mut magic_number_buffer = [0; MAGIC_NUMBER.len()];
            decoder.file.read_exact(&mut magic_number_buffer)?;
            if std::str::from_utf8(&magic_number_buffer)? != MAGIC_NUMBER {
                return Err(ParseError::General(
                    "the file does not contain AEDAT4 data (wrong magic number)".to_string(),
                ));
            }
            decoder.position += MAGIC_NUMBER.len() as i64;
        }
        decoder = read_io_header(decoder)?;

        Ok(decoder)
    }


    pub fn new_from_unix_stream<P: std::convert::AsRef<std::path::Path> + Clone>(
        path: P) -> Result<Self, ParseError> {
        let mut decoder = Decoder {
            id_to_stream: std::collections::HashMap::new(),
            file: Box::new(UnixStream::connect(path)?),
            position: 0i64,
            file_data_position: -1,
            compression: ioheader_generated::Compression::None,
        };
        decoder = read_io_header(decoder)?;
        Ok(decoder)
    }

    pub fn new_from_tcp_stream<P: ToSocketAddrs + Clone>(
        path: P,
    ) -> Result<Self, ParseError> {
        let mut decoder = Decoder {
            id_to_stream: std::collections::HashMap::new(),
            file: Box::new(TcpStream::connect(path)?),
            position: 0i64,
            file_data_position: -1,
            compression: ioheader_generated::Compression::None,
        };
        decoder = read_io_header(decoder)?;
        Ok(decoder)
    }
}

fn read_io_header(mut decoder: Decoder) -> Result<Decoder, ParseError> {
    let length = {
        let mut bytes = [0; 4];
        decoder.file.read_exact(&mut bytes)?;
        u32::from_le_bytes(bytes)
    };
    decoder.position += 4i64 + length as i64;
    {
        let mut buffer = std::vec![0; length as usize];
        decoder.file.read_exact(&mut buffer)?;
        let ioheader = unsafe { ioheader_generated::root_as_ioheader_unchecked(&buffer) };
        decoder.compression = ioheader.compression();
        decoder.file_data_position = ioheader.file_data_position();
        let description = match ioheader.description() {
            Some(content) => content,
            None => return Err(ParseError::General("the description is empty".to_string())),
        };
        let document = roxmltree::Document::parse(description)?;
        let dv_node = match document.root().first_child() {
            Some(content) => content,
            None => return Err(ParseError::General("the description has no dv node".to_string())),
        };
        if !dv_node.has_tag_name("dv") {
            return Err(ParseError::General("unexpected dv node tag".to_string()));
        }
        let output_node = match dv_node.children().find(|node| {
            node.is_element()
                && node.has_tag_name("node")
                && node.attribute("name") == Some("outInfo")
        }) {
            Some(content) => content,
            None => return Err(ParseError::General("the description has no output node".to_string())),
        };
        for stream_node in output_node.children() {
            if stream_node.is_element() && stream_node.has_tag_name("node") {
                if !stream_node.has_tag_name("node") {
                    return Err(ParseError::General("unexpected stream node tag".to_string()));
                }
                let stream_id = match stream_node.attribute("name") {
                    Some(content) => content,
                    None => return Err(ParseError::General("missing stream node id".to_string())),
                }
                    .parse::<u32>()?;
                let identifier = match stream_node.children().find(|node| {
                    node.is_element()
                        && node.has_tag_name("attr")
                        && node.attribute("key") == Some("typeIdentifier")
                }) {
                    Some(content) => match content.text() {
                        Some(content) => content,
                        None => {
                            return Err(ParseError::General("empty stream node type identifier".to_string()))
                        }
                    },
                    None => return Err(ParseError::General("missing stream node type identifier".to_string())),
                }
                    .to_string();
                let mut width = 0u16;
                let mut height = 0u16;
                if identifier == "EVTS" || identifier == "FRME" {
                    let info_node = match stream_node.children().find(|node| {
                        node.is_element()
                            && node.has_tag_name("node")
                            && node.attribute("name") == Some("info")
                    }) {
                        Some(content) => content,
                        None => return Err(ParseError::General("missing info node".to_string())),
                    };
                    width = match info_node.children().find(|node| {
                        node.is_element()
                            && node.has_tag_name("attr")
                            && node.attribute("key") == Some("sizeX")
                    }) {
                        Some(content) => match content.text() {
                            Some(content) => content,
                            None => return Err(ParseError::General("empty sizeX attribute".to_string())),
                        },
                        None => return Err(ParseError::General("missing sizeX attribute".to_string())),
                    }
                        .parse::<u16>()?;
                    height = match info_node.children().find(|node| {
                        node.is_element()
                            && node.has_tag_name("attr")
                            && node.attribute("key") == Some("sizeY")
                    }) {
                        Some(content) => match content.text() {
                            Some(content) => content,
                            None => return Err(ParseError::General("empty sizeX attribute".to_string())),
                        },
                        None => return Err(ParseError::General("missing sizeX attribute".to_string())),
                    }
                        .parse::<u16>()?;
                }
                if decoder
                    .id_to_stream
                    .insert(
                        stream_id,
                        Stream {
                            content: StreamContent::from(&identifier)?,
                            width,
                            height,
                        },
                    )
                    .is_some()
                {
                    return Err(ParseError::General("duplicated stream id".to_string()));
                }
            }
        }
    }
    if decoder.id_to_stream.is_empty() {
        return Err(ParseError::General("no stream found in the description".to_string()));
    }
    Ok(decoder)

}

#[derive(Debug, Clone)]
pub struct Packet {
    pub buffer: std::vec::Vec<u8>,
    pub stream_id: u32,
}

impl Iterator for Decoder {
    type Item = Result<Packet, ParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.file_data_position > -1 && self.position == self.file_data_position {
            return None;
        }
        let mut packet = Packet {
            buffer: Vec::new(),
            stream_id: {
                let mut bytes = [0; 4];
                match self.file.read_exact(&mut bytes) {
                    Ok(()) => (),
                    Err(_) => return None,
                }
                u32::from_le_bytes(bytes)
            },
        };
        let length = {
            let mut bytes = [0; 4];
            if let Err(error) = self.file.read_exact(&mut bytes) {
                return Some(Err(ParseError::from(error)));
            }
            u32::from_le_bytes(bytes)
        };
        self.position += 8i64 + length as i64;
        let mut raw_buffer = std::vec![0; length as usize];
        if let Err(error) = self.file.read_exact(&mut raw_buffer) {
            return Some(Err(ParseError::from(error)));
        }
        match self.compression {
            ioheader_generated::Compression::None => {
                std::mem::swap(&mut raw_buffer, &mut packet.buffer)
            }
            ioheader_generated::Compression::Lz4 | ioheader_generated::Compression::Lz4High => {
                match lz4::Decoder::new(&raw_buffer[..]) {
                    Ok(mut result) => {
                        if let Err(error) = result.read_to_end(&mut packet.buffer) {
                            return Some(Err(ParseError::from(error)));
                        }
                    }
                    Err(error) => return Some(Err(ParseError::from(error))),
                }
            }
            ioheader_generated::Compression::Zstd | ioheader_generated::Compression::ZstdHigh => {
                match zstd::stream::Decoder::new(&raw_buffer[..]) {
                    Ok(mut result) => {
                        if let Err(error) = result.read_to_end(&mut packet.buffer) {
                            return Some(Err(ParseError::from(error)));
                        }
                    }
                    Err(error) => return Some(Err(ParseError::from(error))),
                }
            }
            _ => return Some(Err(ParseError::General("unknown compression algorithm".to_string()))),
        }
        let expected_content = &(match self.id_to_stream.get(&packet.stream_id) {
            Some(content) => content,
            None => return Some(Err(ParseError::General("unknown stream id".to_string()))),
        }
            .content);
        if !flatbuffers::buffer_has_identifier(&packet.buffer, &expected_content.to_string(), true)
        {
            return Some(Err(ParseError::General(
                "the stream id and the identifier do not match".to_string(),
            )));
        }
        Some(Ok(packet))
    }
}
