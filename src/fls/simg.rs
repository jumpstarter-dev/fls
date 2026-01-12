//! Android Sparse Image (simg) format parser
//!
//! The sparse image format is designed for efficient block device flashing.
//! It encodes disk images by separating actual data from empty space, allowing
//! the flasher to skip writing to unallocated regions.
//!
//! Format specification:
//! - Header: 28 bytes (magic, version, sizes, block count, chunk count)
//! - Chunks: Variable length, each with 12-byte header
//!   - RAW: Contains actual data blocks
//!   - FILL: 4-byte pattern repeated across blocks
//!   - DONT_CARE: Empty blocks to skip (seek over)

use std::io;

/// Sparse image magic number: 0xED26FF3A
pub const SPARSE_HEADER_MAGIC: u32 = 0xED26FF3A;

/// Standard sparse image header size
pub const SPARSE_HEADER_SIZE: usize = 28;

/// Standard chunk header size
pub const CHUNK_HEADER_SIZE: usize = 12;

/// Chunk type: Raw data
pub const CHUNK_TYPE_RAW: u16 = 0xCAC1;

/// Chunk type: Fill pattern
pub const CHUNK_TYPE_FILL: u16 = 0xCAC2;

/// Chunk type: Don't care (skip)
pub const CHUNK_TYPE_DONT_CARE: u16 = 0xCAC3;

/// Parsed sparse image header
#[derive(Debug, Clone)]
pub struct SparseHeader {
    /// Block size in bytes (typically 4096)
    pub block_size: u32,
    /// Total blocks in the output image
    pub total_blocks: u32,
    /// Total chunks in the sparse image
    pub total_chunks: u32,
    /// Image checksum (0 if not provided)
    #[allow(dead_code)]
    pub image_checksum: u32,
}

impl SparseHeader {
    /// Total output image size in bytes
    pub fn image_size(&self) -> u64 {
        self.total_blocks as u64 * self.block_size as u64
    }
}

/// Type of sparse chunk
#[derive(Debug, Clone)]
pub enum ChunkType {
    /// Raw data blocks - contains actual data to write
    Raw,
    /// Fill pattern - 4-byte value repeated across all blocks
    Fill([u8; 4]),
    /// Don't care - skip these blocks (seek over them)
    DontCare,
}

/// Parsed chunk header with type information
#[derive(Debug, Clone)]
pub struct SparseChunk {
    /// Type of this chunk
    pub chunk_type: ChunkType,
    /// Number of output blocks this chunk represents
    pub output_blocks: u32,
    /// Size of data following the header (0 for DONT_CARE)
    pub data_size: u64,
}

impl SparseChunk {
    /// Size in bytes this chunk will occupy in the output image
    pub fn output_size(&self, block_size: u32) -> u64 {
        self.output_blocks as u64 * block_size as u64
    }
}

/// Error types for sparse image parsing
#[derive(Debug)]
pub enum SparseError {
    /// Invalid magic number
    InvalidMagic(u32),
    /// Unsupported version
    UnsupportedVersion { major: u16, minor: u16 },
    /// Unexpected header size
    InvalidHeaderSize(u16),
    /// Unexpected chunk header size
    InvalidChunkHeaderSize(u16),
    /// Unknown chunk type
    UnknownChunkType(u16),
    /// Chunk size mismatch
    ChunkSizeMismatch { expected: u32, actual: u32 },
    /// IO error
    Io(io::Error),
    /// Not enough data
    UnexpectedEof,
    /// Arithmetic overflow in chunk calculations
    ChunkSizeOverflow { chunk_sz: u32, block_size: u32 },
    /// Output size mismatch at completion
    OutputSizeMismatch { expected: u64, actual: u64 },
}

impl std::fmt::Display for SparseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SparseError::InvalidMagic(magic) => {
                write!(
                    f,
                    "Invalid sparse image magic: 0x{:08X} (expected 0x{:08X})",
                    magic, SPARSE_HEADER_MAGIC
                )
            }
            SparseError::UnsupportedVersion { major, minor } => {
                write!(f, "Unsupported sparse image version: {}.{}", major, minor)
            }
            SparseError::InvalidHeaderSize(size) => {
                write!(
                    f,
                    "Invalid header size: {} (expected {})",
                    size, SPARSE_HEADER_SIZE
                )
            }
            SparseError::InvalidChunkHeaderSize(size) => {
                write!(
                    f,
                    "Invalid chunk header size: {} (expected {})",
                    size, CHUNK_HEADER_SIZE
                )
            }
            SparseError::UnknownChunkType(chunk_type) => {
                write!(f, "Unknown chunk type: 0x{:04X}", chunk_type)
            }
            SparseError::ChunkSizeMismatch { expected, actual } => {
                write!(
                    f,
                    "Chunk size mismatch: expected {}, got {}",
                    expected, actual
                )
            }
            SparseError::Io(e) => write!(f, "IO error: {}", e),
            SparseError::UnexpectedEof => write!(f, "Unexpected end of sparse image data"),
            SparseError::ChunkSizeOverflow {
                chunk_sz,
                block_size,
            } => write!(
                f,
                "Chunk size calculation overflow: {} blocks × {} bytes/block",
                chunk_sz, block_size
            ),
            SparseError::OutputSizeMismatch { expected, actual } => write!(
                f,
                "Output size mismatch: expected {} bytes, got {} bytes",
                expected, actual
            ),
        }
    }
}

impl std::error::Error for SparseError {}

impl From<io::Error> for SparseError {
    fn from(e: io::Error) -> Self {
        SparseError::Io(e)
    }
}

/// Parse a sparse image header from a byte slice
///
/// Returns the header if valid, or an error if the data is not a valid sparse image.
pub fn parse_header(data: &[u8]) -> Result<SparseHeader, SparseError> {
    if data.len() < SPARSE_HEADER_SIZE {
        return Err(SparseError::UnexpectedEof);
    }

    let magic = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
    if magic != SPARSE_HEADER_MAGIC {
        return Err(SparseError::InvalidMagic(magic));
    }

    let major = u16::from_le_bytes([data[4], data[5]]);
    let minor = u16::from_le_bytes([data[6], data[7]]);

    // Reject images with major version > 1
    if major != 1 {
        return Err(SparseError::UnsupportedVersion { major, minor });
    }

    let file_hdr_sz = u16::from_le_bytes([data[8], data[9]]);
    let chunk_hdr_sz = u16::from_le_bytes([data[10], data[11]]);

    if file_hdr_sz != SPARSE_HEADER_SIZE as u16 {
        return Err(SparseError::InvalidHeaderSize(file_hdr_sz));
    }

    if chunk_hdr_sz != CHUNK_HEADER_SIZE as u16 {
        return Err(SparseError::InvalidChunkHeaderSize(chunk_hdr_sz));
    }

    let block_size = u32::from_le_bytes([data[12], data[13], data[14], data[15]]);
    let total_blocks = u32::from_le_bytes([data[16], data[17], data[18], data[19]]);
    let total_chunks = u32::from_le_bytes([data[20], data[21], data[22], data[23]]);
    let image_checksum = u32::from_le_bytes([data[24], data[25], data[26], data[27]]);

    Ok(SparseHeader {
        block_size,
        total_blocks,
        total_chunks,
        image_checksum,
    })
}

/// Parse a chunk header from a byte slice
///
/// For FILL chunks, the additional data bytes (fill pattern) are also read.
/// Returns the chunk and the number of bytes consumed from the slice.
pub fn parse_chunk_header(
    data: &[u8],
    block_size: u32,
) -> Result<(SparseChunk, usize), SparseError> {
    if data.len() < CHUNK_HEADER_SIZE {
        return Err(SparseError::UnexpectedEof);
    }

    let chunk_type_raw = u16::from_le_bytes([data[0], data[1]]);
    // reserved: u16 at bytes 2-3
    let chunk_sz = u32::from_le_bytes([data[4], data[5], data[6], data[7]]);
    let total_sz = u32::from_le_bytes([data[8], data[9], data[10], data[11]]);

    // Helper to validate total_sz matches expected value
    let check_size = |expected: u32| -> Result<(), SparseError> {
        if total_sz != expected {
            return Err(SparseError::ChunkSizeMismatch {
                expected,
                actual: total_sz,
            });
        }
        Ok(())
    };

    // Helper to validate we have enough data for extended header
    let check_data_len = |required: usize| -> Result<(), SparseError> {
        if data.len() < required {
            return Err(SparseError::UnexpectedEof);
        }
        Ok(())
    };

    let (chunk_type, header_consumed, data_size) = match chunk_type_raw {
        CHUNK_TYPE_RAW => {
            // Use checked arithmetic to prevent overflow
            let data_size_u64 = (chunk_sz as u64).checked_mul(block_size as u64).ok_or(
                SparseError::ChunkSizeOverflow {
                    chunk_sz,
                    block_size,
                },
            )?;
            let expected_total = CHUNK_HEADER_SIZE as u64 + data_size_u64;
            if expected_total > u32::MAX as u64 {
                return Err(SparseError::ChunkSizeOverflow {
                    chunk_sz,
                    block_size,
                });
            }
            check_size(expected_total as u32)?;
            (ChunkType::Raw, CHUNK_HEADER_SIZE, data_size_u64)
        }
        CHUNK_TYPE_FILL => {
            check_size(16)?;
            check_data_len(16)?;
            let fill_value = [data[12], data[13], data[14], data[15]];
            (ChunkType::Fill(fill_value), 16, 0)
        }
        CHUNK_TYPE_DONT_CARE => {
            check_size(12)?;
            (ChunkType::DontCare, CHUNK_HEADER_SIZE, 0)
        }
        _ => return Err(SparseError::UnknownChunkType(chunk_type_raw)),
    };

    Ok((
        SparseChunk {
            chunk_type,
            output_blocks: chunk_sz,
            data_size,
        },
        header_consumed,
    ))
}

/// Streaming sparse image parser
///
/// Processes sparse image data incrementally, emitting write commands
/// that can be executed on a block device with seek support.
pub struct SparseParser {
    header: Option<SparseHeader>,
    chunks_processed: u32,
    current_output_offset: u64,
    state: ParserState,
    /// Buffer for accumulating header/chunk header data
    buffer: Vec<u8>,
    /// Current RAW chunk remaining bytes to pass through.
    /// Limited to u32 (~4GB) for streaming efficiency; chunks exceeding this
    /// limit return ChunkSizeOverflow error during parsing.
    raw_remaining: u32,
}

#[derive(Debug, Clone, PartialEq)]
enum ParserState {
    /// Waiting for header
    WaitingHeader,
    /// Waiting for chunk header
    WaitingChunkHeader,
    /// Passing through RAW data
    RawData,
    /// Completed all chunks
    Complete,
}

/// Command emitted by the parser for the writer to execute
#[derive(Debug)]
pub enum WriteCommand {
    /// Seek to an absolute offset before next write
    Seek(u64),
    /// Write this data at current position
    Write(Vec<u8>),
    /// Fill with pattern for N bytes (pattern is 4 bytes, repeated)
    Fill { pattern: [u8; 4], bytes: u64 },
    /// Parsing complete, this is the expected final size
    Complete { expected_size: u64 },
}

impl SparseParser {
    pub fn new() -> Self {
        Self {
            header: None,
            chunks_processed: 0,
            current_output_offset: 0,
            state: ParserState::WaitingHeader,
            buffer: Vec::with_capacity(SPARSE_HEADER_SIZE),
            raw_remaining: 0,
        }
    }

    /// Get the header if parsed
    #[allow(dead_code)]
    pub fn header(&self) -> Option<&SparseHeader> {
        self.header.as_ref()
    }

    /// Check if parsing is complete
    #[allow(dead_code)]
    pub fn is_complete(&self) -> bool {
        self.state == ParserState::Complete
    }

    /// Process input data and emit write commands
    ///
    /// This is the main parsing function. It consumes input data and produces
    /// a series of WriteCommands that should be executed in order.
    ///
    /// Returns the commands and the number of input bytes consumed.
    pub fn process(&mut self, input: &[u8]) -> Result<(Vec<WriteCommand>, usize), SparseError> {
        let mut commands = Vec::new();
        let mut consumed = 0;
        let mut remaining = input;

        loop {
            if remaining.is_empty() {
                break;
            }

            match self.state {
                ParserState::WaitingHeader => {
                    let needed = SPARSE_HEADER_SIZE - self.buffer.len();
                    let take = needed.min(remaining.len());
                    self.buffer.extend_from_slice(&remaining[..take]);
                    remaining = &remaining[take..];
                    consumed += take;

                    if self.buffer.len() == SPARSE_HEADER_SIZE {
                        let header = parse_header(&self.buffer)?;
                        self.header = Some(header);
                        self.buffer.clear();
                        self.state = ParserState::WaitingChunkHeader;
                    }
                }

                ParserState::WaitingChunkHeader => {
                    // We need at least 12 bytes, but FILL chunks need 16
                    // Accumulate up to 16 bytes to handle all cases
                    let needed = 16 - self.buffer.len();
                    let take = needed.min(remaining.len());
                    self.buffer.extend_from_slice(&remaining[..take]);
                    remaining = &remaining[take..];
                    consumed += take;

                    // Try to parse if we have at least the minimum
                    if self.buffer.len() >= CHUNK_HEADER_SIZE {
                        let header = self.header.as_ref().unwrap();
                        let block_size = header.block_size;

                        match parse_chunk_header(&self.buffer, block_size) {
                            Ok((chunk, header_consumed)) => {
                                // Handle the chunk
                                match &chunk.chunk_type {
                                    ChunkType::Raw => {
                                        // Validate that chunk data size fits in u32
                                        if chunk.data_size > u32::MAX as u64 {
                                            return Err(SparseError::ChunkSizeOverflow {
                                                chunk_sz: chunk.output_blocks,
                                                block_size: header.block_size,
                                            });
                                        }
                                        self.raw_remaining = chunk.data_size as u32;

                                        // Handle any extra bytes in buffer - these are RAW data
                                        let extra = self.buffer.len() - header_consumed;
                                        if extra > 0 {
                                            let extra_bytes =
                                                self.buffer[header_consumed..].to_vec();
                                            let write_len =
                                                (extra_bytes.len() as u32).min(self.raw_remaining);
                                            if write_len > 0 {
                                                commands.push(WriteCommand::Write(
                                                    extra_bytes[..write_len as usize].to_vec(),
                                                ));
                                                self.current_output_offset += write_len as u64;
                                                self.raw_remaining -= write_len;
                                            }
                                        }
                                        self.buffer.clear();

                                        // Check if RAW chunk is complete (unlikely but possible for small chunks)
                                        if self.raw_remaining == 0 {
                                            self.chunks_processed += 1;
                                            let header = self.header.as_ref().unwrap();
                                            if self.chunks_processed == header.total_chunks {
                                                // Validate final output offset matches expected size
                                                let expected = header.image_size();
                                                if self.current_output_offset != expected {
                                                    return Err(SparseError::OutputSizeMismatch {
                                                        expected,
                                                        actual: self.current_output_offset,
                                                    });
                                                }
                                                self.state = ParserState::Complete;
                                                commands.push(WriteCommand::Complete {
                                                    expected_size: expected,
                                                });
                                            } else {
                                                self.state = ParserState::WaitingChunkHeader;
                                            }
                                        } else {
                                            self.state = ParserState::RawData;
                                        }
                                        continue; // Skip the extra bytes handling below
                                    }
                                    ChunkType::Fill(pattern) => {
                                        let bytes = chunk.output_size(block_size);
                                        commands.push(WriteCommand::Fill {
                                            pattern: *pattern,
                                            bytes,
                                        });
                                        self.current_output_offset += bytes;
                                        self.chunks_processed += 1;
                                    }
                                    ChunkType::DontCare => {
                                        let skip_bytes = chunk.output_size(block_size);
                                        self.current_output_offset += skip_bytes;
                                        // Emit seek command for next write
                                        commands
                                            .push(WriteCommand::Seek(self.current_output_offset));
                                        self.chunks_processed += 1;
                                    }
                                }

                                // Keep any extra bytes beyond the header (for non-RAW chunks)
                                self.buffer.drain(..header_consumed);

                                // Check for completion
                                if self.chunks_processed == header.total_chunks {
                                    // Validate final output offset matches expected size
                                    let expected = header.image_size();
                                    if self.current_output_offset != expected {
                                        return Err(SparseError::OutputSizeMismatch {
                                            expected,
                                            actual: self.current_output_offset,
                                        });
                                    }
                                    self.state = ParserState::Complete;
                                    commands.push(WriteCommand::Complete {
                                        expected_size: expected,
                                    });
                                } else if self.state != ParserState::RawData {
                                    self.state = ParserState::WaitingChunkHeader;
                                }
                            }
                            Err(SparseError::UnexpectedEof) => {
                                // Need more data
                                break;
                            }
                            Err(e) => return Err(e),
                        }
                    }
                }

                ParserState::RawData => {
                    let take = (self.raw_remaining as usize).min(remaining.len());
                    if take > 0 {
                        commands.push(WriteCommand::Write(remaining[..take].to_vec()));
                        self.current_output_offset += take as u64;
                        self.raw_remaining -= take as u32;
                        remaining = &remaining[take..];
                        consumed += take;
                    }

                    if self.raw_remaining == 0 {
                        self.chunks_processed += 1;
                        let header = self.header.as_ref().unwrap();
                        if self.chunks_processed == header.total_chunks {
                            // Validate final output offset matches expected size
                            let expected = header.image_size();
                            if self.current_output_offset != expected {
                                return Err(SparseError::OutputSizeMismatch {
                                    expected,
                                    actual: self.current_output_offset,
                                });
                            }
                            self.state = ParserState::Complete;
                            commands.push(WriteCommand::Complete {
                                expected_size: expected,
                            });
                        } else {
                            self.state = ParserState::WaitingChunkHeader;
                            self.buffer.clear();
                        }
                    }
                }

                ParserState::Complete => {
                    // Ignore any trailing data
                    break;
                }
            }
        }

        Ok((commands, consumed))
    }
}

impl Default for SparseParser {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_header(block_size: u32, total_blocks: u32, total_chunks: u32) -> Vec<u8> {
        let mut header = Vec::with_capacity(28);
        header.extend_from_slice(&SPARSE_HEADER_MAGIC.to_le_bytes());
        header.extend_from_slice(&1u16.to_le_bytes()); // major
        header.extend_from_slice(&0u16.to_le_bytes()); // minor
        header.extend_from_slice(&28u16.to_le_bytes()); // file_hdr_sz
        header.extend_from_slice(&12u16.to_le_bytes()); // chunk_hdr_sz
        header.extend_from_slice(&block_size.to_le_bytes());
        header.extend_from_slice(&total_blocks.to_le_bytes());
        header.extend_from_slice(&total_chunks.to_le_bytes());
        header.extend_from_slice(&0u32.to_le_bytes()); // checksum
        header
    }

    fn make_raw_chunk(blocks: u32, block_size: u32) -> Vec<u8> {
        let data_size = blocks * block_size;
        let total_size = 12 + data_size;
        let mut chunk = Vec::with_capacity(12);
        chunk.extend_from_slice(&CHUNK_TYPE_RAW.to_le_bytes());
        chunk.extend_from_slice(&0u16.to_le_bytes()); // reserved
        chunk.extend_from_slice(&blocks.to_le_bytes());
        chunk.extend_from_slice(&total_size.to_le_bytes());
        chunk
    }

    fn make_dont_care_chunk(blocks: u32) -> Vec<u8> {
        let mut chunk = Vec::with_capacity(12);
        chunk.extend_from_slice(&CHUNK_TYPE_DONT_CARE.to_le_bytes());
        chunk.extend_from_slice(&0u16.to_le_bytes()); // reserved
        chunk.extend_from_slice(&blocks.to_le_bytes());
        chunk.extend_from_slice(&12u32.to_le_bytes()); // total_sz
        chunk
    }

    #[test]
    fn test_parse_header() {
        let header_bytes = make_header(4096, 1000, 10);
        let header = parse_header(&header_bytes).unwrap();
        assert_eq!(header.block_size, 4096);
        assert_eq!(header.total_blocks, 1000);
        assert_eq!(header.total_chunks, 10);
    }

    #[test]
    fn test_invalid_magic() {
        let mut header_bytes = make_header(4096, 1000, 10);
        header_bytes[0] = 0xFF; // Corrupt magic
        assert!(matches!(
            parse_header(&header_bytes),
            Err(SparseError::InvalidMagic(_))
        ));
    }

    #[test]
    fn test_streaming_parser() {
        let block_size = 4096u32;
        let mut data = Vec::new();

        // Header: 1 RAW block + 2 DONT_CARE blocks = 3 total blocks, 2 chunks
        data.extend(make_header(block_size, 3, 2));

        // RAW chunk: 1 block
        data.extend(make_raw_chunk(1, block_size));
        data.extend(vec![0xAB; block_size as usize]); // data

        // DONT_CARE chunk: 2 blocks
        data.extend(make_dont_care_chunk(2));

        let mut parser = SparseParser::new();
        let (commands, consumed) = parser.process(&data).unwrap();

        assert_eq!(consumed, data.len());
        assert!(parser.is_complete());

        // Expected: Write(4096 bytes), Seek(3*4096), Complete
        assert!(commands.len() >= 3);
    }

    fn make_fill_chunk(blocks: u32, pattern: [u8; 4]) -> Vec<u8> {
        let mut chunk = Vec::with_capacity(16);
        chunk.extend_from_slice(&CHUNK_TYPE_FILL.to_le_bytes());
        chunk.extend_from_slice(&0u16.to_le_bytes()); // reserved
        chunk.extend_from_slice(&blocks.to_le_bytes());
        chunk.extend_from_slice(&16u32.to_le_bytes()); // total_sz
        chunk.extend_from_slice(&pattern); // fill pattern
        chunk
    }

    #[test]
    fn test_fill_chunk_parsing() {
        let block_size = 4096u32;
        let fill_pattern = [0xDE, 0xAD, 0xBE, 0xEF];
        let chunk_data = make_fill_chunk(2, fill_pattern);

        let (chunk, consumed) = parse_chunk_header(&chunk_data, block_size).unwrap();
        assert_eq!(consumed, 16);
        assert_eq!(chunk.output_blocks, 2);
        assert_eq!(chunk.data_size, 0); // FILL chunks have no additional data
        assert!(matches!(chunk.chunk_type, ChunkType::Fill(pat) if pat == fill_pattern));
    }

    #[test]
    fn test_streaming_parser_with_fill() {
        let block_size = 4096u32;
        let mut data = Vec::new();

        // Header: 2 FILL blocks, 1 chunk
        data.extend(make_header(block_size, 2, 1));

        // FILL chunk: 2 blocks with pattern
        let fill_pattern = [0x12, 0x34, 0x56, 0x78];
        data.extend(make_fill_chunk(2, fill_pattern));

        let mut parser = SparseParser::new();
        let (commands, consumed) = parser.process(&data).unwrap();

        assert_eq!(consumed, data.len());
        assert!(parser.is_complete());

        // Should have Fill command and Complete command
        let fill_found = commands.iter().any(|cmd| {
            matches!(cmd, WriteCommand::Fill { pattern, bytes } if pattern == &fill_pattern && *bytes == 2 * block_size as u64)
        });
        assert!(fill_found);
    }

    #[test]
    fn test_incremental_byte_feeding() {
        let block_size = 4096u32;
        let mut full_data = Vec::new();

        // Create a complete sparse image
        full_data.extend(make_header(block_size, 1, 1));
        full_data.extend(make_raw_chunk(1, block_size));
        full_data.extend(vec![0xCC; block_size as usize]);

        let mut parser = SparseParser::new();
        let mut total_commands = Vec::new();

        // Feed data byte by byte
        for byte in full_data {
            let (mut commands, _) = parser.process(&[byte]).unwrap();
            total_commands.append(&mut commands);
        }

        assert!(parser.is_complete());
        // Should have at least Write and Complete commands
        assert!(total_commands.len() >= 2);
    }

    #[test]
    fn test_overflow_detection() {
        let block_size = u32::MAX;
        let chunk_sz = 2u32;

        // This should cause overflow in chunk_sz * block_size
        let mut chunk_data = Vec::with_capacity(12);
        chunk_data.extend_from_slice(&CHUNK_TYPE_RAW.to_le_bytes());
        chunk_data.extend_from_slice(&0u16.to_le_bytes()); // reserved
        chunk_data.extend_from_slice(&chunk_sz.to_le_bytes());
        chunk_data.extend_from_slice(&u32::MAX.to_le_bytes()); // total_sz (will be invalid)

        let result = parse_chunk_header(&chunk_data, block_size);
        assert!(matches!(result, Err(SparseError::ChunkSizeOverflow { .. })));
    }

    #[test]
    fn test_large_but_valid_chunk_sizes() {
        let block_size = 65536u32; // 64KB blocks
        let chunk_sz = 65535u32; // Many blocks, but within u64 limits

        let mut chunk_data = Vec::with_capacity(12);
        chunk_data.extend_from_slice(&CHUNK_TYPE_RAW.to_le_bytes());
        chunk_data.extend_from_slice(&0u16.to_le_bytes()); // reserved
        chunk_data.extend_from_slice(&chunk_sz.to_le_bytes());

        // Calculate correct total size without overflow
        let data_size = chunk_sz as u64 * block_size as u64;
        let total_size = 12u64 + data_size;

        // This should be within bounds but test our checking logic
        if total_size <= u32::MAX as u64 {
            chunk_data.extend_from_slice(&(total_size as u32).to_le_bytes());

            let result = parse_chunk_header(&chunk_data, block_size);
            assert!(result.is_ok());
            let (chunk, _) = result.unwrap();
            assert_eq!(chunk.data_size, data_size);
        } else {
            // If the calculation would exceed u32::MAX for total_size,
            // we expect an overflow error
            chunk_data.extend_from_slice(&u32::MAX.to_le_bytes());

            let result = parse_chunk_header(&chunk_data, block_size);
            assert!(matches!(result, Err(SparseError::ChunkSizeOverflow { .. })));
        }
    }
}
