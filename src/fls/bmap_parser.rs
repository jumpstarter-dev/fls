/// BMAP (Block Map) file parser for efficient image flashing
/// BMAP files describe which blocks in an image contain data vs zeros
use std::io;

#[derive(Debug, Clone)]
pub struct BmapRange {
    pub start_block: u64,
    pub end_block: u64, // inclusive
    #[allow(dead_code)]
    pub checksum: Option<String>,
}

#[derive(Debug)]
pub struct BmapFile {
    #[allow(dead_code)]
    pub image_size: u64,
    pub block_size: u64,
    #[allow(dead_code)]
    pub blocks_count: u64,
    #[allow(dead_code)]
    pub mapped_blocks_count: u64,
    pub ranges: Vec<BmapRange>,
}

impl BmapFile {
    /// Parse a BMAP XML file
    pub fn parse(xml_content: &str) -> io::Result<Self> {
        // Strip XML comments first (<!-- ... -->)
        let xml_no_comments = Self::strip_xml_comments(xml_content);

        // Simple XML parsing - look for key elements
        let image_size = Self::parse_tag_u64(&xml_no_comments, "ImageSize")?;
        let block_size = Self::parse_tag_u64(&xml_no_comments, "BlockSize")?;
        let blocks_count = Self::parse_tag_u64(&xml_no_comments, "BlocksCount")?;
        let mapped_blocks_count = Self::parse_tag_u64(&xml_no_comments, "MappedBlocksCount")?;

        eprintln!(
            "[BMAP] Image size: {} GB",
            image_size / (1024 * 1024 * 1024)
        );
        eprintln!("[BMAP] Block size: {} bytes", block_size);
        eprintln!("[BMAP] Total blocks: {}", blocks_count);
        eprintln!(
            "[BMAP] Mapped blocks: {} ({:.1}%)",
            mapped_blocks_count,
            (mapped_blocks_count as f64 / blocks_count as f64) * 100.0
        );

        let ranges = Self::parse_ranges(&xml_no_comments)?;
        eprintln!("[BMAP] Parsed {} block ranges", ranges.len());

        Ok(Self {
            image_size,
            block_size,
            blocks_count,
            mapped_blocks_count,
            ranges,
        })
    }

    /// Check if a byte range is mapped (should be written)
    #[allow(dead_code)]
    pub fn is_range_mapped(&self, byte_offset: u64, size: u64) -> bool {
        let start_block = byte_offset / self.block_size;
        let end_block = (byte_offset + size - 1) / self.block_size;

        // Check if any part of this range overlaps with a mapped range
        for range in &self.ranges {
            if start_block <= range.end_block && end_block >= range.start_block {
                return true; // Overlaps with mapped range
            }
        }

        false // Not in any mapped range
    }

    /// Strip XML comments (<!-- ... -->)
    fn strip_xml_comments(xml: &str) -> String {
        let mut result = String::with_capacity(xml.len());
        let mut chars = xml.chars().peekable();

        while let Some(c) = chars.next() {
            if c == '<' {
                // Check if this is a comment start
                let mut peek_ahead = String::new();
                peek_ahead.push(c);

                // Peek next 3 chars to see if it's <!--
                for _ in 0..3 {
                    if let Some(&next_c) = chars.peek() {
                        peek_ahead.push(next_c);
                        chars.next();
                    }
                }

                if peek_ahead == "<!--" {
                    // Skip until we find -->
                    let mut comment_end = String::new();
                    for c in chars.by_ref() {
                        comment_end.push(c);
                        if comment_end.ends_with("-->") {
                            break;
                        }
                        if comment_end.len() > 2 {
                            comment_end.remove(0);
                        }
                    }
                } else {
                    // Not a comment, keep the characters
                    result.push_str(&peek_ahead);
                }
            } else {
                result.push(c);
            }
        }

        result
    }

    /// Parse a simple XML tag value as u64
    fn parse_tag_u64(xml: &str, tag: &str) -> io::Result<u64> {
        let start_tag = format!("<{}>", tag);
        let end_tag = format!("</{}>", tag);

        if let Some(start_pos) = xml.find(&start_tag) {
            if let Some(end_pos) = xml[start_pos..].find(&end_tag) {
                let content = &xml[start_pos + start_tag.len()..start_pos + end_pos];
                // More aggressive whitespace removal - strip all whitespace chars
                let cleaned: String = content.chars().filter(|c| !c.is_whitespace()).collect();

                if cleaned.is_empty() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Tag <{}> is empty", tag),
                    ));
                }

                return cleaned.parse::<u64>().map_err(|e| {
                    eprintln!(
                        "[BMAP] Failed to parse {} from '{}' (cleaned: '{}')",
                        tag, content, cleaned
                    );
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Failed to parse {}: {} (value: '{}')", tag, e, cleaned),
                    )
                });
            }
        }

        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Tag <{}> not found", tag),
        ))
    }

    /// Parse all <Range> elements
    fn parse_ranges(xml: &str) -> io::Result<Vec<BmapRange>> {
        let mut ranges = Vec::new();
        let mut search_start = 0;

        while let Some(range_start) = xml[search_start..].find("<Range") {
            let abs_start = search_start + range_start;

            if let Some(range_end) = xml[abs_start..].find("</Range>") {
                let range_xml = &xml[abs_start..abs_start + range_end + 8];

                // Extract checksum if present
                let checksum = if let Some(chksum_start) = range_xml.find("chksum=\"") {
                    range_xml[chksum_start + 8..].find("\"").map(|chksum_end| {
                        range_xml[chksum_start + 8..chksum_start + 8 + chksum_end].to_string()
                    })
                } else {
                    None
                };

                // Extract block range (between > and <)
                if let Some(content_start) = range_xml.find(">") {
                    if let Some(content_end) = range_xml[content_start..].find("</Range>") {
                        let content =
                            range_xml[content_start + 1..content_start + content_end].trim();

                        if let Some((start, end)) = Self::parse_range_content(content) {
                            ranges.push(BmapRange {
                                start_block: start,
                                end_block: end,
                                checksum,
                            });
                        }
                    }
                }

                search_start = abs_start + range_end + 8;
            } else {
                break;
            }
        }

        Ok(ranges)
    }

    /// Parse range content like "256-1805" or "0"
    fn parse_range_content(content: &str) -> Option<(u64, u64)> {
        if let Some(dash_pos) = content.find('-') {
            // Range like "256-1805"
            let start = content[..dash_pos].trim().parse::<u64>().ok()?;
            let end = content[dash_pos + 1..].trim().parse::<u64>().ok()?;
            Some((start, end))
        } else {
            // Single block like "0"
            let block = content.trim().parse::<u64>().ok()?;
            Some((block, block))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_range_mapped_cases(bmap: &BmapFile, cases: &[(u64, u64, bool, &str)]) {
        for (byte_offset, size, expected, description) in cases {
            let actual = bmap.is_range_mapped(*byte_offset, *size);
            assert_eq!(
                actual, *expected,
                "Failed for {}: byte_offset={}, size={}, expected={}, actual={}",
                description, byte_offset, size, expected, actual
            );
        }
    }

    #[test]
    fn test_parse_simple_bmap() {
        let xml = r#"
        <?xml version="1.0" ?>
        <bmap version="2.0">
            <ImageSize> 8589934592 </ImageSize>
            <BlockSize> 4096 </BlockSize>
            <BlocksCount> 2097152 </BlocksCount>
            <MappedBlocksCount> 312199 </MappedBlocksCount>
            <BlockMap>
                <Range chksum="abc"> 0 </Range>
                <Range chksum="def"> 256-1805 </Range>
            </BlockMap>
        </bmap>
        "#;

        let bmap = BmapFile::parse(xml).unwrap();
        assert_eq!(bmap.image_size, 8589934592);
        assert_eq!(bmap.block_size, 4096);
        assert_eq!(bmap.ranges.len(), 2);
        assert_eq!(bmap.ranges[0].start_block, 0);
        assert_eq!(bmap.ranges[0].end_block, 0);
        assert_eq!(bmap.ranges[1].start_block, 256);
        assert_eq!(bmap.ranges[1].end_block, 1805);
    }

    #[test]
    fn test_is_range_mapped_edge_cases() {
        let block_size = 4096;
        let bmap = BmapFile {
            image_size: 1000000,
            block_size,
            blocks_count: 1000,
            mapped_blocks_count: 12,
            ranges: vec![
                BmapRange {
                    start_block: 10,
                    end_block: 15,
                    checksum: None,
                },
                BmapRange {
                    start_block: 20,
                    end_block: 25,
                    checksum: None,
                },
            ],
        };

        let test_cases = vec![
            (10 * block_size, 1, true, "single byte at start of range 1"),
            (
                10 * block_size,
                block_size,
                true,
                "full block at start of range 1",
            ),
            (15 * block_size, 1, true, "single byte at end of range 1"),
            (
                15 * block_size,
                block_size,
                true,
                "full block at end of range 1",
            ),
            (
                14 * block_size + 1,
                block_size,
                true,
                "query ending at last byte of block 15",
            ),
            (
                15 * block_size,
                block_size + 1,
                true,
                "query spanning from range 1 into gap",
            ),
            (
                15 * block_size + block_size / 2,
                block_size,
                true,
                "query starting mid-block 15, ending in gap",
            ),
            (
                16 * block_size,
                block_size,
                false,
                "single block in gap (block 16)",
            ),
            (
                17 * block_size,
                block_size,
                false,
                "single block in gap (block 17)",
            ),
            (
                18 * block_size,
                block_size,
                false,
                "single block in gap (block 18)",
            ),
            (
                19 * block_size,
                block_size,
                false,
                "single block in gap (block 19)",
            ),
            (
                16 * block_size,
                4 * block_size,
                false,
                "multi-block query in gap",
            ),
            (0, block_size, false, "query before first range (block 0)"),
            (
                5 * block_size,
                block_size,
                false,
                "query before first range (block 5)",
            ),
            (
                0,
                5 * block_size,
                false,
                "multi-block query before first range",
            ),
            (
                30 * block_size,
                block_size,
                false,
                "query after last range (block 30)",
            ),
            (
                35 * block_size,
                block_size,
                false,
                "query after last range (block 35)",
            ),
            (
                30 * block_size,
                5 * block_size,
                false,
                "multi-block query after last range",
            ),
            (
                5 * block_size,
                25 * block_size,
                true,
                "query spanning all ranges and gaps",
            ),
            (
                9 * block_size,
                3 * block_size,
                true,
                "partial overlap: starts before range 1, ends in range 1",
            ),
            (
                9 * block_size + block_size / 2,
                2 * block_size,
                true,
                "partial overlap: starts mid-block 9, ends in range 1",
            ),
            (
                14 * block_size,
                3 * block_size,
                true,
                "partial overlap: starts in range 1, ends in gap",
            ),
            (
                14 * block_size + block_size / 2,
                2 * block_size,
                true,
                "partial overlap: starts mid-block 14, ends in gap",
            ),
            (
                12 * block_size,
                block_size,
                true,
                "single full block in mapped range (block 12)",
            ),
            (
                12 * block_size,
                1,
                true,
                "single byte in mapped range (block 12)",
            ),
            (
                12 * block_size + 100,
                block_size - 100,
                true,
                "partial block in mapped range (block 12)",
            ),
            (
                17 * block_size,
                block_size,
                false,
                "single full block in gap (block 17)",
            ),
            (17 * block_size, 1, false, "single byte in gap (block 17)"),
            (
                17 * block_size + 100,
                block_size - 100,
                false,
                "partial block in gap (block 17)",
            ),
            (
                12 * block_size,
                10 * block_size,
                true,
                "query spanning both ranges and gap",
            ),
            (
                19 * block_size,
                block_size + 1,
                true,
                "query spanning from gap into range 2",
            ),
            (
                19 * block_size + block_size / 2,
                block_size,
                true,
                "query starting mid-block 19, ending in range 2",
            ),
            (25 * block_size, 1, true, "single byte at end of range 2"),
            (
                25 * block_size,
                block_size,
                true,
                "full block at end of range 2",
            ),
            (
                24 * block_size + 1,
                block_size,
                true,
                "query ending at last byte of block 25",
            ),
            (
                10 * block_size - 1,
                1,
                false,
                "query ending exactly at last byte of block 9 (unmapped)",
            ),
            (
                10 * block_size - 1,
                2,
                true,
                "query spanning from block 9 into block 10 (mapped)",
            ),
            (
                10 * block_size - 100,
                200,
                true,
                "query starting before block 10, ending in block 10",
            ),
            (
                9 * block_size + 2000,
                block_size + 1000,
                true,
                "query starting in block 9, ending in block 11",
            ),
            (
                15 * block_size + 2000,
                block_size + 1000,
                true,
                "query starting in block 15, ending in block 17",
            ),
            (10 * block_size, 1, true, "1 byte at start of range 1"),
            (15 * block_size, 1, true, "1 byte at end of range 1"),
            (20 * block_size, 1, true, "1 byte at start of range 2"),
            (25 * block_size, 1, true, "1 byte at end of range 2"),
            (16 * block_size, 1, false, "1 byte in gap"),
            (0, 1, false, "1 byte before first range"),
            (30 * block_size, 1, false, "1 byte after last range"),
        ];

        test_range_mapped_cases(&bmap, &test_cases);
    }
}
