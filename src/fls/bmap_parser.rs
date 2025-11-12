/// BMAP (Block Map) file parser for efficient image flashing
/// BMAP files describe which blocks in an image contain data vs zeros
use std::io;

#[derive(Debug, Clone)]
pub struct BmapRange {
    pub start_block: u64,
    pub end_block: u64,   // inclusive
    pub checksum: Option<String>,
}

#[derive(Debug)]
pub struct BmapFile {
    pub image_size: u64,
    pub block_size: u64,
    pub blocks_count: u64,
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
        
        eprintln!("[BMAP] Image size: {} GB", image_size / (1024 * 1024 * 1024));
        eprintln!("[BMAP] Block size: {} bytes", block_size);
        eprintln!("[BMAP] Total blocks: {}", blocks_count);
        eprintln!("[BMAP] Mapped blocks: {} ({:.1}%)", 
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
                let cleaned: String = content.chars()
                    .filter(|c| !c.is_whitespace())
                    .collect();
                    
                if cleaned.is_empty() {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, 
                        format!("Tag <{}> is empty", tag)));
                }
                
                return cleaned.parse::<u64>().map_err(|e| {
                    eprintln!("[BMAP] Failed to parse {} from '{}' (cleaned: '{}')", tag, content, cleaned);
                    io::Error::new(io::ErrorKind::InvalidData, 
                        format!("Failed to parse {}: {} (value: '{}')", tag, e, cleaned))
                });
            }
        }
        
        Err(io::Error::new(io::ErrorKind::InvalidData, 
            format!("Tag <{}> not found", tag)))
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
                    if let Some(chksum_end) = range_xml[chksum_start + 8..].find("\"") {
                        Some(range_xml[chksum_start + 8..chksum_start + 8 + chksum_end].to_string())
                    } else {
                        None
                    }
                } else {
                    None
                };
                
                // Extract block range (between > and <)
                if let Some(content_start) = range_xml.find(">") {
                    if let Some(content_end) = range_xml[content_start..].find("</Range>") {
                        let content = range_xml[content_start + 1..content_start + content_end].trim();
                        
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
}

