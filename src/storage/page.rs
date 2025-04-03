use std::io::{Cursor, Read, Write};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

/// Represents a page of data in the database.
/// A page is a fixed-size block of bytes that can store various data types.
pub struct Page {
    buffer: Vec<u8>,
}

impl Page {
    /// Creates a new empty page with the specified block size.
    pub fn new(blocksize: usize) -> Self {
        Page {
            buffer: vec![0; blocksize],
        }
    }

    /// Creates a page from an existing byte array.
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Page { buffer: bytes }
    }

    /// Gets an integer value from the specified offset in the page.
    pub fn get_int(&self, offset: usize) -> i32 {
        let mut cursor = Cursor::new(&self.buffer[offset..offset + 4]);
        cursor.read_i32::<BigEndian>().unwrap()
    }

    /// Sets an integer value at the specified offset in the page.
    pub fn set_int(&mut self, offset: usize, n: i32) {
        let mut cursor = Cursor::new(&mut self.buffer[offset..offset + 4]);
        cursor.write_i32::<BigEndian>(n).unwrap();
    }

    /// Gets a byte array from the specified offset in the page.
    /// The first 4 bytes at the offset specify the length of the array.
    pub fn get_bytes(&self, offset: usize) -> Vec<u8> {
        let length = self.get_int(offset) as usize;
        let start = offset + 4;
        let end = start + length;
        self.buffer[start..end].to_vec()
    }

    /// Sets a byte array at the specified offset in the page.
    /// The length of the array is stored as an integer before the actual bytes.
    pub fn set_bytes(&mut self, offset: usize, bytes: &[u8]) {
        self.set_int(offset, bytes.len() as i32);
        let start = offset + 4;
        let end = start + bytes.len();
        self.buffer[start..end].copy_from_slice(bytes);
    }

    /// Gets a string from the specified offset in the page.
    pub fn get_string(&self, offset: usize) -> String {
        let bytes = self.get_bytes(offset);
        String::from_utf8_lossy(&bytes).to_string()
    }

    /// Sets a string at the specified offset in the page.
    pub fn set_string(&mut self, offset: usize, s: &str) {
        self.set_bytes(offset, s.as_bytes());
    }

    /// Returns a reference to the underlying buffer.
    pub fn contents(&self) -> &[u8] {
        &self.buffer
    }
    
    /// Returns a mutable reference to the underlying buffer.
    pub fn contents_mut(&mut self) -> &mut [u8] {
        &mut self.buffer
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_page() {
        let page = Page::new(100);
        assert_eq!(page.buffer.len(), 100);
        assert!(page.buffer.iter().all(|&b| b == 0));
    }

    #[test]
    fn test_from_bytes() {
        let data = vec![1, 2, 3, 4, 5];
        let page = Page::from_bytes(data.clone());
        assert_eq!(page.buffer, data);
    }

    #[test]
    fn test_get_set_int() {
        let mut page = Page::new(100);
        
        // Test setting and getting at offset 0
        page.set_int(0, 42);
        assert_eq!(page.get_int(0), 42);
        
        // Test setting and getting at different offsets
        page.set_int(4, -123);
        assert_eq!(page.get_int(4), -123);
        
        // Test max and min values
        page.set_int(8, i32::MAX);
        assert_eq!(page.get_int(8), i32::MAX);
        
        page.set_int(12, i32::MIN);
        assert_eq!(page.get_int(12), i32::MIN);
    }

    #[test]
    fn test_get_set_bytes() {
        let mut page = Page::new(100);
        let test_data = vec![10, 20, 30, 40, 50];
        
        // Test setting and getting bytes
        page.set_bytes(0, &test_data);
        let retrieved = page.get_bytes(0);
        assert_eq!(retrieved, test_data);
        
        // Test with empty array
        let empty: Vec<u8> = vec![];
        page.set_bytes(20, &empty);
        assert_eq!(page.get_bytes(20), empty);
        
        // Test with larger data
        let large_data: Vec<u8> = (0..50).collect();
        page.set_bytes(30, &large_data);
        assert_eq!(page.get_bytes(30), large_data);
    }

    #[test]
    fn test_get_set_string() {
        let mut page = Page::new(100);
        
        let test_str = "Hello, world!";
        page.set_string(0, test_str);
        assert_eq!(page.get_string(0), test_str);
        
        page.set_string(20, "");
        assert_eq!(page.get_string(20), "");
        
        let special = "Special chars: !@#$%^&*()_+";
        page.set_string(30, special);
        assert_eq!(page.get_string(30), special);

        let unicode = "Unicode: 你好, こんにちは, Привет";
        page.set_string(50, unicode);
        assert_eq!(page.get_string(50), unicode);
    }

    #[test]
    fn test_contents() {
        let mut page = Page::new(10);
        
        assert_eq!(page.contents().len(), 10);
        
        page.contents_mut()[0] = 42;
        assert_eq!(page.buffer[0], 42);
    }

    #[test]
    fn test_complex_scenario() {
        let mut page = Page::new(1000);
        
        page.set_int(0, 12345);
        page.set_string(4, "This is a test string");
        page.set_bytes(100, &[1, 2, 3, 4, 5]);
        page.set_int(200, -98765);
        
        assert_eq!(page.get_int(0), 12345);
        assert_eq!(page.get_string(4), "This is a test string");
        assert_eq!(page.get_bytes(100), vec![1, 2, 3, 4, 5]);
        assert_eq!(page.get_int(200), -98765);
    }
} 