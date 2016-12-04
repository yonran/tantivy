use std::io::{self, Write};

pub struct CountingWriter<W> {
    underlying_writer: W,
    bytes_written: usize,
}

impl<W: Write> CountingWriter<W> {
    pub fn bytes_written(&self) -> usize {
        self.bytes_written
    }

    pub fn into_inner(self) -> W {
        self.underlying_writer
    }
}

impl<W: Write> From<W> for CountingWriter<W> {
    fn from(writer: W) -> CountingWriter<W> {
        CountingWriter {
            underlying_writer: writer,
            bytes_written: 0, 
        }
    }
}

impl<W: Write> Write for CountingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let extra_bytes_written = try!(self.underlying_writer.write(buf));
        self.bytes_written += extra_bytes_written;
        Ok(extra_bytes_written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.underlying_writer.flush()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use super::CountingWriter;

    #[test]
    fn test_counting_writer() {
        let buffer = Vec::new();
        let mut counting_writer = CountingWriter::from(buffer);
        let data = (0u8..10u8).collect::<Vec<_>>();
        counting_writer.write_all(&data[0..3]).unwrap();
        assert_eq!(counting_writer.bytes_written(), 3);
        counting_writer.write_all(&data[3..6]).unwrap();
        assert_eq!(counting_writer.bytes_written(), 6);
        counting_writer.flush().unwrap();
        let returned_buffer = counting_writer.into_inner();
        assert_eq!(returned_buffer.len(), 6);
    }
}