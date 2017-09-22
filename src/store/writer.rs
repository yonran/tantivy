use directory::WritePtr;
use DocId;
use schema::Document;
use bincode;
use common::BinarySerializable;
use std::io::{self, Write};
use lz4;
use datastruct::SkipListBuilder;
use common::CountingWriter;

const BLOCK_SIZE: usize = 16_384;

fn make_io_error(err: bincode::Error) -> io::Error {
    io::Error::new(io::ErrorKind::Other, err)
}

/// Write tantivy's [`Store`](./index.html)
///
/// Contrary to the other components of `tantivy`,
/// the store is written to disc as document as being added,
/// as opposed to when the segment is getting finalized.
///
/// The skip list index on the other hand, is build in memory.
///
pub struct StoreWriter {
    doc: DocId,
    offset_index_writer: SkipListBuilder<u64>,
    writer: CountingWriter<WritePtr>,
    intermediary_buffer: Vec<u8>,
    current_block: Vec<u8>,
}

impl StoreWriter {
    /// Create a store writer.
    ///
    /// The store writer will writes blocks on disc as
    /// document are added.
    pub fn new(writer: WritePtr) -> StoreWriter {
        StoreWriter {
            doc: 0,
            offset_index_writer: SkipListBuilder::new(3),
            writer: CountingWriter::wrap(writer),
            intermediary_buffer: Vec::new(),
            current_block: Vec::new(),
        }
    }

    /// Store a new document.
    ///
    /// The document id is implicitely the number of times
    /// this method has been called.
    ///
    pub fn store<'a>(&mut self, stored_document: &Document) -> io::Result<()> {
        self.intermediary_buffer.clear();
        bincode::serialize_into(&mut self.intermediary_buffer, stored_document, bincode::Infinite)
            .map_err(make_io_error)?;
        let doc_num_bytes = self.intermediary_buffer.len() as u32;
        <u32 as BinarySerializable>::serialize(&doc_num_bytes, &mut self.current_block)?;
        self.current_block.write_all(&self.intermediary_buffer[..])?;
        self.doc += 1;
        if self.current_block.len() > BLOCK_SIZE {
            self.write_and_compress_block()?;
        }
        Ok(())
    }

    fn write_and_compress_block(&mut self) -> io::Result<()> {
        self.intermediary_buffer.clear();
        {
            let mut encoder = lz4::EncoderBuilder::new()
                .build(&mut self.intermediary_buffer)?;
            encoder.write_all(&self.current_block)?;
            let (_, encoder_result) = encoder.finish();
            encoder_result?;
        }
        let num_bytes: u32 = self.intermediary_buffer.len() as u32;
        <u32 as BinarySerializable>::serialize(&num_bytes, &mut self.writer)?;
        self.writer.write_all(&self.intermediary_buffer)?;
        self.offset_index_writer.insert(
            self.doc,
            &(self.writer.written_bytes() as
                u64),
        )?;
        self.current_block.clear();
        Ok(())
    }


    /// Finalized the store writer.
    ///
    /// Compress the last unfinished block if any,
    /// and serializes the skip list index on disc.
    pub fn close(mut self) -> io::Result<()> {
        if !self.current_block.is_empty() {
            self.write_and_compress_block()?;
        }
        let header_offset: u64 = self.writer.written_bytes() as u64;
        self.offset_index_writer.write(&mut self.writer)?;
        <u64 as BinarySerializable>::serialize(&header_offset, &mut self.writer)?;
        <u32 as BinarySerializable>::serialize(&self.doc, &mut self.writer)?;
        self.writer.flush()
    }
}
