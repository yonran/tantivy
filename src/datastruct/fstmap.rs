#![allow(should_implement_trait)]

use std::io;
use std::io::Write;
use fst;
use fst::raw::Fst;
use fst::Streamer;
use std::mem;
use common::CountingWriter;
use directory::ReadOnlySource;
use common::BinarySerializable;
use std::marker::PhantomData;

const CACHE_SIZE: usize = 2_000_000;
const EMPTY_ARRAY: [u8; 0] = [0u8; 0];

fn convert_fst_error(e: fst::Error) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e)
}

struct FstBlockBuilder {
    fst_builder: fst::MapBuilder<Vec<u8>>,
    first_key: Vec<u8>,
}

impl FstBlockBuilder {
    fn new(first_key: &[u8]) -> FstBlockBuilder {
        let buffer = Vec::with_capacity(CACHE_SIZE);
        FstBlockBuilder {
            fst_builder: fst::MapBuilder::new(buffer).unwrap(),
            first_key: first_key.to_vec(),
        }
    }

    fn insert(&mut self, key: &[u8], val: u64) {
        self.fst_builder.insert(key, val).unwrap()
    }

    fn bytes_written(&self) -> usize {
        self.fst_builder.bytes_written()
    }

    fn into_inner(self) -> (Vec<u8>, Vec<u8>) {
        let buffer = self.fst_builder.into_inner().unwrap();
        (self.first_key, buffer)
    }
    
    fn cut(&mut self, first_key: &[u8]) -> (Vec<u8>, Vec<u8>) {
        let mut fst_builder: FstBlockBuilder = FstBlockBuilder::new(first_key);
        mem::swap(self, &mut fst_builder);
        (fst_builder.first_key, fst_builder.fst_builder.into_inner().unwrap())
    }
}




pub struct FstMapBuilder<W: Write, V: BinarySerializable> {
    counting_writer: CountingWriter<W>, 
    block_stack: Vec<FstBlockBuilder>,
    _phantom_: PhantomData<V>,
}

impl<W: Write, V: BinarySerializable> FstMapBuilder<W, V> {
    
    pub fn new(write: W) -> io::Result<FstMapBuilder<W, V>> {
        let fst_block_builder = FstBlockBuilder::new(&EMPTY_ARRAY);
        Ok(FstMapBuilder {
            counting_writer: CountingWriter::from(write),
            block_stack: vec!(fst_block_builder),
            _phantom_: PhantomData,
        })
    }
    
    fn insert_key_addr(&mut self, layer_id: usize, key: &[u8], addr: u64) -> io::Result<()> {
        if layer_id >= self.block_stack.len() {
            // we need one extra layer.
            self.block_stack.push(FstBlockBuilder::new(key));
        }
        {
            let block = &mut self.block_stack[layer_id];
            if block.bytes_written() <= CACHE_SIZE {
                block.insert(key, addr);
                return Ok(());
            }
        }
        // we need to flush the current block.
        let (first_key, data) = self.block_stack[layer_id].cut(key);
        let new_addr = self.counting_writer.bytes_written() as u64;
        self.counting_writer.write_all(&data)?;
        self.block_stack[layer_id].insert(key, addr);
        return self.insert_key_addr(layer_id + 1, &first_key, new_addr);
    }
    
    pub fn insert(&mut self, key: &[u8], value: &V) -> io::Result<()>{
        let val_addr = self.counting_writer.bytes_written() as u64;
        value.serialize(&mut self.counting_writer)?;
        self.insert_key_addr(0, key, val_addr)
    }

    pub fn finish(mut self) -> io::Result<W> {
        let mut previous_block_key_offset: Option<(Vec<u8>, u64)> = None;
        for mut block in self.block_stack {
            if let Some((key, offset)) = previous_block_key_offset {
                block.insert(&key, offset);
            }
            let (first_key, data) = block.into_inner();
            previous_block_key_offset = Some((first_key, self.counting_writer.bytes_written() as u64));
            self.counting_writer.write_all(&data)?;
        }
        if let Some((_, first_fst_offset)) = previous_block_key_offset {
            first_fst_offset.serialize(&mut self.counting_writer);
        }
        Ok(self.counting_writer.into_inner())
    }
}

pub struct FstMap<V: BinarySerializable> {
    fst_index: fst::Map,
    values_mmap: ReadOnlySource,
    _phantom_: PhantomData<V>,
}


fn open_fst_index(source: ReadOnlySource) -> io::Result<fst::Map> {
    Ok(fst::Map::from(match source {
        ReadOnlySource::Anonymous(data) => try!(Fst::from_shared_bytes(data.data, data.start, data.len).map_err(convert_fst_error)),
        ReadOnlySource::Mmap(mmap_readonly) => try!(Fst::from_mmap(mmap_readonly).map_err(convert_fst_error)),
    }))
}

pub struct FstKeyIter<'a, V: 'static + BinarySerializable> {
    streamer: fst::map::Stream<'a>,
    __phantom__: PhantomData<V>
}

impl<'a, V: 'static + BinarySerializable> FstKeyIter<'a, V> {
    pub fn next(&mut self) -> Option<(&[u8])> {
        self.streamer
            .next()
            .map(|(k, _)| k)
    }
}


impl<V: BinarySerializable> FstMap<V> {

    pub fn keys(&self,) -> FstKeyIter<V> {
        FstKeyIter {
            streamer: self.fst_index.stream(),
            __phantom__: PhantomData,
        }
    }

    pub fn from_source(source: ReadOnlySource)  -> io::Result<FstMap<V>> {
        let total_len = source.len();
        let length_offset = total_len - 4;
        let mut split_len_buffer: &[u8] = &source.as_slice()[length_offset..];
        let footer_size = try!(u32::deserialize(&mut split_len_buffer)) as  usize;
        let split_len = length_offset - footer_size;
        let fst_source = source.slice(0, split_len);
        let values_source = source.slice(split_len, length_offset);
        let fst_index = try!(open_fst_index(fst_source));
        Ok(FstMap {
            fst_index: fst_index,
            values_mmap: values_source,
            _phantom_: PhantomData,
        })
    }

    fn read_value(&self, offset: u64) -> V {
        let buffer = self.values_mmap.as_slice();
        let mut cursor = &buffer[(offset as usize)..];
        V::deserialize(&mut cursor).expect("Data in FST is corrupted")
    }

    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Option<V> {
        self.fst_index
            .get(key)
            .map(|offset| self.read_value(offset))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use directory::{RAMDirectory, Directory};
    use std::path::PathBuf;

    #[test]
    fn test_fstmap() {
        let mut directory = RAMDirectory::create();
        let path = PathBuf::from("fstmap");
        {
            let write = directory.open_write(&path).unwrap();
            let mut fstmap_builder = FstMapBuilder::new(write).unwrap();
            fstmap_builder.insert("abc".as_bytes(), &34u32).unwrap();
            fstmap_builder.insert("abcd".as_bytes(), &346u32).unwrap();
            fstmap_builder.finish().unwrap();
        }
        let source = directory.open_read(&path).unwrap();
        let fstmap: FstMap<u32> = FstMap::from_source(source).unwrap();
        assert_eq!(fstmap.get("abc"), Some(34u32));
        assert_eq!(fstmap.get("abcd"), Some(346u32));
        let mut keys = fstmap.keys();
        assert_eq!(keys.next().unwrap(), "abc".as_bytes());
        assert_eq!(keys.next().unwrap(), "abcd".as_bytes());
        assert_eq!(keys.next(), None);
 
    }

}
