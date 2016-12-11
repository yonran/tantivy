#![allow(should_implement_trait)]

use std::io;
use std::io::Write;
use fst;
use fst::raw::Fst;
use fst::Streamer;
use fst::IntoStreamer;
use std::mem;
use common::CountingWriter;
use directory::ReadOnlySource;
use common::BinarySerializable;
use std::marker::PhantomData;


const CACHE_SIZE: usize = 2_000_000;

fn convert_fst_error(e: fst::Error) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e)
}

struct FstBlockBuilder {
    fst_builder: fst::MapBuilder<Vec<u8>>,
}

impl FstBlockBuilder {
    fn new() -> FstBlockBuilder {
        let buffer = Vec::with_capacity(CACHE_SIZE);
        FstBlockBuilder {
            fst_builder: fst::MapBuilder::new(buffer).unwrap(),
        }
    }

    fn insert(&mut self, key: &[u8], val: u64) {
        self.fst_builder.insert(key, val).unwrap()
    }

    fn bytes_written(&self) -> usize {
        self.fst_builder.bytes_written()
    }

    fn into_inner(self) -> Vec<u8> {
        self.fst_builder.into_inner().unwrap()
    }
    
    fn cut(&mut self) -> Vec<u8> {
        let mut fst_builder: FstBlockBuilder = FstBlockBuilder::new();
        mem::swap(self, &mut fst_builder);
        fst_builder.fst_builder.into_inner().unwrap()
    }
}




pub struct FstMapBuilder<W: Write, V: BinarySerializable> {
    counting_writer: CountingWriter<W>, 
    block_stack: Vec<FstBlockBuilder>,
    last_key: Vec<u8>,
    _phantom_: PhantomData<V>,
}

impl<W: Write, V: BinarySerializable> FstMapBuilder<W, V> {
    
    pub fn new(write: W) -> io::Result<FstMapBuilder<W, V>> {
        let fst_block_builder = FstBlockBuilder::new();
        Ok(FstMapBuilder {
            counting_writer: CountingWriter::from(write),
            block_stack: vec!(fst_block_builder),
            last_key: Vec::new(),
            _phantom_: PhantomData,
        })
    }
    
    fn insert_key_addr(&mut self, layer_id: usize, key: &[u8], addr: u64) -> io::Result<()> {
        if layer_id >= self.block_stack.len() {
            // we need one extra layer.
            self.block_stack.push(FstBlockBuilder::new());
        }
        {
            let block = &mut self.block_stack[layer_id];
            if block.bytes_written() <= CACHE_SIZE {
                block.insert(key, addr);
                return Ok(());
            }
        }
        // we need to flush the current block.
        let data = self.block_stack[layer_id].cut();
        let new_addr = self.counting_writer.bytes_written() as u64;
        (data.len() as u32).serialize(&mut self.counting_writer)?;
        self.counting_writer.write_all(&data)?;
        self.block_stack[layer_id].insert(key, addr);
        return self.insert_key_addr(layer_id + 1, key, new_addr);
    }
    
    pub fn insert(&mut self, key: &[u8], value: &V) -> io::Result<()>{
        let val_addr = self.counting_writer.bytes_written() as u64;
        value.serialize(&mut self.counting_writer)?;
        self.insert_key_addr(0, key, val_addr)?;
        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        Ok(())
    }

    pub fn finish(mut self) -> io::Result<W> {
        let mut previous_block_key_offset: u64 = 0;
        let block_depth = self.block_stack.len();
        for mut block in self.block_stack {
            if previous_block_key_offset != 0 {
                block.insert(&self.last_key, previous_block_key_offset);
            }
            let data = block.into_inner();
            previous_block_key_offset = self.counting_writer.bytes_written() as u64;
            (data.len() as u32).serialize(&mut self.counting_writer)?;
            self.counting_writer.write_all(&data)?;
        }
        previous_block_key_offset.serialize(&mut self.counting_writer)?;
        (block_depth as u8).serialize(&mut self.counting_writer)?;
        let mut writer = self.counting_writer.into_inner();
        writer.flush()?;
        Ok(writer)
    }
}

pub struct FstMap<V: BinarySerializable> {
    depth: u8,
    source: ReadOnlySource,
    top_fst_offset: usize,
    _phantom_: PhantomData<V>,
}


fn open_fst_index(source: ReadOnlySource) -> io::Result<fst::Map> {
    Ok(fst::Map::from(match source {
        ReadOnlySource::Anonymous(data) => try!(Fst::from_shared_bytes(data.data, data.start, data.len).map_err(convert_fst_error)),
        ReadOnlySource::Mmap(mmap_readonly) => try!(Fst::from_mmap(mmap_readonly).map_err(convert_fst_error)),
    }))
}

pub struct CascadeStreamer {
    source: ReadOnlySource,
    parent: Option<Box<CascadeStreamer>>,
    fst: Option<Box<fst::Map>>,
    inner_streamer: Option<fst::map::Stream<'static>>,
    
    key: Vec<u8>,
    value: u64, 
}

impl CascadeStreamer {

    fn load_fst(&mut self, offset: usize) {
        println!("offset {}", offset);
        println!("data len {}", self.source.len());
        self.inner_streamer = None;
        self.fst = Some(box read_fst(&self.source, offset as usize).unwrap());
        let inner_streamer: fst::map::Stream<'static> = unsafe { mem::transmute(self.fst.as_ref().unwrap().stream()) };
        self.inner_streamer = Some(inner_streamer);
    }

    fn new(source: &ReadOnlySource, offset: usize, parent: Option<Box<CascadeStreamer>>) -> CascadeStreamer {
        let fst = read_fst(source, offset as usize).unwrap();
        let mut streamer = CascadeStreamer {
            source: source.clone(),
            fst: None,
            inner_streamer: None,
            parent: parent,
            key: Vec::new(),
            value: 0,
        };
        streamer.load_fst(offset);
        streamer
    }

    fn load_next(&mut self,) -> bool {
        println!("aaa");
        match self.inner_streamer.as_mut().unwrap().next() {
            Some((key, value)) => {
                self.key.clear();
                self.key.extend_from_slice(key);
                self.value = value;
                true
            }
            None => {
                false
            }
        }
    }

    fn read_with_depth(
        source: &ReadOnlySource,
        offset: usize,
        depth: u8) -> CascadeStreamer {
        // TODO depth == 0? 
        if depth == 1u8 {
            CascadeStreamer::new(source, offset, None)
        }
        else {
            let mut parent = CascadeStreamer::read_with_depth(source, offset, depth - 1);
            // TODO empty?
            let (_, offset) = parent.next().unwrap();
            CascadeStreamer::new(source, offset as usize, Some(box parent))
        }
    }
}



impl<'a> Streamer<'a> for CascadeStreamer {
    type Item = (&'a [u8], u64);

    /// Emits the next element in this stream, or `None` to indicate the stream
    /// has been exhausted.
    ///
    /// It is not specified what a stream does after `None` is emitted. In most
    /// cases, `None` should be emitted on every subsequent call.
    fn next(&'a mut self) -> Option<Self::Item> {
        // if we still have items in the current iterator,
        // just return that
        if self.load_next() {
            return Some((&self.key, self.value));
        }
        else {
            if self.parent.is_none() {
                return None;
            }
            if let Some(new_offset) = self.parent.as_mut().unwrap().next().map(|(_, offset)| offset) {
                self.load_fst(new_offset as usize);
                if self.load_next() {
                    return Some((&self.key, self.value));
                }
                else {
                    return None
                }
            }
            else {
                None
            }    
        }
        
    }
}



fn read_fst(source: &ReadOnlySource, start_offset: usize) -> io::Result<fst::Map> {
    let len_data = source.slice(start_offset, start_offset + 4);
    let mut cursor_len = len_data.as_slice();
    let fst_len = u32::deserialize(&mut cursor_len)? as usize;
    let fst_start = start_offset + 4;
    let fst_stop = fst_start + fst_len;
    let fst_data = source.slice(fst_start, fst_stop);
    open_fst_index(fst_data)
}



impl<V: BinarySerializable> FstMap<V> {

    pub fn iter_kvs(&self,) -> CascadeStreamer {
        CascadeStreamer::read_with_depth(
            &self.source,
            self.top_fst_offset,
            self.depth)
    }

    pub fn from_source(source: ReadOnlySource)  -> io::Result<FstMap<V>> {
        let total_len = source.len();
        if total_len < 9 {
            return Ok(FstMap {
                source: ReadOnlySource::empty(),
                depth: 0,
                top_fst_offset: 0,
                _phantom_: PhantomData,
            })
        }
        let footer_start = total_len - 9;
        let footer = source.slice(footer_start, total_len);
        let depth: u8 = footer.as_slice()[8];
        let mut footer_data = footer.as_slice();
        let top_fst_offset = u64::deserialize(&mut footer_data)? as usize;
        Ok(FstMap {
            source: source,
            depth: depth,
            top_fst_offset: top_fst_offset,
            _phantom_: PhantomData,
        })
    }

    fn read_value(&self, offset: u64) -> V {
        let buffer = self.source.as_slice();
        let mut cursor = &buffer[(offset as usize)..];
        V::deserialize(&mut cursor).expect("Data in FST is corrupted")
    }

    fn read_fst(&self, start_offset: usize) -> io::Result<fst::Map> {
        let len_data = self.source.slice(start_offset, start_offset + 4);
        let mut cursor_len = len_data.as_slice();
        let fst_len = u32::deserialize(&mut cursor_len)? as usize;
        let fst_start = start_offset + 4;
        let fst_stop = fst_start + fst_len;
        let fst_data = self.source.slice(fst_start, fst_stop);
        open_fst_index(fst_data)
    }

    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Option<V> {
        if self.depth == 0 {
            return None
        }
        let mut offset = self.top_fst_offset;
        for _ in 0..self.depth - 1 {
            let fst = read_fst(&self.source, offset).unwrap();
            if let Some((_, new_offset)) = fst
                    .range()
                    .ge(key.as_ref())
                    .into_stream()
                    .next() {
                offset = new_offset as usize;
            }
            else {
                return None;
            }
        }
        read_fst(&self.source, offset).unwrap()
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
        let mut kv_it = fstmap.iter_kvs();
        {
            let (k, _) = kv_it.next().unwrap();
            assert_eq!(k, "abc".as_bytes());
        }
        {
            let (k, _) = kv_it.next().unwrap();
            assert_eq!(k, "abcd".as_bytes());
        }
        assert_eq!(kv_it.next(), None);
    }
}
