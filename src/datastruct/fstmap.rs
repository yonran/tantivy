#![allow(should_implement_trait)]

use std::io;
use std::io::Write;
use fst;
use fst::raw::Fst;
use fst::map::{Stream, StreamBuilder};
use fst::{IntoStreamer, Streamer};
use directory::ReadOnlySource;
use common::BinarySerializable;
use std::marker::PhantomData;

fn convert_fst_error(e: fst::Error) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e)
}

pub struct FstMapBuilder<W: Write, V: BinarySerializable> {
    fst_builder: fst::MapBuilder<W>,
    data: Vec<u8>,
    _phantom_: PhantomData<V>,
}

impl<W: Write, V: BinarySerializable> FstMapBuilder<W, V> {
    
    pub fn new(w: W) -> io::Result<FstMapBuilder<W, V>> {
        let fst_builder = try!(fst::MapBuilder::new(w).map_err(convert_fst_error));
        Ok(FstMapBuilder {
            fst_builder: fst_builder,
            data: Vec::new(),
            _phantom_: PhantomData,
        })
    }

    /// Horribly unsafe, nobody should ever do that... except me :)
    /// 
    /// If used, it must be used by systematically alternating calls
    /// to insert_key and insert_value.
    ///
    /// TODO see if I can bend Rust typesystem to enforce that
    /// in a nice way.
    pub fn insert_key(&mut self, key: &[u8]) -> io::Result<()> {
        try!(self.fst_builder
            .insert(key, self.data.len() as u64)
            .map_err(convert_fst_error));
        Ok(())
    }

    /// Horribly unsafe, nobody should ever do that... except me :)
    pub fn insert_value(&mut self, value: &V) -> io::Result<()> {
        try!(value.serialize(&mut self.data));
        Ok(())
    }

    #[cfg(test)]
    pub fn insert(&mut self, key: &[u8], value: &V) -> io::Result<()> {
        try!(self.fst_builder
            .insert(key, self.data.len() as u64)
            .map_err(convert_fst_error));
        try!(value.serialize(&mut self.data));
        Ok(())
    }

    pub fn finish(self,) -> io::Result<W> {
        let mut file = try!(
            self.fst_builder
                 .into_inner()
                 .map_err(convert_fst_error));
        let footer_size = self.data.len() as u32;
        try!(file.write_all(&self.data));
        try!((footer_size as u32).serialize(&mut file));
        try!(file.flush());
        Ok(file)
    }
}

pub struct FstMap<V: BinarySerializable + Default> {
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


pub struct FstMapStreamer<'a, V: BinarySerializable + Default> {
    fst_stream: Stream<'a>,
    values_data: &'a [u8],
    _phantom_: PhantomData<V>,
    buffer: Vec<u8>,
    value: V,
}

impl<'a, V: BinarySerializable + Default> FstMapStreamer<'a, V> {

    pub fn next(&mut self) -> Option<(&[u8], &V)> {
        if let Some((key, addr)) = self.fst_stream.next() {
                let key_len = key.len();
                self.buffer.reserve(key_len);
                unsafe { self.buffer.set_len(key_len); }
                self.buffer[..key_len].copy_from_slice(key);
                let mut reader: &[u8] = &self.values_data[(addr as usize)..];
                self.value = V::deserialize(&mut reader).expect("Data corrupted");
                Some((&self.buffer[..], &self.value))
        }
        else {
            None
        }
    }

    pub fn key(&self) -> &[u8] {
        &self.buffer[..]
    }

    pub fn value(&self) -> &V {
        &self.value
    }
}

pub struct FstMapStreamBuilder<'a, V: BinarySerializable + Default> { 
    fst_stream_builder: StreamBuilder<'a>,
    values_data: &'a [u8],
    _phantom_: PhantomData<V>,
}

impl<'a, V: BinarySerializable + Default> FstMapStreamBuilder<'a, V> {
    pub fn ge<T: AsRef<[u8]>>(mut self, bound: T) -> Self {
        self.fst_stream_builder = self.fst_stream_builder.ge(bound);
        self
    }

    pub fn gt<T: AsRef<[u8]>>(mut self, bound: T) -> Self {
        self.fst_stream_builder = self.fst_stream_builder.gt(bound);
        self
    }

    pub fn le<T: AsRef<[u8]>>(mut self, bound: T) -> Self {
        self.fst_stream_builder = self.fst_stream_builder.le(bound);
        self
    }

    pub fn lt<T: AsRef<[u8]>>(mut self, bound: T) -> Self {
        self.fst_stream_builder = self.fst_stream_builder.lt(bound);
        self
    }

    pub fn into_stream(self) -> FstMapStreamer<'a, V> {
        FstMapStreamer {
            fst_stream: self.fst_stream_builder.into_stream(),
            values_data: self.values_data,
            _phantom_: self._phantom_,
            buffer: vec!(),
            value: V::default(),
        }
    }
}

impl<V: BinarySerializable + Default> FstMap<V> {

    pub fn range(&self) -> FstMapStreamBuilder<V> {
        FstMapStreamBuilder {
            fst_stream_builder: self.fst_index.range(),
            values_data: self.values_mmap.as_slice(),
            _phantom_: self._phantom_,
        }
    }

    pub fn keys(&self,) -> fst::map::Keys {
        self.fst_index.keys()
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

    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Option<V> {
        self.fst_index
            .get(key)
            .map(|offset| {
                let buffer = self.values_mmap.as_slice();
                let mut cursor = &buffer[(offset as usize)..];
                V::deserialize(&mut cursor).expect("Data in FST is corrupted")
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use directory::{RAMDirectory, Directory};
    use std::path::PathBuf;
    use fst::Streamer;

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
