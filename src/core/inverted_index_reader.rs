use directory::{ReadOnlySource, SourceRead};
use termdict::{TermDictionary, TermDictionaryImpl};
use postings::{BlockSegmentPostings, SegmentPostings};
use postings::TermInfo;
use schema::IndexRecordOption;
use schema::Term;
use fastfield::DeleteBitSet;
use compression::CompressedIntStream;
use postings::FreqReadingOption;
use common::BinarySerializable;

/// The inverted index reader is in charge of accessing
/// the inverted index associated to a specific field.
///
/// # Note
///
/// It is safe to delete the segment associated to
/// an `InvertedIndexReader`. As long as it is open,
/// the `ReadOnlySource` it is relying on should
/// stay available.
///
///
/// `InvertedIndexReader` are created by calling
/// the `SegmentReader`'s [`.inverted_index(...)`] method
pub struct InvertedIndexReader {
    termdict: TermDictionaryImpl,
    postings_source: ReadOnlySource,
    positions_source: ReadOnlySource,
    delete_bitset: DeleteBitSet,
    record_option: IndexRecordOption,
}

impl InvertedIndexReader {
    pub(crate) fn new(
        termdict_source: ReadOnlySource,
        postings_source: ReadOnlySource,
        positions_source: ReadOnlySource,
        delete_bitset: DeleteBitSet,
        record_option: IndexRecordOption,
    ) -> InvertedIndexReader {
        InvertedIndexReader {
            termdict: TermDictionaryImpl::from_source(termdict_source),
            postings_source,
            positions_source,
            delete_bitset,
            record_option,
        }
    }

    /// Returns the term info associated with the term.
    pub fn get_term_info(&self, term: &Term) -> Option<TermInfo> {
        self.termdict.get(term.value_bytes())
    }

    /// Return the term dictionary datastructure.
    pub fn terms(&self) -> &TermDictionaryImpl {
        &self.termdict
    }

    /// Resets the block segment to another position of the postings
    /// file.
    ///
    /// This is useful for enumerating through a list of terms,
    /// and consuming the associated posting lists while avoiding
    /// reallocating a `BlockSegmentPostings`.
    ///
    /// # Warning
    ///
    /// This does not reset the positions list.
    pub fn reset_block_postings_from_terminfo(
        &self,
        term_info: &TermInfo,
        block_postings: &mut BlockSegmentPostings,
    ) {
        let offset = term_info.postings_offset as usize;
        let end_source = self.postings_source.len();
        let postings_slice = self.postings_source.slice(offset, end_source);
        let postings_reader = SourceRead::from(postings_slice);
        block_postings.reset(term_info.doc_freq as usize, postings_reader);
    }

    /// Returns a block postings given a `term_info`.
    /// This method is for an advanced usage only.
    ///
    /// Most user should prefer using `read_postings` instead.
    pub fn read_block_postings_from_terminfo(
        &self,
        term_info: &TermInfo,
        requested_option: IndexRecordOption,
    ) -> BlockSegmentPostings {
        let offset = term_info.postings_offset as usize;
        let postings_data = self.postings_source.slice_from(offset);
        let freq_reading_option = match (self.record_option, requested_option) {
            (IndexRecordOption::Basic, _) => FreqReadingOption::NoFreq,
            (_, IndexRecordOption::Basic) => FreqReadingOption::SkipFreq,
            (_, _) => FreqReadingOption::ReadFreq,
        };
        BlockSegmentPostings::from_data(
            term_info.doc_freq as usize,
            SourceRead::from(postings_data),
            freq_reading_option,
        )
    }

    /// Returns a posting object given a `term_info`.
    /// This method is for an advanced usage only.
    ///
    /// Most user should prefer using `read_postings` instead.
    pub fn read_postings_from_terminfo(
        &self,
        term_info: &TermInfo,
        option: IndexRecordOption,
    ) -> SegmentPostings {
        let block_postings = self.read_block_postings_from_terminfo(term_info, option);
        let delete_bitset = self.delete_bitset.clone();
        let position_stream = {
            if option.has_positions() {
                let position_offset = term_info.positions_offset;
                let positions_source = self.positions_source.slice_from(position_offset as usize);
                let mut stream = CompressedIntStream::wrap(positions_source);
                stream.skip(term_info.positions_inner_offset as usize);
                Some(stream)
            } else {
                None
            }
        };
        SegmentPostings::from_block_postings(block_postings, delete_bitset, position_stream)
    }

    /// Returns the total number of tokens recorded for all documents
    /// (including deleted documents).
    pub fn total_num_tokens(&self) -> u64 {
        let total_num_tokens_data = self.postings_source.slice(0, 8);
        let mut total_num_tokens_cursor = total_num_tokens_data.as_slice();
        let result = u64::deserialize(&mut total_num_tokens_cursor).unwrap_or(0u64);
        result
    }



    /// Returns the segment postings associated with the term, and with the given option,
    /// or `None` if the term has never been encountered and indexed.
    ///
    /// If the field was not indexed with the indexing options that cover
    /// the requested options, the returned `SegmentPostings` the method does not fail
    /// and returns a `SegmentPostings` with as much information as possible.
    ///
    /// For instance, requesting `IndexRecordOption::Freq` for a
    /// `TextIndexingOptions` that does not index position will return a `SegmentPostings`
    /// with `DocId`s and frequencies.
    pub fn read_postings(&self, term: &Term, option: IndexRecordOption) -> Option<SegmentPostings> {
        let term_info = get!(self.get_term_info(term));
        Some(self.read_postings_from_terminfo(&term_info, option))
    }

    /// Returns the number of documents containing the term.
    pub fn doc_freq(&self, term: &Term) -> u32 {
        self.get_term_info(term)
            .map(|term_info| term_info.doc_freq)
            .unwrap_or(0u32)
    }
}


pub const FIELDNORM_TABLE: [u64; 256] = [
    0, 1, 2, 3, 4, 5, 6, 7,
    8, 9, 10, 11, 12, 13, 14, 15,
    16, 17, 18, 19, 20, 21, 22, 23,
    24, 25, 26, 27, 28, 29, 30, 31,
    32, 33, 34, 35, 36, 37, 38, 39,
    40, 42, 44, 46, 48, 50, 52, 54,
    56, 60, 64, 68, 72, 76, 80, 84,
    88, 96, 104, 112, 120, 128, 136, 144,
    152, 168, 184, 200, 216, 232, 248, 264,
    280, 312, 344, 376, 408, 440, 472, 504,
    536, 600, 664, 728, 792, 856, 920, 984,
    1048, 1176, 1304, 1432, 1560, 1688, 1816, 1944,
    2072, 2328, 2584, 2840, 3096, 3352, 3608, 3864, 4120,
    4632,
    5144,
    5656,
    6168,
    6680,
    7192,
    7704,
    8216,
    9240,
    10264,
    11288,
    12312,
    13336,
    14360,
    15384,
    16408,
    18456,
    20504,
    22552,
    24600,
    26648,
    28696,
    30744,
    32792,
    36888,
    40984,
    45080,
    49176,
    53272,
    57368,
    61464,
    65560,
    73752,
    81944,
    90136,
    98328,
    106520,
    114712,
    122904,
    131096,
    147480,
    163864,
    180248,
    196632,
    213016,
    229400,
    245784,
    262168,
    294936,
    327704,
    360472,
    393240,
    426008,
    458776,
    491544,
    524312,
    589848,
    655384,
    720920,
    786456,
    851992,
    917528,
    983064,
    1048600,
    1179672,
    1310744,
    1441816,
    1572888,
    1703960,
    1835032,
    1966104,
    2097176,
    2359320,
    2621464,
    2883608,
    3145752,
    3407896,
    3670040,
    3932184,
    4194328,
    4718616,
    5242904,
    5767192,
    6291480,
    6815768,
    7340056,
    7864344,
    8388632,
    9437208,
    10485784,
    11534360,
    12582936,
    13631512,
    14680088,
    15728664,
    16777240,
    18874392,
    20971544,
    23068696,
    25165848,
    27263000,
    29360152,
    31457304,
    33554456,
    37748760,
    41943064,
    46137368,
    50331672,
    54525976,
    58720280,
    62914584,
    67108888,
    75497496,
    83886104,
    92274712,
    100663320,
    109051928,
    117440536,
    125829144,
    134217752,
    150994968,
    167772184,
    184549400,
    201326616,
    218103832,
    234881048,
    251658264,
    268435480,
    301989912,
    335544344,
    369098776,
    402653208,
    436207640,
    469762072,
    503316504,
    536870936,
    603979800,
    671088664,
    738197528,
    805306392,
    872415256,
    939524120,
    1006632984,
    1073741848,
    1207959576,
    1342177304,
    1476395032,
    1610612760,
    1744830488,
    1879048216,
    2013265944
];


#[cfg(test)]
mod tests {
    #[test]
    fn test_fieldnorm_byte() {
        // const expression are not really a thing
        // yet... Therefore we do things the other way around.

        // The array is defined as a const,
        // and we check in the unit test that the const
        // value is matching the logic.
        const IDENTITY_PART: u8 = 24u8;
        fn decode_field_norm_exp_part(b: u8) -> u64 {
            let bits = (b & 0b00000111) as u64;
            let shift = b >> 3;
            if shift == 0 {
                bits
            } else {
                (bits | 8u64) << ((shift - 1u8) as u64)
            }
        }
        fn decode_fieldnorm_byte(b: u8) -> u64 {
            if b < IDENTITY_PART {
                b as u64
            } else {
                IDENTITY_PART as u64 + decode_field_norm_exp_part(b - IDENTITY_PART)
            }
        }
        for i in 0..256{
            //assert_eq!(FIELD_NORMS_TABLE[i], decode_fieldnorm_byte(i as u8));
            println!("{} {}", i, decode_fieldnorm_byte(i as u8));
        }
        assert!(false);
    }
}