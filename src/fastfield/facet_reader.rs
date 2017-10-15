use super::MultiValueIntFastFieldReader;
use DocId;
use schema::Facet;
use termdict::{TermDictionary, TermDictionaryImpl};

pub struct FacetReader {
    term_ords: MultiValueIntFastFieldReader,
    term_dict: TermDictionaryImpl,
    facet: Facet,
}

impl FacetReader {
    pub fn new(
        term_ords: MultiValueIntFastFieldReader,
        term_dict: TermDictionaryImpl,
    ) -> FacetReader {
        FacetReader {
            term_ords: term_ords,
            term_dict: term_dict,
            facet: Facet::new()
        }
    }

    pub fn facet_from_ord(&mut self, term_ord: usize) -> &Facet {
        let term = self.term_dict.ord_to_term(term_ord as u64, self.facet.inner_buffer_mut());
        &self.facet
    }

    pub fn term_ords(&mut self, doc: DocId) -> &[u64] {
        self.term_ords.get_vals(doc)
    }
}