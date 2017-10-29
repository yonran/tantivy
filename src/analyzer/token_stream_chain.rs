use analyzer::{TokenStream, Token};


/// We do not want phrase queries to accidently match over two
/// field values because the first one was ending by the beginning
/// of the phrase while the second one was starting by the end of
/// the phrase.
///
/// In order to address this behavior, we do add a position gap between
/// the two fields.
///
/// The first one will contain tokens bearing position `0..n`
/// The second one will contain tokens bearing position `n + POSITION_GAP..`
const POSITION_GAP: usize = 2;

/// `TokenStreamChain` is the result of the concatenation of a list
/// of token streams.
pub struct TokenStreamChain<TTokenStream: TokenStream> {
    offsets: Vec<usize>,
    token_streams: Vec<TTokenStream>,
    position_shift: usize,
    stream_idx: usize,
    token: Token,
}

impl<'a, TTokenStream> TokenStreamChain<TTokenStream> 
    where TTokenStream: TokenStream {

    /// Creates a new chained token stream.
    ///
    /// `Offsets` makes it possible to shift each of the offsets
    /// of the different `TokenStream`s by a given offset given
    /// in argument.
    pub fn new(offsets: Vec<usize>,
               token_streams: Vec<TTokenStream>) -> TokenStreamChain<TTokenStream> {
        TokenStreamChain {
            offsets: offsets,
            stream_idx: 0,
            token_streams: token_streams,
            position_shift: 0,
            token: Token::default(),
        }
    }
}

impl<'a, TTokenStream> TokenStream for TokenStreamChain<TTokenStream>
    where TTokenStream: TokenStream {
    fn advance(&mut self) -> bool {
        while self.stream_idx < self.token_streams.len() {
            let token_stream = &mut self.token_streams[self.stream_idx];
            if token_stream.advance() {
                let token = token_stream.token();
                let offset_offset = self.offsets[self.stream_idx];
                self.token.offset_from = token.offset_from + offset_offset;
                self.token.offset_from = token.offset_from + offset_offset;
                self.token.position = token.position + self.position_shift;
                self.token.term.clear();
                self.token.term.push_str(token.term.as_str());
                return true;
            }
            else {
                self.stream_idx += 1;
                self.position_shift = self.token.position + POSITION_GAP;
            }
        }
        false
    }

    fn token(&self) -> &Token {
        assert!(
            self.stream_idx < self.token_streams.len(),
            "You called .token() after the end of the token stream has been reached"
        );
        &self.token
    }

    fn token_mut(&mut self) -> &mut Token {
        assert!(
            self.stream_idx < self.token_streams.len(),
            "You called .token() after the end of the token stream has been reached"
        );
        &mut self.token
    }
}
