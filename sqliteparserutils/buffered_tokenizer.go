package sqliteparserutils

import (
	"fmt"

	"github.com/antlr4-go/antlr/v4"
)

// bufferedTokenizer preload few next tokens and put them in the tokens buffer to support look-ahead access with limited distance
// Tokenizer maintains loaded tokens in ring buffer and use constant memory to support all requests (which is important for streaming mode)
type bufferedTokenizer struct {
	source antlr.TokenSource
	tokens []antlr.Token
	index  int
}

// createBufferedTokenizer initialize tokenizer which will store current token and bufferSize-1 next tokens for look-ahead accesses
func createBufferedTokenizer(source antlr.TokenSource, bufferSize int) *bufferedTokenizer {
	tokens := make([]antlr.Token, bufferSize)
	stream := bufferedTokenizer{source: source, tokens: tokens, index: 0}
	stream.load(bufferSize)
	return &stream
}

func (stream *bufferedTokenizer) load(n int) {
	i := 0
	for i < n {
		token := stream.source.NextToken()
		if token.GetChannel() != antlr.TokenDefaultChannel {
			continue
		}
		stream.tokens[(stream.index+i)%len(stream.tokens)] = token
		i += 1
	}
	stream.index = (stream.index + i) % len(stream.tokens)
}

func (stream *bufferedTokenizer) Consume() {
	stream.load(1)
}

func (stream *bufferedTokenizer) Get(k int) antlr.Token {
	if k >= len(stream.tokens) {
		panic(fmt.Errorf("out of buffer read attempts: %d >= %d", k, len(stream.tokens)))
	}
	return stream.tokens[(stream.index+k)%len(stream.tokens)]
}

func (stream *bufferedTokenizer) IsEOF() bool {
	return stream.Get(0).GetTokenType() == antlr.TokenEOF
}
