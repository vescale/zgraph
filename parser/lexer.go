// Copyright 2022 zGraph Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package parser

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode"

	"github.com/vescale/zgraph/datum"
)

var _ = yyLexer(&Lexer{})

// Pos represents the position of a token.
type Pos struct {
	Line   int
	Col    int
	Offset int
}

// Lexer implements the yyLexer interface.
type Lexer struct {
	r   reader
	buf bytes.Buffer

	errs         []error
	warns        []error
	stmtStartPos int

	// inBangComment is true if we are inside a `/*! ... */` block.
	// It is used to ignore a stray `*/` when scanning.
	inBangComment bool

	// Whether record the original text keyword position to the AST node.
	skipPositionRecording bool

	// lastScanOffset indicates last offset returned by scan().
	// It's used to substring sql in syntax error message.
	lastScanOffset int

	// lastKeyword records the previous keyword returned by scan().
	// determine whether an optimizer hint should be parsed or ignored.
	lastKeyword int
	// lastKeyword2 records the keyword before lastKeyword, it is used
	// to disambiguate hint after for update, which should be ignored.
	lastKeyword2 int
	// lastKeyword3 records the keyword before lastKeyword2, it is used
	// to disambiguate hint after create binding for update, which should
	// be pertained.
	lastKeyword3 int

	// hintPos records the start position of the previous optimizer hint.
	lastHintPos Pos

	// true if a dot follows an identifier
	identifierDot bool
}

// Errors returns the errors and warns during a scan.
func (l *Lexer) Errors() (warns []error, errs []error) {
	return l.warns, l.errs
}

// reset resets the sql string to be scanned.
func (l *Lexer) reset(sql string) {
	l.r = reader{s: sql, p: Pos{Line: 1}, l: len(sql)}
	l.buf.Reset()
	l.errs = l.errs[:0]
	l.warns = l.warns[:0]
	l.stmtStartPos = 0
	l.inBangComment = false
	l.lastKeyword = 0
}

func (l *Lexer) stmtText() string {
	endPos := l.r.pos().Offset
	if l.r.s[endPos-1] == '\n' {
		endPos = endPos - 1 // trim new line
	}
	if l.r.s[l.stmtStartPos] == '\n' {
		l.stmtStartPos++
	}

	text := l.r.s[l.stmtStartPos:endPos]

	l.stmtStartPos = endPos
	return text
}

// Errorf tells scanner something is wrong.
// Lexer satisfies yyLexer interface which need this function.
func (l *Lexer) Errorf(format string, a ...interface{}) (err error) {
	str := fmt.Sprintf(format, a...)
	val := l.r.s[l.lastScanOffset:]
	var lenStr = ""
	if len(val) > 2048 {
		lenStr = "(total length " + strconv.Itoa(len(val)) + ")"
		val = val[:2048]
	}
	err = fmt.Errorf("line %d column %d near \"%s\"%s %s",
		l.r.p.Line, l.r.p.Col, val, str, lenStr)
	return
}

// AppendError sets error into scanner.
// Lexer satisfies yyLexer interface which need this function.
func (l *Lexer) AppendError(err error) {
	if err == nil {
		return
	}
	l.errs = append(l.errs, err)
}

// AppendWarn sets warning into scanner.
func (l *Lexer) AppendWarn(err error) {
	if err == nil {
		return
	}
	l.warns = append(l.warns, err)
}

func (l *Lexer) getNextToken() int {
	r := l.r
	tok, pos, lit := l.scan()
	if tok == identifier {
		if tok1 := l.isTokenIdentifier(lit, pos.Offset); tok1 != 0 {
			tok = tok1
		}
	}
	l.r = r
	return tok
}

func (l *Lexer) getNextTwoTokens() (tok1 int, tok2 int) {
	r := l.r
	tok1, pos, lit := l.scan()
	if tok1 == identifier {
		if tmpToken := l.isTokenIdentifier(lit, pos.Offset); tmpToken != 0 {
			tok1 = tmpToken
		}
	}
	tok2, pos, lit = l.scan()
	if tok2 == identifier {
		if tmpToken := l.isTokenIdentifier(lit, pos.Offset); tmpToken != 0 {
			tok2 = tmpToken
		}
	}
	l.r = r
	return tok1, tok2
}

// Lex returns a token and store the token value in v.
// Lexer satisfies yyLexer interface.
// 0 and invalid are special token id this function would return:
// return 0 tells parser that scanner meets EOF,
// return invalid tells parser that scanner meets illegal character.
func (l *Lexer) Lex(v *yySymType) int {
	tok, pos, lit := l.scan()
	l.lastScanOffset = pos.Offset
	l.lastKeyword3 = l.lastKeyword2
	l.lastKeyword2 = l.lastKeyword
	l.lastKeyword = 0
	v.offset = pos.Offset
	v.ident = lit
	if tok == identifier {
		if tok1 := l.isTokenIdentifier(lit, pos.Offset); tok1 != 0 {
			tok = tok1
			l.lastKeyword = tok1
		}
	}

	switch tok {
	case intLit:
		return toInt(l, v, lit)
	case decLit:
		return toDecimal(l, v, lit)
	case singleAtIdentifier, doubleAtIdentifier, cast, extract:
		v.item = lit
		return tok
	case null:
		v.item = nil
	case quotedIdentifier, identifier:
		tok = identifier
		l.identifierDot = l.r.peek() == '.'
		v.ident = lit
	case stringLit:
		v.ident = lit
	}

	return tok
}

func toInt(l yyLexer, lval *yySymType, str string) int {
	n, err := strconv.ParseUint(str, 10, 64)
	if err != nil {
		e := err.(*strconv.NumError)
		if e.Err == strconv.ErrRange {
			// TODO: toDecimal maybe out of range still.
			// This kind of error should be throw to higher level, because truncated data maybe legal.
			// For example, this SQL returns error:
			// create table test (id decimal(30, 0));
			// insert into test values(123456789012345678901234567890123094839045793405723406801943850);
			// While this SQL:
			// select 1234567890123456789012345678901230948390457934057234068019438509023041874359081325875128590860234789847359871045943057;
			// get value 99999999999999999999999999999999999999999999999999999999999999999
			return toDecimal(l, lval, str)
		}
		l.AppendError(fmt.Errorf("integer literal: %v", err))
		return invalid
	}

	switch {
	case n <= math.MaxInt64:
		lval.item = int64(n)
	default:
		lval.item = n
	}
	return intLit
}

func toDecimal(l yyLexer, lval *yySymType, str string) int {
	dec, err := datum.NewDecimal(str)
	if err != nil {
		l.AppendError(err)
	}
	lval.item = dec
	return decLit
}

// LexLiteral returns the value of the converted literal
func (l *Lexer) LexLiteral() interface{} {
	symType := &yySymType{}
	l.Lex(symType)
	if symType.item == nil {
		return symType.ident
	}
	return symType.item
}

// InheritScanner returns a new scanner object which inherits configurations from the parent scanner.
func (l *Lexer) InheritScanner(sql string) *Lexer {
	return &Lexer{
		r: reader{s: sql},
	}
}

// NewLexer returns a new scanner object.
func NewLexer(s string) *Lexer {
	lexer := &Lexer{r: reader{s: s}}
	lexer.reset(s)
	return lexer
}

func (l *Lexer) skipWhitespace() byte {
	return l.r.incAsLongAs(func(b byte) bool {
		return unicode.IsSpace(rune(b))
	})
}

func (l *Lexer) scan() (tok int, pos Pos, lit string) {
	ch0 := l.r.peek()
	if unicode.IsSpace(rune(ch0)) {
		ch0 = l.skipWhitespace()
	}
	pos = l.r.pos()
	if l.r.eof() {
		// when scanner meets EOF, the returned token should be 0,
		// because 0 is a special token id to remind the parser that stream is end.
		return 0, pos, ""
	}

	if isIdentExtend(ch0) {
		return scanIdentifier(l)
	}

	// search a trie to get a token.
	node := &ruleTable
	for !(node.childs[ch0] == nil || l.r.eof()) {
		node = node.childs[ch0]
		if node.fn != nil {
			return node.fn(l)
		}
		l.r.inc()
		ch0 = l.r.peek()
	}

	tok, lit = node.token, l.r.data(&pos)
	return
}

func startWithSharp(s *Lexer) (tok int, pos Pos, lit string) {
	s.r.incAsLongAs(func(ch byte) bool {
		return ch != '\n'
	})
	return s.scan()
}

func startWithStar(s *Lexer) (tok int, pos Pos, lit string) {
	pos = s.r.pos()
	s.r.inc()

	// skip and exit '/*!' if we see '*/'
	if s.inBangComment && s.r.peek() == '/' {
		s.inBangComment = false
		s.r.inc()
		return s.scan()
	}
	// otherwise it is just a normal star.
	s.identifierDot = false
	return '*', pos, "*"
}

func startWithAt(s *Lexer) (tok int, pos Pos, lit string) {
	pos = s.r.pos()
	s.r.inc()

	tok, lit = scanIdentifierOrString(s)
	switch tok {
	case '@':
		s.r.inc()
		stream := s.r.s[pos.Offset+2:]
		var prefix string
		for _, v := range []string{"global.", "session.", "local."} {
			if len(v) > len(stream) {
				continue
			}
			if strings.EqualFold(stream[:len(v)], v) {
				prefix = v
				s.r.incN(len(v))
				break
			}
		}
		tok, lit = scanIdentifierOrString(s)
		switch tok {
		case stringLit, quotedIdentifier:
			tok, lit = doubleAtIdentifier, "@@"+prefix+lit
		case identifier:
			tok, lit = doubleAtIdentifier, s.r.data(&pos)
		}
	case invalid:
		return
	default:
		tok = singleAtIdentifier
	}

	return
}

func scanIdentifier(s *Lexer) (int, Pos, string) {
	pos := s.r.pos()
	s.r.incAsLongAs(isIdentChar)
	return identifier, pos, s.r.data(&pos)
}

func scanIdentifierOrString(s *Lexer) (tok int, lit string) {
	ch1 := s.r.peek()
	switch ch1 {
	case '\'', '"':
		tok, _, lit = startString(s)
	case '`':
		tok, _, lit = scanQuotedIdent(s)
	default:
		if isUserVarChar(ch1) {
			pos := s.r.pos()
			s.r.incAsLongAs(isUserVarChar)
			tok, lit = identifier, s.r.data(&pos)
		} else {
			tok = int(ch1)
		}
	}
	return
}

var (
	quotedIdentifier = -identifier
)

func scanQuotedIdent(s *Lexer) (tok int, pos Pos, lit string) {
	pos = s.r.pos()
	s.r.inc()
	s.buf.Reset()
	for !s.r.eof() {
		ch := s.r.readByte()
		if ch == '`' {
			if s.r.peek() != '`' {
				// don't return identifier in case that it's interpreted as keyword token later.
				tok, lit = quotedIdentifier, s.buf.String()
				return
			}
			s.r.inc()
		}
		s.buf.WriteByte(ch)
	}
	tok = invalid
	return
}

func startString(s *Lexer) (tok int, pos Pos, lit string) {
	return s.scanString()
}

// lazyBuf is used to avoid allocation if possible.
// it has a useBuf field indicates whether bytes.Buffer is necessary. if
// useBuf is false, we can avoid calling bytes.Buffer.String(), which
// make a copy of data and cause allocation.
type lazyBuf struct {
	useBuf bool
	r      *reader
	b      *bytes.Buffer
	p      *Pos
}

func (mb *lazyBuf) setUseBuf(str string) {
	if !mb.useBuf {
		mb.useBuf = true
		mb.b.Reset()
		mb.b.WriteString(str)
	}
}

func (mb *lazyBuf) writeRune(r rune, w int) {
	if mb.useBuf {
		if w > 1 {
			mb.b.WriteRune(r)
		} else {
			mb.b.WriteByte(byte(r))
		}
	}
}

func (mb *lazyBuf) data() string {
	var lit string
	if mb.useBuf {
		lit = mb.b.String()
	} else {
		lit = mb.r.data(mb.p)
		lit = lit[1 : len(lit)-1]
	}
	return lit
}

func (l *Lexer) scanString() (tok int, pos Pos, lit string) {
	tok, pos = stringLit, l.r.pos()
	ending := l.r.readByte()
	l.buf.Reset()
	for !l.r.eof() {
		ch0 := l.r.readByte()
		if ch0 == ending {
			if l.r.peek() != ending {
				lit = l.buf.String()
				return
			}
			l.r.inc()
			l.buf.WriteByte(ch0)
		} else if ch0 == '\\' {
			if l.r.eof() {
				break
			}
			l.handleEscape(l.r.peek(), &l.buf)
			l.r.inc()
		} else {
			l.buf.WriteByte(ch0)
		}
	}

	tok = invalid
	return
}

// handleEscape handles the case in scanString when previous char is '\'.
func (*Lexer) handleEscape(b byte, buf *bytes.Buffer) {
	var ch0 byte
	/*
		\" \' \\ \n \0 \b \Z \r \t ==> escape to one char
		\% \_ ==> preserve both char
		other ==> remove \
	*/
	switch b {
	case 'n':
		ch0 = '\n'
	case '0':
		ch0 = 0
	case 'b':
		ch0 = 8
	case 'Z':
		ch0 = 26
	case 'r':
		ch0 = '\r'
	case 't':
		ch0 = '\t'
	case '%', '_':
		buf.WriteByte('\\')
		ch0 = b
	default:
		ch0 = b
	}
	buf.WriteByte(ch0)
}

func startWithNumber(s *Lexer) (tok int, pos Pos, lit string) {
	if s.identifierDot {
		return scanIdentifier(s)
	}
	pos = s.r.pos()
	tok = intLit
	ch0 := s.r.readByte()
	if ch0 == '0' {
		tok = intLit
		ch1 := s.r.peek()
		switch {
		case ch1 >= '0' && ch1 <= '7':
			s.r.inc()
			s.scanOct()
		case ch1 == 'x' || ch1 == 'X':
			s.r.inc()
			p1 := s.r.pos()
			s.scanHex()
			p2 := s.r.pos()
			// 0x, 0x7fz3 are identifier
			if p1 == p2 || isDigit(s.r.peek()) {
				s.r.incAsLongAs(isIdentChar)
				return identifier, pos, s.r.data(&pos)
			}
			tok = intLit
		case ch1 == 'b':
			s.r.inc()
			p1 := s.r.pos()
			s.scanBit()
			p2 := s.r.pos()
			// 0b, 0b123, 0b1ab are identifier
			if p1 == p2 || isDigit(s.r.peek()) {
				s.r.incAsLongAs(isIdentChar)
				return identifier, pos, s.r.data(&pos)
			}
			tok = intLit
		case ch1 == '.':
			return s.scanFloat(&pos)
		case ch1 == 'B':
			s.r.incAsLongAs(isIdentChar)
			return identifier, pos, s.r.data(&pos)
		}
	}

	s.scanDigits()
	ch0 = s.r.peek()
	if ch0 == '.' || ch0 == 'e' || ch0 == 'E' {
		return s.scanFloat(&pos)
	}

	// Identifiers may begin with a digit but unless quoted may not consist solely of digits.
	if !s.r.eof() && isIdentChar(ch0) {
		s.r.incAsLongAs(isIdentChar)
		return identifier, pos, s.r.data(&pos)
	}
	lit = s.r.data(&pos)
	return
}

func startWithDot(s *Lexer) (tok int, pos Pos, lit string) {
	pos = s.r.pos()
	s.r.inc()
	if s.identifierDot {
		return int('.'), pos, "."
	}
	if isDigit(s.r.peek()) {
		tok, p, l := s.scanFloat(&pos)
		if tok == identifier {
			return invalid, p, l
		}
		return tok, p, l
	}
	tok, lit = int('.'), "."
	return
}

func (l *Lexer) scanOct() {
	l.r.incAsLongAs(func(ch byte) bool {
		return ch >= '0' && ch <= '7'
	})
}

func (l *Lexer) scanHex() {
	l.r.incAsLongAs(func(ch byte) bool {
		return ch >= '0' && ch <= '9' ||
			ch >= 'a' && ch <= 'f' ||
			ch >= 'A' && ch <= 'F'
	})
}

func (l *Lexer) scanBit() {
	l.r.incAsLongAs(func(ch byte) bool {
		return ch == '0' || ch == '1'
	})
}

func (l *Lexer) scanFloat(beg *Pos) (tok int, pos Pos, lit string) {
	l.r.updatePos(*beg)
	// float = D1 . D2 e D3
	l.scanDigits()
	ch0 := l.r.peek()
	if ch0 == '.' {
		l.r.inc()
		l.scanDigits()
		ch0 = l.r.peek()
	}
	if ch0 == 'e' || ch0 == 'E' {
		l.r.inc()
		ch0 = l.r.peek()
		if ch0 == '-' || ch0 == '+' {
			l.r.inc()
		}
		if isDigit(l.r.peek()) {
			l.scanDigits()
			tok = decLit
		} else {
			// D1 . D2 e XX when XX is not D3, parse the result to an identifier.
			// 9e9e = 9e9(float) + e(identifier)
			// 9est = 9est(identifier)
			l.r.updatePos(*beg)
			l.r.incAsLongAs(isIdentChar)
			tok = identifier
		}
	} else {
		tok = decLit
	}
	pos, lit = *beg, l.r.data(beg)
	return
}

func (l *Lexer) scanDigits() string {
	pos := l.r.pos()
	l.r.incAsLongAs(isDigit)
	return l.r.data(&pos)
}

// scanVersionDigits scans for `min` to `max` digits (range inclusive) used in
// `/*!12345 ... */` comments.
func (l *Lexer) scanVersionDigits(min, max int) {
	pos := l.r.pos()
	for i := 0; i < max; i++ {
		ch := l.r.peek()
		if isDigit(ch) {
			l.r.inc()
		} else if i < min {
			l.r.updatePos(pos)
			return
		} else {
			break
		}
	}
}

func (l *Lexer) scanFeatureIDs() (featureIDs []string) {
	pos := l.r.pos()
	const init, expectChar, obtainChar = 0, 1, 2
	state := init
	var b strings.Builder
	for !l.r.eof() {
		ch := l.r.peek()
		l.r.inc()
		switch state {
		case init:
			if ch == '[' {
				state = expectChar
				break
			}
			l.r.updatePos(pos)
			return nil
		case expectChar:
			if isIdentChar(ch) {
				b.WriteByte(ch)
				state = obtainChar
				break
			}
			l.r.updatePos(pos)
			return nil
		case obtainChar:
			if isIdentChar(ch) {
				b.WriteByte(ch)
				state = obtainChar
				break
			} else if ch == ',' {
				featureIDs = append(featureIDs, b.String())
				b.Reset()
				state = expectChar
				break
			} else if ch == ']' {
				featureIDs = append(featureIDs, b.String())
				return featureIDs
			}
			l.r.updatePos(pos)
			return nil
		}
	}
	l.r.updatePos(pos)
	return nil
}

func (l *Lexer) lastErrorAsWarn() {
	if len(l.errs) == 0 {
		return
	}
	l.warns = append(l.warns, l.errs[len(l.errs)-1])
	l.errs = l.errs[:len(l.errs)-1]
}

type reader struct {
	s string
	p Pos
	l int
}

var eof = Pos{-1, -1, -1}

func (r *reader) eof() bool {
	return r.p.Offset >= r.l
}

// peek() peeks a rune from underlying reader.
// if reader meets EOF, it will return 0. to distinguish from
// the real 0, the caller should call r.eof() again to check.
func (r *reader) peek() byte {
	if r.eof() {
		return 0
	}
	return r.s[r.p.Offset]
}

// inc increase the position offset of the reader.
// peek must be called before calling inc!
func (r *reader) inc() {
	if r.s[r.p.Offset] == '\n' {
		r.p.Line++
		r.p.Col = 0
	}
	r.p.Offset++
	r.p.Col++
}

func (r *reader) incN(n int) {
	for i := 0; i < n; i++ {
		r.inc()
	}
}

func (r *reader) readByte() (ch byte) {
	ch = r.peek()
	if r.eof() {
		return
	}
	r.inc()
	return
}

func (r *reader) pos() Pos {
	return r.p
}

func (r *reader) updatePos(pos Pos) {
	r.p = pos
}

func (r *reader) data(from *Pos) string {
	return r.s[from.Offset:r.p.Offset]
}

func (r *reader) incAsLongAs(fn func(b byte) bool) byte {
	for {
		ch := r.peek()
		if !fn(ch) {
			return ch
		}
		if r.eof() {
			return 0
		}
		r.inc()
	}
}
