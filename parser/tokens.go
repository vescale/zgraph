// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package parser

func isLetter(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}

func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

func isIdentChar(ch byte) bool {
	return isLetter(ch) || isDigit(ch) || ch == '_' || ch == '$' || isIdentExtend(ch)
}

func isIdentExtend(ch byte) bool {
	return ch >= 0x80
}

func isUserVarChar(ch byte) bool {
	return isLetter(ch) || isDigit(ch) || ch == '_' || ch == '$' || ch == '.' || isIdentExtend(ch)
}

type trieNode struct {
	childs [256]*trieNode
	token  int
	fn     func(s *Lexer) (int, Pos, string)
}

var ruleTable trieNode

func initTokenByte(c byte, tok int) {
	if ruleTable.childs[c] == nil {
		ruleTable.childs[c] = &trieNode{}
	}
	ruleTable.childs[c].token = tok
}

func initTokenString(str string, tok int) {
	node := &ruleTable
	for _, c := range str {
		if node.childs[c] == nil {
			node.childs[c] = &trieNode{}
		}
		node = node.childs[c]
	}
	node.token = tok
}

func initTokenFunc(str string, fn func(s *Lexer) (int, Pos, string)) {
	for i := 0; i < len(str); i++ {
		c := str[i]
		if ruleTable.childs[c] == nil {
			ruleTable.childs[c] = &trieNode{}
		}
		ruleTable.childs[c].fn = fn
	}
}

func init() {
	// invalid is a special token defined in parser.y, when parser meet
	// this token, it will throw an error.
	// set root trie node's token to invalid, so when input match nothing
	// in the trie, invalid will be the default return token.
	ruleTable.token = invalid
	initTokenByte('+', int('+'))
	initTokenByte('-', int('-'))
	initTokenByte('>', int('>'))
	initTokenByte('<', int('<'))
	initTokenByte('(', int('('))
	initTokenByte(')', int(')'))
	initTokenByte('[', int('['))
	initTokenByte(']', int(']'))
	initTokenByte(';', int(';'))
	initTokenByte(',', int(','))
	initTokenByte('&', int('&'))
	initTokenByte('%', int('%'))
	initTokenByte(':', int(':'))
	initTokenByte('|', int('|'))
	initTokenByte('!', int('!'))
	initTokenByte('^', int('^'))
	initTokenByte('~', int('~'))
	initTokenByte('\\', int('\\'))
	initTokenByte('?', paramMarker)
	initTokenByte('=', eq)
	initTokenByte('{', int('{'))
	initTokenByte('}', int('}'))

	initTokenString("||", pipes)
	initTokenString("&&", andand)
	initTokenString("&^", andnot)
	initTokenString(":=", assignmentEq)
	initTokenString("<=>", nulleq)
	initTokenString(">=", ge)
	initTokenString("<=", le)
	initTokenString("!=", neq)
	initTokenString("<>", neqSynonym)
	initTokenString(".*", allProp)
	initTokenString("\\N", null)
	initTokenString("<-", leftArrow)
	initTokenString("->", rightArrow)
	initTokenString("-[", edgeOutgoingLeft)
	initTokenString("]->", edgeOutgoingRight)
	initTokenString("<-[", edgeIncomingLeft)
	initTokenString("]-", edgeIncomingRight)
	initTokenString("-/", reachOutgoingLeft)
	initTokenString("<-/", reachIncomingLeft)

	initTokenFunc("/", startWithSlash)
	initTokenFunc("@", startWithAt)
	initTokenFunc("*", startWithStar)
	initTokenFunc("#", startWithSharp)
	initTokenFunc(".", startWithDot)
	initTokenFunc("_$ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz", scanIdentifier)
	initTokenFunc("`", scanQuotedIdent)
	initTokenFunc("0123456789", startWithNumber)
	initTokenFunc("'\"", startString)
}

// isInTokenMap indicates whether the target string is contained in tokenMap.
func isInTokenMap(target string) bool {
	_, ok := tokenMap[target]
	return ok
}

// tokenMap is a map of known identifiers to the parser token ID.
// Please try to keep the map in alphabetical order.
var tokenMap = map[string]int{
	"ABS":              abs,
	"ALL":              all,
	"ALL_DIFFERENT":    allDifferent,
	"AND":              and,
	"ANY":              any,
	"ARRAY_AGG":        arrayAgg,
	"AS":               as,
	"ASC":              asc,
	"AVG":              avg,
	"BEGIN":            begin,
	"BETWEEN":          between,
	"BIGINT":           bigIntType,
	"BOOLEAN":          booleanType,
	"BY":               by,
	"CASE":             caseKwd,
	"CAST":             cast,
	"CEIL":             ceil,
	"CEILING":          ceiling,
	"CHEAPEST":         cheapest,
	"COMMIT":           commit,
	"COST":             cost,
	"COUNT":            count,
	"CREATE":           create,
	"DATE":             dateType,
	"DAY":              day,
	"DEFAULT":          defaultKwd,
	"DELETE":           deleteKwd,
	"DESC":             desc,
	"DISTINCT":         distinct,
	"DISTINCTROW":      distinct,
	"DIV":              div,
	"DOUBLE":           doubleType,
	"DROP":             drop,
	"ELEMENT_NUMBER":   elementNumber,
	"ELSE":             elseKwd,
	"END":              end,
	"EXISTS":           exists,
	"EXPLAIN":          explain,
	"EXTRACT":          extract,
	"FALSE":            falseKwd,
	"FLOAT":            floatType,
	"FLOOR":            floor,
	"FROM":             from,
	"GRAPH":            graph,
	"GROUP":            group,
	"HAVING":           having,
	"HAS_LABEL":        hasLabel,
	"HOUR":             hour,
	"IF":               ifKwd,
	"IN":               in,
	"IN_DEGREE":        inDegree,
	"INDEX":            index,
	"INSERT":           insert,
	"INT":              intType,
	"INTEGER":          integerType,
	"INTERVAL":         interval,
	"INTO":             into,
	"ID":               id,
	"IS":               is,
	"JAVA_REGEXP_LIKE": javaRegexpLike,
	"LABEL":            label,
	"LABELS":           labels,
	"LIMIT":            limit,
	"LISTAGG":          listagg,
	"LONG":             long,
	"LOWER":            lower,
	"MATCH":            match,
	"MATCH_NUMBER":     matchNumber,
	"MAX":              max,
	"MIN":              min,
	"MINUTE":           minute,
	"MOD":              mod,
	"MONTH":            month,
	"NOT":              not,
	"NULL":             null,
	"OFFSET":           offset,
	"ON":               on,
	"OR":               or,
	"ORDER":            order,
	"OUT_DEGREE":       outDegree,
	"PATH":             path,
	"PRECISION":        precisionType,
	"PRIMARY":          primary,
	"PROPERTIES":       properties,
	"ROLLBACK":         rollback,
	"SECOND":           second,
	"SELECT":           selectKwd,
	"SET":              set,
	"SHORTEST":         shortest,
	"STRING":           stringKwd,
	"SUBSTR":           substring,
	"SUBSTRING":        substring,
	"SUM":              sum,
	"THEN":             then,
	"TIME":             timeType,
	"TIMESTAMP":        timestampType,
	"TOP":              top,
	"TRUE":             trueKwd,
	"UNIQUE":           unique,
	"UPDATE":           update,
	"UPPER":            uppper,
	"USE":              use,
	"VERTEX":           vertex,
	"WHEN":             when,
	"WHERE":            where,
	"WITH":             with,
	"XOR":              xor,
	"YEAR":             yearType,
}

var btFuncTokenMap = map[string]int{}

func (l *Lexer) isTokenIdentifier(lit string, offset int) int {
	// An identifier before or after '.' means it is part of a qualified identifier.
	// We do not parse it as keyword.
	if l.r.peek() == '.' {
		return 0
	}
	if offset > 0 && l.r.s[offset-1] == '.' {
		return 0
	}
	buf := &l.buf
	buf.Reset()
	buf.Grow(len(lit))
	data := buf.Bytes()[:len(lit)]
	for i := 0; i < len(lit); i++ {
		if lit[i] >= 'a' && lit[i] <= 'z' {
			data[i] = lit[i] + 'A' - 'a'
		} else {
			data[i] = lit[i]
		}
	}

	checkBtFuncToken := false
	if l.r.peek() == '(' {
		checkBtFuncToken = true
	}
	if checkBtFuncToken {
		if tok := btFuncTokenMap[string(data)]; tok != 0 {
			return tok
		}
	}
	tok := tokenMap[string(data)]
	return tok
}
