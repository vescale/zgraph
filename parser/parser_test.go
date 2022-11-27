package parser_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vescale/zgraph/parser"
)

func TestSimple(t *testing.T) {
	p := parser.New()
	stmts, _, err := p.Parse(`
SELECT label(owner),
       COUNT(*) AS numTransactions,
       SUM(out.amount) AS totalOutgoing,
       LISTAGG(out.amount) AS amounts
FROM MATCH (a:Account) -[:owner]-> (owner:Person|Company) ON financial_transactions
     , MATCH (a) -[out:transaction]-> (:Account) ON financial_transactions
GROUP BY label(owner)
ORDER BY label(owner);
`)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
}
