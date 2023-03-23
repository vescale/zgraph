# zgraph
[![Go Reference](https://pkg.go.dev/badge/github.com/vescale/zgraph.svg)](https://pkg.go.dev/github.com/vescale/zgraph)
[![GitHub Actions](https://github.com/vescale/zgraph/workflows/Check/badge.svg)](https://github.com/vescale/zgraph/actions?query=workflow%3ACheck)

An embeddable graph database for large-scale vertices and edges.

## Installation

```bash
go get -u github.com/vescale/zgraph
```

## Usage

zGraph uses [PGQL](https://pgql-lang.org/) as its query language.

The following example shows how to create a graph and query it. The graph is an example in [PGQL specification](https://pgql-lang.org/spec/1.5/#edge-patterns).

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/vescale/zgraph"
	"github.com/vescale/zgraph/session"
)

func main() {
	db, err := zgraph.Open("test.db", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	sess := db.NewSession()
	defer sess.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create a graph.
	mustExec(ctx, sess, "CREATE GRAPH student_network")

	// Change the current graph.
	mustExec(ctx, sess, "USE student_network")

	// Create labels.
	mustExec(ctx, sess, "CREATE LABEL Person")
	mustExec(ctx, sess, "CREATE LABEL University")
	mustExec(ctx, sess, "CREATE LABEL knows")
	mustExec(ctx, sess, "CREATE LABEL studentOf")

	// Create vertices.
	mustExec(ctx, sess, `INSERT VERTEX x LABELS (Person) PROPERTIES (x.name = 'Kathrine', x.dob = DATE '1994-01-15')`)
	mustExec(ctx, sess, `INSERT VERTEX x LABELS (Person) PROPERTIES (x.name = 'Riya', x.dob = DATE '1995-03-20')`)
	mustExec(ctx, sess, `INSERT VERTEX x LABELS (Person) PROPERTIES (x.name = 'Lee', x.dob = DATE '1996-01-20')`)
	mustExec(ctx, sess, `INSERT VERTEX x LABELS (University) PROPERTIES (x.name = 'UC Berkeley')`)

	// Create edges.
	mustExec(ctx, sess, `INSERT EDGE e BETWEEN x AND y LABELS ( knows ) FROM MATCH (x), MATCH (y) WHERE x.name = 'Kathrine' AND y.name = 'Lee'`)
	mustExec(ctx, sess, `INSERT EDGE e BETWEEN x AND y LABELS ( knows ) FROM MATCH (x), MATCH (y) WHERE x.name = 'Kathrine' AND y.name = 'Riya'`)
	mustExec(ctx, sess, `INSERT EDGE e BETWEEN x AND y LABELS ( knows ) FROM MATCH (x), MATCH (y) WHERE x.name = 'Lee' AND y.name = 'Kathrine'`)
	mustExec(ctx, sess, `INSERT EDGE e BETWEEN x AND y LABELS ( studentOf ) FROM MATCH (x), MATCH (y) WHERE x.name = 'Kathrine' AND y.name = 'UC Berkeley'`)
	mustExec(ctx, sess, `INSERT EDGE e BETWEEN x AND y LABELS ( studentOf ) FROM MATCH (x), MATCH (y) WHERE x.name = 'Lee' AND y.name = 'UC Berkeley'`)
	mustExec(ctx, sess, `INSERT EDGE e BETWEEN x AND y LABELS ( studentOf ) FROM MATCH (x), MATCH (y) WHERE x.name = 'Riya' AND y.name = 'UC Berkeley'`)

	// Query the graph.
	rs, err := sess.Execute(ctx, "SELECT a.name AS a, b.name AS b FROM MATCH (a:Person) -[e:knows]-> (b:Person)")
	if err != nil {
		log.Fatal(err)
	}
	var a, b string
	for {
		if err := rs.Next(ctx); err != nil {
			log.Fatal(err)
		}
		if !rs.Valid() {
			break
		}
		if err := rs.Scan(&a, &b); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("'%s' knows '%s'\n", a, b)
	}

}

func mustExec(ctx context.Context, session *session.Session, query string) {
	rs, err := session.Execute(ctx, query)
	if err != nil {
		log.Fatal(err)
	}
	if err := rs.Next(ctx); err != nil {
		log.Fatal(err)
	}
}

```

## Contributing

We welcome contributions from everyone. zGraph is in its early stages, if you have any ideas or suggestions, please feel free to open an issue or pull request.

## License

zGraph is licensed under the Apache 2.0 license. See [LICENSE](LICENSE) for the full license text.
