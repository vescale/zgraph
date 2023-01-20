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

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/knz/bubbline"
	"github.com/spf13/cobra"
	"github.com/vescale/zgraph"
	"github.com/vescale/zgraph/session"
)

type options struct {
	dataDir string
}

func main() {
	var opt options
	cmd := cobra.Command{
		Use: "zgraph --data <dirname>",
		RunE: func(cmd *cobra.Command, args []string) error {
			db, err := zgraph.Open(opt.dataDir, nil)
			if err != nil {
				return err
			}

			session := db.NewSession()
			defer session.Close()

			interact(session)

			return nil
		},
		SilenceErrors: true,
	}

	cmd.Flags().StringVarP(&opt.dataDir, "data", "D", "./data", "Specify the data directory path")

	err := cmd.Execute()
	cobra.CheckErr(err)
}

func interact(session *session.Session) {
	fmt.Println("Welcome to zGraph interactive command line.")

	m := bubbline.New()
	m.Prompt = "zgraph> "
	m.NextPrompt = "      > "
	m.CheckInputComplete = func(input [][]rune, line, col int) bool {
		inputLine := string(input[line])
		if len(input) == 1 && strings.TrimSpace(inputLine) == "" {
			return true
		}
		return strings.Contains(inputLine, ";")
	}

	var lastStmt string
	for {
		m.Reset()
		if _, err := tea.NewProgram(m).Run(); err != nil {
			outputError(err)
			continue
		}

		if m.Err != nil {
			if m.Err == io.EOF {
				// No more input.
				break
			}
			if errors.Is(m.Err, bubbline.ErrInterrupted) {
				// Entered Ctrl+C to cancel input.
				fmt.Println("^C")
			} else {
				outputError(m.Err)
			}
			continue
		}

		input := lastStmt + m.Value()
		stmts := strings.Split(input, ";")
		for i := 0; i < len(stmts)-1; i++ {
			stmt := strings.TrimSpace(stmts[i])
			if stmt == "" {
				continue
			}
			runQuery(session, stmt)
		}
		lastStmt = stmts[len(stmts)-1]
	}
}

func runQuery(session *session.Session, query string) {
	rs, err := session.Execute(context.Background(), query)
	if err != nil {
		outputError(err)
		return
	}
	output, err := render(rs)
	if err != nil {
		outputError(err)
		return
	}
	if len(output) > 0 {
		fmt.Println(output)
	}
}

func render(rs session.ResultSet) (string, error) {
	defer rs.Close()
	w := table.NewWriter()
	w.Style().Format = table.FormatOptions{
		Footer: text.FormatDefault,
		Header: text.FormatDefault,
		Row:    text.FormatDefault,
	}

	if len(rs.Fields()) > 0 {
		var header []any
		for _, field := range rs.Fields() {
			header = append(header, field.Name)
		}
		w.AppendHeader(header)
	}

	fields := make([]any, 0, len(rs.Fields()))
	for range rs.Fields() {
		var s string
		fields = append(fields, &s)
	}

	for {
		if err := rs.Next(context.Background()); err != nil {
			return "", err
		}
		if !rs.Valid() {
			break
		}
		if err := rs.Scan(fields...); err != nil {
			return "", err
		}
		var row []any
		for _, f := range fields {
			row = append(row, *f.(*string))
		}
		w.AppendRow(row)
	}

	return w.Render(), nil
}

func outputError(err error) {
	fmt.Printf("Error: %v\n", err)
}
