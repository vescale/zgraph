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
	"database/sql"
	"errors"
	"fmt"
	"io"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/knz/bubbline"
	"github.com/spf13/cobra"
	_ "github.com/vescale/zgraph"
)

type options struct {
	global struct {
		dataDir string
	}
	play struct {
	}
	serve struct {
	}
}

func main() {
	var opt options
	cmd := cobra.Command{
		Use: "zgraph [command] [flags]",
		Long: `zgraph is a command line tool for wrapping the embeddable graph
database 'github.com/vescale/zgraph'', and provide a convenient
approach to experiencing the power to graph database`,
		Example: strings.TrimLeft(`
  zgraph play                      # Launch a zGraph playground
  zgraph play --datadir ./test     # Specify the data directory of the playground
  zgraph service                   # Launch a zGraph as a data service`, "\n"),
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
		SilenceErrors:         true,
		DisableFlagsInUseLine: true,
	}

	// Global options
	cmd.Flags().StringVarP(&opt.global.dataDir, "datadir", "D", "./data", "Specify the data directory path")

	// Subcommands
	cmd.AddCommand(playCmd(&opt))
	cmd.AddCommand(servCmd(&opt))

	err := cmd.Execute()
	cobra.CheckErr(err)
}

func playCmd(opt *options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "play -D <dirname>",
		Short: "Run the zgraph as a playground",
		RunE: func(cmd *cobra.Command, args []string) error {
			db, err := sql.Open("zgraph", opt.global.dataDir)
			if err != nil {
				return err
			}
			defer db.Close()

			conn, err := db.Conn(context.Background())
			if err != nil {
				return err
			}
			defer conn.Close()

			interact(conn)

			return nil
		},
		SilenceErrors: true,
	}

	return cmd
}

func servCmd(opt *options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "service -D <dirname> -L :8080 --apis api.yaml",
		Short: "Run the zgraph as a data service instance",
		RunE: func(cmd *cobra.Command, args []string) error {
			// TODO: implement the data API.
			return nil
		},
		SilenceErrors: true,
	}

	return cmd
}

func interact(conn *sql.Conn) {
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
			runQuery(conn, stmt)
		}
		lastStmt = stmts[len(stmts)-1]
	}
}

func runQuery(conn *sql.Conn, query string) {
	rows, err := conn.QueryContext(context.Background(), query)
	if err != nil {
		outputError(err)
		return
	}
	defer rows.Close()

	output, err := render(rows)
	if err != nil {
		outputError(err)
		return
	}
	if len(output) > 0 {
		fmt.Println(output)
	}
}

func render(rows *sql.Rows) (string, error) {
	w := table.NewWriter()
	w.Style().Format = table.FormatOptions{
		Footer: text.FormatDefault,
		Header: text.FormatDefault,
		Row:    text.FormatDefault,
	}

	cols, err := rows.Columns()
	if err != nil {
		return "", err
	}

	if len(cols) > 0 {
		var header []any
		for _, col := range cols {
			header = append(header, col)
		}
		w.AppendHeader(header)
	}

	dest := make([]any, len(cols))
	for i := range cols {
		var anyStr sql.NullString
		dest[i] = &anyStr
	}

	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return "", err
		}
		var row []any
		for _, d := range dest {
			anyStr := *d.(*sql.NullString)
			row = append(row, anyStr.String)
		}
		w.AppendRow(row)
	}
	if rows.Err() != nil {
		return "", rows.Err()
	}
	return w.Render(), nil
}

func outputError(err error) {
	fmt.Printf("Error: %v\n", err)
}
