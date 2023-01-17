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
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/jedib0t/go-pretty/v6/table"
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
	for {
		fmt.Print("zgraph> ")
		var buf string
		reader := bufio.NewReader(os.Stdin)
		for {

			line, _ := reader.ReadString('\n')
			buf += line
			if strings.Contains(line, ";") {
				break
			}
			fmt.Print("      > ")
		}

		semicolon := strings.Index(buf, ";")
		query := strings.TrimSpace(buf[:semicolon])
		buf = buf[semicolon+1:]
		if query == "" {
			continue
		}

		rs, err := session.Execute(context.Background(), query)
		if err != nil {
			outputError(err)
			continue
		}
		output, err := renderResult(rs)
		if err != nil {
			outputError(err)
			continue
		}
		if len(output) > 0 {
			fmt.Println(output)
		}
	}
}

func renderResult(rs session.ResultSet) (string, error) {
	defer rs.Close()
	w := table.NewWriter()

	// TODO: set table header

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
