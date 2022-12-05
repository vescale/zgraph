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
	"fmt"

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
		// TODO: scan text and execute it.
	}
}
