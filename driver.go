// Copyright 2023 zGraph Authors. All rights reserved.
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

package zgraph

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"

	"github.com/vescale/zgraph/datum"
	"github.com/vescale/zgraph/session"
	"github.com/vescale/zgraph/types"
)

const driverName = "zgraph"

func init() {
	sql.Register(driverName, &Driver{})
}

var (
	_ driver.Driver           = &Driver{}
	_ driver.DriverContext    = &Driver{}
	_ driver.Connector        = &connector{}
	_ io.Closer               = &connector{}
	_ driver.Conn             = &conn{}
	_ driver.Stmt             = &stmt{}
	_ driver.StmtExecContext  = &stmt{}
	_ driver.StmtQueryContext = &stmt{}
	_ driver.Rows             = &rows{}
)

type Driver struct{}

func (d *Driver) Open(_ string) (driver.Conn, error) {
	return nil, errors.New("Driver.Open should not be called as Driver.OpenConnector is implemented")
}

func (d *Driver) OpenConnector(dsn string) (driver.Connector, error) {
	db, err := Open(dsn, nil)
	if err != nil {
		return nil, err
	}
	return &connector{db: db}, nil
}

type connector struct {
	db *DB
}

func (c *connector) Connect(_ context.Context) (driver.Conn, error) {
	return &conn{session: c.db.NewSession()}, nil
}

func (c *connector) Driver() driver.Driver {
	return &Driver{}
}

func (c *connector) Close() error {
	return c.db.Close()
}

type conn struct {
	session *session.Session
}

func (c *conn) Ping(_ context.Context) error {
	return nil
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *conn) PrepareContext(_ context.Context, query string) (driver.Stmt, error) {
	return &stmt{session: c.session, query: query}, nil
}

func (c *conn) Close() error {
	c.session.Close()
	return nil
}

func (c *conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *conn) BeginTx(_ context.Context, _ driver.TxOptions) (driver.Tx, error) {
	return nil, errors.New("transactions are not supported")
}

type stmt struct {
	session *session.Session
	query   string
}

func (s *stmt) Close() error {
	return nil
}

func (s *stmt) NumInput() int {
	return -1
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	if len(args) > 0 {
		return nil, fmt.Errorf("placeholder arguments not supported")
	}
	return s.ExecContext(context.Background(), nil)
}

func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if len(args) > 0 {
		return nil, fmt.Errorf("placeholder arguments not supported")
	}
	rs, err := s.session.Execute(ctx, s.query)
	if err != nil {
		return nil, err
	}
	if err := rs.Next(ctx); err != nil {
		return nil, err
	}
	return driver.ResultNoRows, nil
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	if len(args) > 0 {
		return nil, fmt.Errorf("placeholder arguments not supported")
	}
	return s.QueryContext(context.Background(), nil)
}

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if len(args) > 0 {
		return nil, fmt.Errorf("placeholder arguments not supported")
	}
	rs, err := s.session.Execute(ctx, s.query)
	if err != nil {
		return nil, err
	}
	return &rows{ctx: ctx, rs: rs}, nil
}

type rows struct {
	ctx context.Context
	rs  session.ResultSet
}

func (r *rows) Columns() []string {
	return r.rs.Columns()
}

func (r *rows) Close() error {
	return r.rs.Close()
}

func (r *rows) Next(dest []driver.Value) error {
	if err := r.rs.Next(r.ctx); err != nil {
		return err
	}
	if !r.rs.Valid() {
		return io.EOF
	}
	for i, d := range r.rs.Row() {
		if d == datum.Null {
			dest[i] = nil
			continue
		}
		switch d.Type() {
		case types.Bool:
			dest[i] = datum.AsBool(d)
		case types.Int:
			dest[i] = datum.AsInt(d)
		case types.Float:
			dest[i] = datum.AsFloat(d)
		default:
			dest[i] = datum.AsString(d)
		}
	}
	return nil
}
