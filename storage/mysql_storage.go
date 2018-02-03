// Copyright (C) 2017 go-nebulas authors
//
// This file is part of the go-nebulas library.
//
// the go-nebulas library is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// the go-nebulas library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with the go-nebulas library.  If not, see <http://www.gnu.org/licenses/>.
//

/*Package storage scheme
+-------+------------------+------+-----+---------+-------+
| Field | Type             | Null | Key | Default | Extra |
+-------+------------------+------+-----+---------+-------+
| nkey   | varbinary(256)  | NO   | PRI | NULL    |       |
| nvalue | blob            | YES  |     | NULL    |       |
+-------+------------------+------+-----+---------+-------+
*/
package storage

import (
	"database/sql"

	"github.com/go-sql-driver/mysql"
	lru "github.com/hashicorp/golang-lru"
	"github.com/nebulasio/go-nebulas/util/byteutils"
	"github.com/nebulasio/go-nebulas/util/logging"
	"github.com/sirupsen/logrus"
)

var (
	dbDsn                 string
	dbName                string
	tableName             = "nebchain"
	createTableStatements = []string{
		`CREATE TABLE IF NOT EXISTS nebchain (
			nkey varbinary(256) NOT NULL,
			nvalue blob NOT NULL,
			PRIMARY KEY (nkey)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8;`,
	}
)

// MysqlStorage the nodes in trie.
type MysqlStorage struct {
	db    *sql.DB
	cache *lru.Cache
}

// NewMysqlStorage init a storage
func NewMysqlStorage(dsn, database string) (*MysqlStorage, error) {
	dbDsn = dsn
	dbName = database
	cache, err := lru.New(40960)
	if err != nil {
		return nil, err
	}

	if err := ensureDBExists(); err != nil {
		return nil, err
	}

	db, err := sql.Open("mysql", dbDsn+dbName)
	db.SetMaxOpenConns(500)
	db.SetMaxIdleConns(200)

	if err != nil {
		return nil, err
	}
	return &MysqlStorage{
		db:    db,
		cache: cache,
	}, nil
}

func ensureDBExists() error {

	conn, err := sql.Open("mysql", dbDsn)
	if err != nil {
		return err
	}
	defer conn.Close()

	if _, err := conn.Exec("USE " + dbName); err != nil {
		// MySQL error 1049 is "database does not exist"
		if mErr, ok := err.(*mysql.MySQLError); ok && mErr.Number == 1049 {
			return createDBTable(conn)
		}

		// Unknown error.
		return err
	}

	if _, err := conn.Exec("DESCRIBE " + tableName); err != nil {
		// MySQL error 1146 is "table does not exist"
		if mErr, ok := err.(*mysql.MySQLError); ok && mErr.Number == 1146 {
			return createDBTable(conn)
		}
		// Unknown error.
		return err
	}

	return nil
}

// createTable creates the table, and if necessary, the database.
func createDBTable(conn *sql.DB) error {

	var sqls = []string{
		`CREATE DATABASE IF NOT EXISTS ` + dbName + ` DEFAULT CHARACTER SET = 'utf8' DEFAULT COLLATE 'utf8_general_ci';`,
		`USE ` + dbName + `;`,
	}

	sqls = append(sqls, createTableStatements...)

	logging.CLog().WithFields(logrus.Fields{
		"sql": sqls,
	}).Info("create DB sql.")
	for _, stmt := range sqls {
		_, err := conn.Exec(stmt)
		if err != nil {
			return err
		}
	}
	return nil
}

// Get return value to the key in Storage
func (storage *MysqlStorage) Get(key []byte) ([]byte, error) {
	if value, exist := storage.cache.Get(byteutils.Hex(key)); exist {
		return value.([]byte), nil
	}

	var value []byte
	err := storage.db.QueryRow("SELECT `nvalue` FROM "+tableName+" WHERE `nkey` = ? ", key).Scan(&value)
	if err != nil && err == sql.ErrNoRows {
		return nil, ErrKeyNotFound
	}
	return value, err
}

// Put put the key-value entry to Storage
func (storage *MysqlStorage) Put(key []byte, value []byte) error {

	// del
	_, err := storage.db.Exec("INSERT INTO "+tableName+" (`nkey`, `nvalue`) VALUES ( ? , ? ) on  DUPLICATE key update `nvalue` = ? ", key, value, value)
	if err != nil {
		return err
	}

	storage.cache.Add(byteutils.Hex(key), value)
	return nil
}

// Del delete the key in Storage.
func (storage *MysqlStorage) Del(key []byte) error {

	if _, err := storage.db.Exec("DELETE FROM "+tableName+" WHERE `nkey`  = ? ", key); err != nil {
		return err
	}
	storage.cache.Remove(byteutils.Hex(key))
	return nil
}

// Close levelDB
func (storage *MysqlStorage) Close() error {
	return storage.db.Close()
}
