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

package storage

import (
	lru "github.com/hashicorp/golang-lru"
	"github.com/nebulasio/go-nebulas/util/byteutils"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

var (
	chain_table string
)

// MysqlStorage the nodes in trie.
type MysqlStorage struct {
	db    *sql.DB
	cache *lru.Cache
}

// NewMysqlStorage init a storage
func NewMysqlStorage(dsn, table string) (*MysqlStorage, error) {
	chain_table = table
	cache, err := lru.New(40960)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("mysql", dsn)

	if err != nil {
		return nil, err
	}
	return &MysqlStorage{
		db:    db,
		cache: cache,
	}, nil
}

// Get return value to the key in Storage
func (storage *MysqlStorage) Get(key []byte) ([]byte, error) {
	if value, exist := storage.cache.Get(byteutils.Hex(key)); exist {
		return value.([]byte), nil
	}

	//table = "nebchain"
	var value  []byte
	err := storage.db.QueryRow("SELECT `value` FROM nebchain  WHERE `key` = ? ", key).Scan(&value)
	if err != nil && err == sql.ErrNoRows {
		return nil, ErrKeyNotFound
	}
	return value, err
}

// Put put the key-value entry to Storage
func (storage *MysqlStorage) Put(key []byte, value []byte) error {

	// del 
	_, err := storage.Get(key)

	if err == ErrKeyNotFound{ // insert 
		_, err := storage.db.Exec( "INSERT INTO "+ chain_table +" (`key`, `value`) VALUES ( ? , ? )", key, value)
		if (err != nil){
			return err
		}
	}else { //update 
		_ , err := storage.db.Exec( "UPDATE "+ chain_table +" SET `value` = ?  WHERE `key` = ?", value, key)
		if (err != nil){
			return err
		}
	}
	storage.cache.Add(byteutils.Hex(key), value)
	return nil
}

// Del delete the key in Storage.
func (storage *MysqlStorage) Del(key []byte) error {

	if _, err := storage.db.Exec("DELETE FROM "+ chain_table +" WHERE `key`  = ? ", key); err != nil {
		return err
	}
	storage.cache.Remove(byteutils.Hex(key))
	return nil
}

// Close levelDB
func (storage *MysqlStorage) Close() error {
	return storage.db.Close()
}
