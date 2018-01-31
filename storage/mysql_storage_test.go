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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewMysqlStorage(t *testing.T) {
	storage, _ := NewMysqlStorage("root:@/nebulas", "nebchain")
	keys := [][]byte{[]byte("key1"), []byte("key2")}
	values := [][]byte{[]byte("value1"), []byte("value2")}
	storage.Put(keys[0], values[0])
	storage.Put(keys[1], values[1])
	value1, err1 := storage.Get(keys[0])
	assert.Nil(t, err1)
	assert.Equal(t, value1, values[0])
	storage.Del(keys[1])
	_, err2 := storage.Get(keys[1])
	assert.NotNil(t, err2)
}
