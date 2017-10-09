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

package main

import (
	"log"

	"github.com/nebulasio/go-nebulas/rpc"
	"github.com/nebulasio/go-nebulas/rpc/pb"
	"golang.org/x/net/context"
)

// RPC testing client.
func main() {
	// Set up a connection to the server.
	conn, err := rpc.Dial()
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	ac := rpcpb.NewAPIServiceClient(conn)

	{
		r, err := ac.GetBalance(context.Background(), &rpcpb.GetBalanceRequest{Address: "0x1"})
		if err != nil {
			log.Println("GetBalance failed: ", err)
		} else {
			log.Println("GetBalance respnonse: ", r)
		}
	}

	{
		r, err := ac.SendTransaction(context.Background(), &rpcpb.SendTransactionRequest{From: "0x1", To: "0x2", Value: 1})
		if err != nil {
			log.Println("SendTransaction failed: ", err)
		} else {
			log.Println("SendTransaction response: ", r)
		}
	}

}
