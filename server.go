package main

import (
	"io"
	"github.com/Ryan-Ficklin/distributed_btc_simulation/shared"
	"net/http"
	"net/rpc"
)

func main() {
	// create a Membership list
	nodes := shared.NewMembership()
	requests := shared.NewRequests()
  ballots := shared.NewBallots()

	// register nodes with `rpc.DefaultServer`
	rpc.Register(nodes)
	rpc.Register(requests)
  rpc.Register(ballots)

	// register an HTTP handler for RPC communication
	rpc.HandleHTTP()

	// sample test endpoint
	http.HandleFunc("/", func(res http.ResponseWriter, req *http.Request) {
		io.WriteString(res, "RPC SERVER LIVE!")
	})

	// listen and serve default HTTP server
	http.ListenAndServe("localhost:9005", nil)
}
