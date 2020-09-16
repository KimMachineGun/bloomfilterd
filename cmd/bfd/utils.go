package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

type JoinMessage struct {
	NodeID string `json:"id"`
	Addr   string `json:"addr"`
}

func JoinCluster(nodeID string, addr string, joinAddr string) error {
	b, err := json.Marshal(JoinMessage{
		NodeID: nodeID,
		Addr:   addr,
	})
	if err != nil {
		return err
	}

	url := fmt.Sprintf("http://%s/join", joinAddr)
	resp, err := http.Post(url, "application-type/json", bytes.NewReader(b))
	if err != nil {
		return fmt.Errorf("cannot join cluster: %v", err)
	}
	defer resp.Body.Close()

	return nil
}
