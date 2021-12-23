package main

import (
	"errors"
	"time"
)

var (
	DefaultConfig = Config{
		TransportMaxPool: 5,
		TransportTimeout: 10 * time.Second,
		SnapshotRetain:   2,
		ApplyTimeout:     10 * time.Second,
	}
)

type Config struct {
	HTTP     string `flago:"http,Address for communicating with client"`
	JoinAddr string `flago:"join,Leader address of a cluster to join"`

	N         uint64  `flago:"n,number of items in the filter"`
	P         float64 `flago:"p,probability of false positives, fraction between 0 and 1"`
	Retention uint64  `flago:"retention,retention of bloomfilters"`

	NodeID           string        `flago:"id,node id"`
	Addr             string        `flago:"addr,raft bind address"`
	TransportMaxPool int           `flago:"transportMaxPool,max pool of tcp transport"`
	TransportTimeout time.Duration `flago:"transportTimeout,timeout of tcp transport"`
	SnapshotRetain   int           `flago:"snapshotRetain,how many snapshots are retained"`
	Dir              string        `flago:"dir,raft snapshot/log/stable store backup directory"`

	ApplyTimeout time.Duration `flago:"applyTimeout,fsm command timeout"`
}

func (c *Config) Validate() error {
	if c.HTTP == "" {
		return errors.New("invalid HTTP")
	}

	if c.N == 0 {
		return errors.New("invalid N")
	}
	if c.P == 0 {
		return errors.New("invalid P")
	}
	if c.Retention == 0 {
		return errors.New("invalid Retention")
	}

	if c.NodeID == "" {
		return errors.New("invalid NodeID")
	}
	if c.Addr == "" {
		return errors.New("invalid Addr")
	}
	if c.TransportMaxPool == 0 {
		return errors.New("invalid TransportMaxPool")
	}
	if c.TransportTimeout == 0 {
		return errors.New("invalid TransportTimeout")
	}
	if c.SnapshotRetain == 0 {
		return errors.New("invalid SnapshotRetain")
	}
	if c.Dir == "" {
		return errors.New("invalid Dir")
	}

	if c.ApplyTimeout == 0 {
		return errors.New("invalid ApplyTimeout")
	}

	return nil
}
