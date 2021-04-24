package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/KimMachineGun/bloomfilterd/internal/bsm"
	"github.com/KimMachineGun/flago"
	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/logger"
	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/lni/goutils/syncutil"
)

const (
	clusterID uint64 = 128
)

func init() {
	logger.GetLogger("raft").SetLevel(logger.ERROR)
	logger.GetLogger("rsm").SetLevel(logger.WARNING)
	logger.GetLogger("transport").SetLevel(logger.WARNING)
	logger.GetLogger("grpc").SetLevel(logger.WARNING)
}

func main() {
	c := DefaultConfig
	flago.Bind(flag.CommandLine, &c)
	flag.Parse()

	err := c.Validate()
	if err != nil {
		log.Fatalln(err)
	}

	datadir := filepath.Join("example-data", "helloworld-data", fmt.Sprintf("node%d", c.NodeID))
	nh, err := dragonboat.NewNodeHost(config.NodeHostConfig{
		WALDir:         datadir,
		NodeHostDir:    datadir,
		RTTMillisecond: 200,
		RaftAddress:    c.Addr,
	})
	if err != nil {
		log.Fatalln(err)
	}

	// As a summary, when -
	//  - starting a brand new Raft cluster, set join to false and specify all initial
	//    member node details in the initialMembers map.
	//  - joining a new node to an existing Raft cluster, set join to true and leave
	//    the initialMembers map empty. This requires the joining node to have already
	//    been added as a member node of the Raft cluster.
	//  - restarting an crashed or stopped node, set join to false and leave the
	//    initialMembers map to be empty. This applies to both initial member nodes
	//    and those joined later.
	err = nh.StartCluster(c.InitialMembers, c.Join, bsm.NewBuilder(c.N, c.P).BuildBSM, config.Config{
		NodeID:             c.NodeID,
		ClusterID:          clusterID,
		ElectionRTT:        10,
		HeartbeatRTT:       1,
		CheckQuorum:        true,
		SnapshotEntries:    10,
		CompactionOverhead: 5,
	})
	if err != nil {
		log.Fatalln("failed to add cluster:", err)
	}

	raftStopper := syncutil.NewStopper()
	consoleStopper := syncutil.NewStopper()
	ch := make(chan string, 16)
	consoleStopper.RunWorker(func() {
		reader := bufio.NewReader(os.Stdin)
		for {
			s, err := reader.ReadString('\n')
			if err != nil {
				close(ch)
				return
			}
			if s == "exit\n" {
				raftStopper.Stop()
				nh.Stop()
				return
			}
			ch <- strings.TrimSuffix(s, "\n")
		}
	})
	raftStopper.RunWorker(func() {
		for {
			select {
			case v := <-ch:
				vs := strings.SplitN(v, " ", 2)
				if len(vs) != 2 {
					log.Printf("invalid command: %v", vs)
					return
				}
				switch strings.ToLower(vs[0]) {
				case "get":
					ctx, _ := context.WithTimeout(context.TODO(), c.Timeout)
					v, err := nh.SyncRead(ctx, clusterID, []byte(vs[1]))
					if err != nil {
						log.Printf("cannot get %s: %v", vs[1], err)
						continue
					}
					res := v.(sm.Result)
					fmt.Printf("GET(%s):%v\n", vs[1], res.Value)
				case "set":
					ctx, _ := context.WithTimeout(context.TODO(), c.Timeout)
					res, err := nh.SyncPropose(ctx, nh.GetNoOPSession(clusterID), []byte(vs[1]))
					if err != nil {
						log.Printf("cannot set %s: %v", vs[1], err)
						continue
					}
					fmt.Printf("SET(%s):%v\n", vs[1], res.Value)
				}
			case <-raftStopper.ShouldStop():
				return
			}
		}
	})
	raftStopper.Wait()
}
