# bloomfilterd (WIP)
bloomfilterd is a 'distributed [bloom filter](https://en.wikipedia.org/wiki/Bloom_filter)' for real-time deduplication at scale.
It replicates the data based on the raft consensus algorithm.

## Usage
Let's create the first node.

```sh
bfd -http=:10001 -n=4000 -p=0.0000001 -id=node1 -addr=:11001 -dir=node1
```

The first node(node1) will bootstrap the cluster.

Below command will create two more nodes, and they will join the cluster.

```sh
bfd -http=:10002 -join=:10001 -n=4000 -p=0.0000001 -id=node2 -addr=:11002 -dir=node2
bfd -http=:10003 -join=:10001 -n=4000 -p=0.0000001 -id=node3 -addr=:11003 -dir=node3
```

You can check if the key exists in the filter through the request below.
(It will return `true` or `false`.)

```sh
curl -X GET http://localhost:10001/key/{KEY}
```

You can set the key to the filter through the request below.
(It will return `true` or `false`.)

```sh
curl -X POST http://localhost:10001/key/{KEY}
```
