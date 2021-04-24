package main

import (
	"fmt"
	"strconv"
	"strings"
)

type StringsFlag []string

func (f *StringsFlag) String() string {
	return strings.Join(*f, ",")
}

func (f *StringsFlag) Set(s string) error {
	*f = strings.Split(s, ",")
	return nil
}

type MapFlag map[uint64]string

func (m MapFlag) String() string {
	kvs := make([]string, 0, len(m))
	for k, v := range m {
		kvs = append(kvs, fmt.Sprintf("%d=%s", k, v))
	}
	return strings.Join(kvs, ",")
}

func (m MapFlag) Set(s string) error {
	kvs := strings.Split(s, ",")
	for _, _kv := range kvs {
		kv := strings.SplitN(_kv, "=", 2)
		if len(kv) != 2 {
			return fmt.Errorf("invalid key-value pair: %s", _kv)
		}
		k, err := strconv.ParseUint(kv[0], 10, 64)
		if err != nil {
			return fmt.Errorf("invalid key: %s", kv[0])
		}
		m[k] = kv[1]
	}
	return nil
}
