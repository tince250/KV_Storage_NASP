package main

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

type defValues struct{
	MemtableSize uint64 `yaml:"memtable_size"`
	WalSize uint64		`yaml:"wal_size"`
	Threshold uint64	`yaml:"threshold"`
	CacheSize uint32	`yaml:"cache_size"`
	LsmLevel uint64 	`yaml:"lsm_level"`
	MaxHeight uint64	`yaml:"max_height"`
	WalThreshold uint64	`yaml:"wal_threshold"`
}

func (defVals *defValues) getDefaultValues(filename string) {
	configData, err := ioutil.ReadFile(filename)
	if err != nil{
		defVals.MemtableSize = 1
		defVals.WalSize = 15
		defVals.Threshold = 15
		defVals.CacheSize = 20
		defVals.LsmLevel = 4
		defVals.MaxHeight = 5
		defVals.WalThreshold = 300
	}else{
		// ukoliko postoji fajl, postaviti vrednosti iz fajla
		err := yaml.Unmarshal(configData, &defVals)
		if err != nil {
			defVals.MemtableSize = 1
			defVals.WalSize = 15
			defVals.Threshold = 15
			defVals.CacheSize = 20
			defVals.LsmLevel = 4
			defVals.MaxHeight = 5
			defVals.WalThreshold = 300
		}
	}

}

