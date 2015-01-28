package main

import (
	"encoding/json"
	"os"
)

type loggingConfigType struct {
	Level string `json:"level"`
}

type influxdbConfigType struct {
	BatchSize int64  `json:"batch_size"`
	Username  string `json:"username"`
	Password  string `json:"password"`
	Database  string `json:"database"`
	Host      string `json:"host"`
}

type persisterConfigType struct {
	LoggingConfig  loggingConfigType  `json:"logging"`
	InfluxdbConfig influxdbConfigType `json:"influxdb"`
}

func readJSONConfigFile(fileName string, configType interface{}) {

	f, err := os.Open(fileName)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	decoder := json.NewDecoder(f)

	if decoder.Decode(configType) != nil {
		panic(err)
	}

}
