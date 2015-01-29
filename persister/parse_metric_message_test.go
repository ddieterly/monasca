package main

import (
	"fmt"
	"strconv"
	"testing"
)

func TestParseMetricMsg(t *testing.T) {

	metricMsg := []byte(`{"metric":{"name":"mysql.innodb.buffer_pool_free","dimensions":{"z-dim":"z-val","component":"mysql","service":"mysql","hostname":"devstack"},"timestamp":1422549458,"value":1.0108928E8},"meta":{"tenantId":"361791db7d3e44a2a42e5dee8d384b21","region":"useast"},"creation_time":1422549458}`)

	serieName, value, timeStamp := parseMetric(metricMsg)

	fmt.Println(serieName)

	if serieName != "361791db7d3e44a2a42e5dee8d384b21?useast&mysql.innodb.buffer_pool_free&component=mysql&hostname=devstack&service=mysql&z-dim=z-val" {
		t.Errorf("Failed serieName")
	}

	if value != "101089280" {
		t.Errorf("Failed value")
	}

	if strconv.FormatFloat(timeStamp, 'f', -1, 64) != "1422549458" {
		t.Errorf("Failed timeStamp")
	}
}
