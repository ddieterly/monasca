// persister project main.go
package main

import (
	l4g "code.google.com/p/log4go"
	"encoding/json"
	influxdbClient "github.com/influxdb/influxdb/client"
	kafkaClient "github.com/stealthly/go_kafka_client"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	consumerConfigFileName  string = "go_kafka_client_consumer.properties"
	persisterConfigFileName string = "persister.properties"
	persisterLogFileName    string = "persister.log"
)

type serieMapType map[string][][]interface{}

var persisterConfig persisterConfigType

func init() {
	readJSONConfigFile(persisterConfigFileName, &persisterConfig)
}

func main() {

	setUpLogging(&persisterConfig)

	config, topic, _, _, _ := resolveConfig(consumerConfigFileName)

	messageChannel := make(chan *kafkaClient.Message, 1000)

	config.Strategy = func(_ *kafkaClient.Worker, msg *kafkaClient.Message, id kafkaClient.TaskId) kafkaClient.WorkerResult {
		messageChannel <- msg

		return kafkaClient.NewSuccessfulResult(id)
	}

	config.WorkerFailureCallback = FailedCallback
	config.WorkerFailedAttemptCallback = FailedAttemptCallback

	consumer := kafkaClient.NewConsumer(config)

	topics := map[string]int{topic: config.NumConsumerFetchers}
	go func() {
		consumer.StartStatic(topics)
	}()

	influxdbClientConfig := influxdbClient.ClientConfig{
		Username:   persisterConfig.InfluxdbConfig.Username,
		Password:   persisterConfig.InfluxdbConfig.Password,
		Database:   persisterConfig.InfluxdbConfig.Database,
		Host:       persisterConfig.InfluxdbConfig.Host,
		HttpClient: http.DefaultClient,
	}

	influxdbClientClient, err := influxdbClient.NewClient(&influxdbClientConfig)
	if err != nil {
		panic(err)
	}

	serieMap := make(serieMapType)
	var count int64

	for {

		msg := <-messageChannel

		serieName, value, timeStamp := parseMetric(msg)

		serieMap[serieName] = append(serieMap[serieName], []interface{}{strconv.FormatFloat(value, 'f', -1, 64), timeStamp})

		if count++; count >= persisterConfig.InfluxdbConfig.BatchSize {

			writePointsToInfluxdb(serieMap, influxdbClientClient)
			count = 0
			serieMap = make(serieMapType)
		}
	}
}

func setUpLogging(persisterConfig *persisterConfigType) {

	switch strings.ToLower(persisterConfig.LoggingConfig.Level) {
	case "finest":
		l4g.AddFilter("file", l4g.FINEST, l4g.NewFileLogWriter(persisterLogFileName, false))
	case "fine":
		l4g.AddFilter("file", l4g.FINE, l4g.NewFileLogWriter(persisterLogFileName, false))
	case "debug":
		l4g.AddFilter("file", l4g.DEBUG, l4g.NewFileLogWriter(persisterLogFileName, false))
	case "trace":
		l4g.AddFilter("file", l4g.TRACE, l4g.NewFileLogWriter(persisterLogFileName, false))
	case "info":
		l4g.AddFilter("file", l4g.INFO, l4g.NewFileLogWriter(persisterLogFileName, false))
	case "warning":
		l4g.AddFilter("file", l4g.WARNING, l4g.NewFileLogWriter(persisterLogFileName, false))
	case "error":
		l4g.AddFilter("file", l4g.ERROR, l4g.NewFileLogWriter(persisterLogFileName, false))
	case "critical":
		l4g.AddFilter("file", l4g.CRITICAL, l4g.NewFileLogWriter(persisterLogFileName, false))
	default:
		panic("No valid logging level (FINEST, FINE, DEBUG, TRACE, INFO, WARNING, ERROR, CRITICAL) specified in properties file")
	}

	l4g.Info("Logging level: %s", persisterConfig.LoggingConfig.Level)
}

func parseMetric(metricMsg *kafkaClient.Message) (string, float64, float64) {

	var metricDataMap map[string]interface{}
	if err := json.Unmarshal(metricMsg.Value, &metricDataMap); err != nil {
		l4g.Error(err)
	}

	metric := metricDataMap["metric"].(map[string]interface{})
	name := metric["name"].(string)
	dimensions := metric["dimensions"].(map[string]interface{})
	timeStamp := metric["timestamp"].(float64)
	value := metric["value"].(float64)
	meta := metricDataMap["meta"].(map[string]interface{})
	tenantId := meta["tenantId"].(string)
	region := meta["region"].(string)

	serieName := tenantId + "?" + region + "&" + url.QueryEscape(name)

	for key, val := range dimensions {
		serieName += "&" + url.QueryEscape(key) + "=" + url.QueryEscape(val.(string))
	}

	return serieName, value, timeStamp

}

func writePointsToInfluxdb(serieMap serieMapType, influxdbClientClient *influxdbClient.Client) {

	series := make([]*influxdbClient.Series, 0)

	for serieName, seriesPoints := range serieMap {

		serie := &influxdbClient.Series{
			Name:    serieName,
			Columns: []string{"value", "time"},
			Points:  seriesPoints,
		}

		series = append(series, serie)

	}

	if err := influxdbClientClient.WriteSeries(series); err != nil {
		panic(err)
	}

}

func resolveConfig(config_file_name string) (*kafkaClient.ConsumerConfig, string, int, string, time.Duration) {
	rawConfig, err := kafkaClient.LoadConfiguration(config_file_name)
	if err != nil {
		panic("Failed to load configuration file " + config_file_name)
	}
	numConsumers, _ := strconv.Atoi(rawConfig["num_consumers"])
	zkTimeout, _ := time.ParseDuration(rawConfig["zookeeper_timeout"])

	numWorkers, _ := strconv.Atoi(rawConfig["num_workers"])
	maxWorkerRetries, _ := strconv.Atoi(rawConfig["max_worker_retries"])
	workerBackoff, _ := time.ParseDuration(rawConfig["worker_backoff"])
	workerRetryThreshold, _ := strconv.Atoi(rawConfig["worker_retry_threshold"])
	workerConsideredFailedTimeWindow, _ := time.ParseDuration(rawConfig["worker_considered_failed_time_window"])
	workerTaskTimeout, _ := time.ParseDuration(rawConfig["worker_task_timeout"])
	workerManagersStopTimeout, _ := time.ParseDuration(rawConfig["worker_managers_stop_timeout"])

	rebalanceBarrierTimeout, _ := time.ParseDuration(rawConfig["rebalance_barrier_timeout"])
	rebalanceMaxRetries, _ := strconv.Atoi(rawConfig["rebalance_max_retries"])
	rebalanceBackoff, _ := time.ParseDuration(rawConfig["rebalance_backoff"])
	partitionAssignmentStrategy, _ := rawConfig["partition_assignment_strategy"]
	excludeInternalTopics, _ := strconv.ParseBool(rawConfig["exclude_internal_topics"])

	numConsumerFetchers, _ := strconv.Atoi(rawConfig["num_consumer_fetchers"])
	fetchBatchSize, _ := strconv.Atoi(rawConfig["fetch_batch_size"])
	fetchMessageMaxBytes, _ := strconv.Atoi(rawConfig["fetch_message_max_bytes"])
	fetchMinBytes, _ := strconv.Atoi(rawConfig["fetch_min_bytes"])
	fetchBatchTimeout, _ := time.ParseDuration(rawConfig["fetch_batch_timeout"])
	requeueAskNextBackoff, _ := time.ParseDuration(rawConfig["requeue_ask_next_backoff"])
	fetchWaitMaxMs, _ := strconv.Atoi(rawConfig["fetch_wait_max_ms"])
	socketTimeout, _ := time.ParseDuration(rawConfig["socket_timeout"])
	queuedMaxMessages, _ := strconv.Atoi(rawConfig["queued_max_messages"])
	refreshLeaderBackoff, _ := time.ParseDuration(rawConfig["refresh_leader_backoff"])
	fetchMetadataRetries, _ := strconv.Atoi(rawConfig["fetch_metadata_retries"])
	fetchMetadataBackoff, _ := time.ParseDuration(rawConfig["fetch_metadata_backoff"])

	time.ParseDuration(rawConfig["fetch_metadata_backoff"])

	offsetsCommitMaxRetries, _ := strconv.Atoi(rawConfig["offsets_commit_max_retries"])

	flushInterval, _ := time.ParseDuration(rawConfig["flush_interval"])
	deploymentTimeout, _ := time.ParseDuration(rawConfig["deployment_timeout"])

	zkConfig := kafkaClient.NewZookeeperConfig()
	zkConfig.ZookeeperConnect = []string{rawConfig["zookeeper_connect"]}
	zkConfig.ZookeeperTimeout = zkTimeout

	config := kafkaClient.DefaultConsumerConfig()
	config.Groupid = rawConfig["group_id"]
	config.NumWorkers = numWorkers
	config.MaxWorkerRetries = maxWorkerRetries
	config.WorkerBackoff = workerBackoff
	config.WorkerRetryThreshold = int32(workerRetryThreshold)
	config.WorkerThresholdTimeWindow = workerConsideredFailedTimeWindow
	config.WorkerTaskTimeout = workerTaskTimeout
	config.WorkerManagersStopTimeout = workerManagersStopTimeout
	config.RebalanceBarrierTimeout = rebalanceBarrierTimeout
	config.RebalanceMaxRetries = int32(rebalanceMaxRetries)
	config.RebalanceBackoff = rebalanceBackoff
	config.PartitionAssignmentStrategy = partitionAssignmentStrategy
	config.ExcludeInternalTopics = excludeInternalTopics
	config.NumConsumerFetchers = numConsumerFetchers
	config.FetchBatchSize = fetchBatchSize
	config.FetchMessageMaxBytes = int32(fetchMessageMaxBytes)
	config.FetchMinBytes = int32(fetchMinBytes)
	config.FetchBatchTimeout = fetchBatchTimeout
	config.FetchTopicMetadataRetries = fetchMetadataRetries
	config.FetchTopicMetadataBackoff = fetchMetadataBackoff
	config.RequeueAskNextBackoff = requeueAskNextBackoff
	config.FetchWaitMaxMs = int32(fetchWaitMaxMs)
	config.SocketTimeout = socketTimeout
	config.QueuedMaxMessages = int32(queuedMaxMessages)
	config.RefreshLeaderBackoff = refreshLeaderBackoff
	config.Coordinator = kafkaClient.NewZookeeperCoordinator(zkConfig)
	config.OffsetsStorage = rawConfig["offsets_storage"]
	config.AutoOffsetReset = rawConfig["auto_offset_reset"]
	config.OffsetsCommitMaxRetries = offsetsCommitMaxRetries
	config.DeploymentTimeout = deploymentTimeout

	return config, rawConfig["topic"], numConsumers, rawConfig["graphite_connect"], flushInterval
}

func FailedCallback(wm *kafkaClient.WorkerManager) kafkaClient.FailedDecision {
	kafkaClient.Info("main", "Failed callback")

	return kafkaClient.DoNotCommitOffsetAndStop
}

func FailedAttemptCallback(task *kafkaClient.Task, result kafkaClient.WorkerResult) kafkaClient.FailedDecision {
	kafkaClient.Info("main", "Failed attempt")

	return kafkaClient.CommitOffsetAndContinue
}
