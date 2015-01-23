// persister project main.go
package main

import (
	"encoding/json"
	influxdbClient "github.com/influxdb/influxdb/client"
	kafkaClient "github.com/stealthly/go_kafka_client"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

const (
	consumer_config_file_name string = "go_kafka_client_consumer.properties"
)

func main() {

	config, topic, _, _, _ := resolveConfig(consumer_config_file_name)

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
		Username:   "root",
		Password:   "root",
		Database:   "test",
		Host:       "192.168.10.4:8086",
		HttpClient: http.DefaultClient,
	}

	influxdbClientClient, err := influxdbClient.NewClient(&influxdbClientConfig)
	if err != nil {
		panic(err)
	}

	serieMap := make(map[string][][]interface{})
	var count int64

	for {

		msg := <-messageChannel

		var data map[string]interface{}
		if err := json.Unmarshal(msg.Value, &data); err != nil {
			panic(err)
		}

		metric := data["metric"].(map[string]interface{})
		name := metric["name"].(string)
		dimensions := metric["dimensions"].(map[string]interface{})
		timeStamp := metric["timestamp"].(float64)
		value := metric["value"].(float64)
		meta := data["meta"].(map[string]interface{})
		tenantId := meta["tenantId"].(string)
		region := meta["region"].(string)

		serieName := tenantId + "?" + region + "&" + url.QueryEscape(name)

		for key, val := range dimensions {
			serieName += "&" + url.QueryEscape(key) + "=" + url.QueryEscape(val.(string))
		}

		if serieMap[serieName] == nil {
			serieMap[serieName] = make([][]interface{}, 0, 100)
		}

		serieMap[serieName] = append(serieMap[serieName], []interface{}{strconv.FormatFloat(value, 'f', -1, 64), timeStamp})

		count++

		if count >= 100 {

			count = 0

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

			serieMap = make(map[string][][]interface{})
		}

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
