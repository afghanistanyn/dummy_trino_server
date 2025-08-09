package main

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "net/http/pprof"

	"github.com/dustin/go-humanize"
	"github.com/gin-contrib/zap"
	"github.com/gin-gonic/gin"
	jsoniter "github.com/json-iterator/go"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	HeaderUa           = "User-Agent"
	HeaderSource       = "X-Trino-Source"
	HeaderClientInfo   = "X-Trino-Client-Info"
	shardCount         = 256
	QueryIDPlaceholder = "_________QUERYID__________"
)

var (
	serverPort, chunkSize                                               int
	externalHost, dataFilePath, completedQueriesLogPath, serverPortHash string
	completedQueriesLogFile                                             *os.File
	completedQueriesLogChan                                             chan map[string]interface{}
	columns                                                             []Column
	chunks                                                              [][][]string
	chunkJSON                                                           []jsoniter.RawMessage
	chunkHashes                                                         []string
	responseCaches                                                      = &ResponseCaches{}
	queryStates                                                         = NewQueryStateMap()
	metrics                                                             = &QueryMetrics{NodesCount: 1}
	startTime                                                           time.Time
	QueryIDDateCache, QueryIDTimeCache                                  string
	logger                                                              *zap.Logger
)

type (
	RespTemplate struct {
		Data       []byte
		QueryIDPos []int
	}
	Column struct {
		Name          string `json:"name"`
		Type          string `json:"type"`
		TypeSignature `json:"typeSignature"`
	}
	TypeSignature struct {
		RawType   string   `json:"rawType"`
		Arguments []string `json:"arguments"`
	}
	QueryState struct {
		ChunkIndex                         int
		State, Client, Query               string
		StartTime, EndTime, LastActiveTime time.Time
	}
	queryShard struct {
		mu sync.RWMutex
		m  map[string]QueryState
	}
	QueryStateMap struct {
		shards [shardCount]queryShard
	}
	QueryMetrics struct {
		NodesCount     uint8
		RunningQueries uint64
		QueuedQueries  uint64
		TotalQueries   uint64
	}
	ResponseCaches struct {
		NewStatementTpl, QueuedStatementTpl RespTemplate
		ExecutingStatementTpls              []RespTemplate
	}
)

func md5sum(data [][]string) string {
	h := md5.New()
	if len(data) == 0 {
		h.Write([]byte("empty"))
	} else {
		for _, col := range data[0] {
			h.Write([]byte(col))
			h.Write([]byte{0})
		}
		h.Write([]byte{1})
		if len(data) > 1 {
			for _, col := range data[len(data)-1] {
				h.Write([]byte(col))
				h.Write([]byte{0})
			}
			h.Write([]byte{1})
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}

func genQueryID() string {
	querySeq := atomic.AddUint64(&metrics.TotalQueries, 1)
	return fmt.Sprintf("%s_%s_%05d_%s", QueryIDDateCache, QueryIDTimeCache, querySeq, serverPortHash)
}

func loadAndChunkCSV(dataFile string, chunkSize int) error {
	if chunkSize <= 0 {
		return fmt.Errorf("invalid chunk size: %d", chunkSize)
	}
	f, err := os.Open(dataFile)
	if err != nil {
		return err
	}
	defer f.Close()
	fi, _ := f.Stat()
	fileSize := humanize.IBytes(uint64(fi.Size()))
	reader := csv.NewReader(f)
	header, err := reader.Read()
	if err != nil {
		return err
	}
	columns = make([]Column, len(header))
	for i, col := range header {
		columns[i] = Column{col, "varchar", TypeSignature{"varchar", nil}}
	}
	var currentChunk [][]string
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		currentChunk = append(currentChunk, row)
		if len(currentChunk) == chunkSize {
			chunks = append(chunks, currentChunk)
			currentChunk = nil
		}
	}
	if len(currentChunk) > 0 {
		chunks = append(chunks, currentChunk)
	}
	chunkHashes = make([]string, len(chunks)+1)
	for i := range chunks {
		chunkHashes[i] = md5sum(chunks[i])
	}
	chunkHashes[len(chunks)] = md5sum(nil)
	logger.Info("Data loaded", zap.String("file", dataFilePath), zap.String("size", fileSize), zap.Int("chunks", len(chunks)), zap.Int("chunk_size", chunkSize))
	return nil
}

func cacheChunkJSON() error {
	chunkJSON = make([]jsoniter.RawMessage, len(chunks))
	for i, chunk := range chunks {
		b, err := jsoniter.Marshal(chunk)
		if err != nil {
			return err
		}
		chunkJSON[i] = b
	}
	return nil
}

func parseQueryClient(c *gin.Context) string {
	for _, h := range []string{HeaderClientInfo, HeaderSource, HeaderUa} {
		if v := c.GetHeader(h); v != "" {
			return v
		}
	}
	return "Unknown"
}

func buildRespTemplate(raw string) RespTemplate {
	data := []byte(raw)
	var qpos []int
	for i := 0; i+len(QueryIDPlaceholder) <= len(data); i++ {
		if string(data[i:i+len(QueryIDPlaceholder)]) == QueryIDPlaceholder {
			qpos = append(qpos, i)
		}
	}
	return RespTemplate{Data: data, QueryIDPos: qpos}
}

func renderResp(tpl RespTemplate, queryID string) []byte {
	b := make([]byte, len(tpl.Data))
	copy(b, tpl.Data)
	for _, pos := range tpl.QueryIDPos {
		copy(b[pos:pos+len(queryID)], queryID)
	}
	return b
}

func buildResponseCaches() error {
	columnsJSON, err := jsoniter.Marshal(columns)
	if err != nil {
		return err
	}
	// NewStatement
	rawNew := fmt.Sprintf(
		`{"id":"%s","infoUri":"http://%s/v1/admin/status/%s","nextUri":"http://%s/v1/statement/queued/%s/%s/1","columns":%s,"stats":{"state":"QUEUED","queued":true,"scheduled":true},"warning":{"message":""}}`,
		QueryIDPlaceholder, externalHost, QueryIDPlaceholder, externalHost, QueryIDPlaceholder, chunkHashes[0], columnsJSON)
	responseCaches.NewStatementTpl = buildRespTemplate(rawNew)
	// QueuedStatement
	rawQueued := fmt.Sprintf(
		`{"id":"%s","infoUri":"http://%s/v1/admin/status/%s","nextUri":"http://%s/v1/statement/executing/%s/%s/0","columns":%s,"stats":{"state":"RUNNING","queued":false,"scheduled":true},"warning":{"message":""}}`,
		QueryIDPlaceholder, externalHost, QueryIDPlaceholder, externalHost, QueryIDPlaceholder, chunkHashes[0], columnsJSON)
	responseCaches.QueuedStatementTpl = buildRespTemplate(rawQueued)
	// ExecutingStatement
	responseCaches.ExecutingStatementTpls = make([]RespTemplate, len(chunks)+1)
	for i := 0; i <= len(chunks); i++ {
		var dataJSON string
		var state, nextUri string
		if i < len(chunks) {
			dataJSON = string(chunkJSON[i])
			state = "RUNNING"
			nextUri = fmt.Sprintf(`"nextUri":"http://%s/v1/statement/executing/%s/%s/%d",`, externalHost, QueryIDPlaceholder, chunkHashes[i+1], i+1)
		} else {
			dataJSON = "[]"
			state = "FINISHED"
		}
		rawExec := fmt.Sprintf(
			`{"id":"%s","infoUri":"http://%s/v1/admin/status/%s",%s"columns":%s,"data":%s,"stats":{"state":"%s","queued":false,"scheduled":true},"warning":{"message":""}}`,
			QueryIDPlaceholder, externalHost, QueryIDPlaceholder, nextUri, columnsJSON, dataJSON, state)
		responseCaches.ExecutingStatementTpls[i] = buildRespTemplate(rawExec)
	}
	return nil
}

func handleNewQuery(c *gin.Context) {
	queryID := genQueryID()
	queryClient := parseQueryClient(c)
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		logger.Error("Failed to read request body", zap.Error(err))
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}
	query := string(body)
	logger.Info("Received new query", zap.String("query_id", queryID), zap.String("client", queryClient), zap.String("query", query))
	qs := QueryState{Client: queryClient, Query: query, State: "QUEUED", StartTime: time.Now(), LastActiveTime: time.Now()}
	queryStates.Set(queryID, qs)
	atomic.AddUint64(&metrics.RunningQueries, 1)
	respBytes := renderResp(responseCaches.NewStatementTpl, queryID)
	c.Data(200, "application/json", respBytes)
}

func handleQueryNextChunk(c *gin.Context) {
	queryID := c.Param("query_id")
	chunkIndex, _ := strconv.Atoi(c.Param("chunk_idx"))
	qs, ok := queryStates.Get(queryID)
	if !ok {
		logger.Warn("Query not found", zap.String("query_id", queryID))
		c.JSON(http.StatusNotFound, gin.H{"error": "query not found"})
		return
	}
	if chunkIndex >= len(chunks) && qs.State != "QUEUED" {
		qs.State, qs.EndTime, qs.ChunkIndex, qs.LastActiveTime = "FINISHED", time.Now(), chunkIndex, time.Now()
		queryStates.Set(queryID, qs)
		atomic.AddUint64(&metrics.RunningQueries, ^uint64(0))
		completedQueriesLogChan <- map[string]interface{}{
			"query_id": queryID, "query": qs.Query, "client": qs.Client,
			"start_time": qs.StartTime.Format(time.RFC3339), "end_time": qs.EndTime.Format(time.RFC3339),
		}
		logger.Info("Query finished", zap.String("query_id", queryID))
		respBytes := renderResp(responseCaches.ExecutingStatementTpls[chunkIndex], queryID)
		c.Data(200, "application/json", respBytes)
		return
	}
	if qs.State == "QUEUED" {
		qs.State, qs.LastActiveTime = "RUNNING", time.Now()
		queryStates.Set(queryID, qs)
		logger.Info("Query state changed to RUNNING", zap.String("query_id", queryID))
		respBytes := renderResp(responseCaches.QueuedStatementTpl, queryID)
		c.Data(200, "application/json", respBytes)
		return
	}
	qs.ChunkIndex, qs.State = chunkIndex+1, "RUNNING"
	queryStates.Set(queryID, qs)
	respBytes := renderResp(responseCaches.ExecutingStatementTpls[chunkIndex], queryID)
	c.Data(200, "application/json", respBytes)
	logger.Info("Query chunk served", zap.String("query_id", queryID), zap.Int("chunk_index", chunkIndex))
}

func QueryManagement(interval, expireAfter, runningExpireAfter time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		for range ticker.C {
			now := time.Now()
			queryStates.Range(func(queryID string, qs QueryState) bool {
				switch {
				case qs.State == "FINISHED" && now.Sub(qs.EndTime) > expireAfter:
					queryStates.Delete(queryID)
					logger.Info("Expired FINISHED query cleaned", zap.String("query_id", queryID))
				case qs.State == "RUNNING" && now.Sub(qs.LastActiveTime) > runningExpireAfter:
					queryStates.Delete(queryID)
					atomic.AddUint64(&metrics.RunningQueries, ^uint64(0))
					logger.Info("Stuck RUNNING query cleaned", zap.String("query_id", queryID))
				}
				return true
			})
		}
	}()
}

func CompletedQueriesLogManagement() error {
	var err error
	completedQueriesLogFile, err = os.OpenFile(completedQueriesLogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	completedQueriesLogChan = make(chan map[string]interface{}, 2000)
	go func() {
		for logData := range completedQueriesLogChan {
			logJSON, _ := jsoniter.Marshal(logData)
			if _, err := completedQueriesLogFile.WriteString(string(logJSON) + "\n"); err != nil {
				logger.Error("Failed to write to completed query log file", zap.Error(err))
			}
		}
	}()
	return nil
}

func QueryIDDateTimeCache() {
	go func() {
		for {
			now := time.Now()
			QueryIDDateCache, QueryIDTimeCache = now.Format("20060102"), now.Format("150405")
			time.Sleep(time.Second - time.Duration(now.Nanosecond())*time.Nanosecond)
		}
	}()
}

func handleQueryStatus(c *gin.Context) {
	queryID := c.Param("query_id")
	qs, ok := queryStates.Get(queryID)
	if !ok {
		logger.Warn("Query not found (status)", zap.String("query_id", queryID))
		c.JSON(http.StatusNotFound, gin.H{"error": "query not found"})
		return
	}
	jsonBytes, _ := jsoniter.Marshal(qs)
	c.Data(http.StatusOK, "application/json", jsonBytes)
}

func handleQueryAllStatus(c *gin.Context) {
	resp := map[string]interface{}{
		"uptime":  time.Since(startTime).String(),
		"queries": queryStates,
		"status":  metrics,
	}
	jsonBytes, _ := jsoniter.Marshal(resp)
	c.Data(http.StatusOK, "application/json", jsonBytes)
}

func NewQueryStateMap() *QueryStateMap {
	qsm := &QueryStateMap{}
	for i := 0; i < shardCount; i++ {
		qsm.shards[i].m = make(map[string]QueryState)
	}
	return qsm
}

func (qsm *QueryStateMap) getShard(key string) *queryShard {
	h := fnv.New32a()
	h.Write([]byte(key))
	return &qsm.shards[uint(h.Sum32())%shardCount]
}
func (qsm *QueryStateMap) Get(key string) (QueryState, bool) {
	shard := qsm.getShard(key)
	shard.mu.RLock()
	v, ok := shard.m[key]
	shard.mu.RUnlock()
	return v, ok
}
func (qsm *QueryStateMap) Set(key string, value QueryState) {
	shard := qsm.getShard(key)
	shard.mu.Lock()
	shard.m[key] = value
	shard.mu.Unlock()
}
func (qsm *QueryStateMap) Range(f func(key string, value QueryState) bool) {
	for i := 0; i < shardCount; i++ {
		shard := &qsm.shards[i]
		shard.mu.RLock()
		for k, v := range shard.m {
			if !f(k, v) {
				shard.mu.RUnlock()
				return
			}
		}
		shard.mu.RUnlock()
	}
}
func (qsm *QueryStateMap) Delete(key string) {
	shard := qsm.getShard(key)
	shard.mu.Lock()
	delete(shard.m, key)
	shard.mu.Unlock()
}

func handleMetrics(c *gin.Context) {
	metricsText := fmt.Sprintf(`# TYPE trino_execution_name_QueryManager_RunningQueries gauge
trino_execution_name_QueryManager_RunningQueries %d

# TYPE trino_execution_name_QueryManager_QueuedQueries gauge
trino_execution_name_QueryManager_QueuedQueries %d

# TYPE trino_metadata_name_DiscoveryNodeManager_ActiveNodeCount gauge
trino_metadata_name_DiscoveryNodeManager_ActiveNodeCount %d

# EOF
`, metrics.RunningQueries, metrics.QueuedQueries, metrics.NodesCount)
	c.Header("Content-Type", "application/openmetrics-text; version=1.0.0; charset=utf-8")
	c.String(http.StatusOK, metricsText)
}

func main() {
	cfg := zap.NewProductionConfig()
	cfg.EncoderConfig.EncodeTime = zapcore.RFC3339TimeEncoder
	logger, _ = cfg.Build()
	defer logger.Sync()

	flag.StringVar(&dataFilePath, "data_file", "./data/lineitem.csv", "data file path")
	flag.IntVar(&chunkSize, "chunk_size", 8*1024*10, "chunk size")
	flag.IntVar(&serverPort, "port", 8080, "server port")
	flag.StringVar(&externalHost, "external_host", "localhost:{port}", "external host of this service")
	flag.StringVar(&completedQueriesLogPath, "completed_queries_log", "logs/completed_queries.log", "completed queries log path")
	flag.Parse()

	externalHost = strings.ReplaceAll(externalHost, "{port}", strconv.Itoa(serverPort))
	externalHost = strings.Replace(externalHost, "http://", "", 1)

	if err := loadAndChunkCSV(dataFilePath, chunkSize); err != nil {
		logger.Fatal("Failed to load CSV", zap.Error(err))
	}
	if err := cacheChunkJSON(); err != nil {
		logger.Fatal("Failed to cache JSON", zap.Error(err))
	}
	if err := CompletedQueriesLogManagement(); err != nil {
		logger.Fatal("Failed to init completed queries log", zap.Error(err))
	}
	if err := buildResponseCaches(); err != nil {
		logger.Fatal("Failed to build response caches", zap.Error(err))
	}
	QueryIDDateTimeCache()
	QueryManagement(30*time.Second, 120*time.Second, 300*time.Second)

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(ginzap.Ginzap(logger, time.RFC3339, true), ginzap.RecoveryWithZap(logger, true))
	r.POST("/v1/statement", handleNewQuery)
	r.GET("/v1/statement/:status(executing|queued)/:query_id/:token/:chunk_idx", handleQueryNextChunk)
	r.GET("/v1/metrics", handleMetrics)
	r.GET("/v1/admin/status/:query_id", handleQueryStatus)
	r.GET("/v1/admin/status", handleQueryAllStatus)

	startTime = time.Now()
	serverPortHash = fmt.Sprintf("%x", sha256.Sum256([]byte(strconv.Itoa(serverPort))))[:4]
	logger.Info("Server starting", zap.Int("port", serverPort))

	go http.ListenAndServe(":6060", nil) // for pprof
	err := r.Run(fmt.Sprintf(":%d", serverPort))
	if err != nil {
		logger.Fatal("Failed to start server", zap.Error(err))
	}
}
