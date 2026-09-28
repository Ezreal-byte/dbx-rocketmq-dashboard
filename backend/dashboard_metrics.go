package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	admin "github.com/amigoer/rocketmq-admin-go"
)

type dashboardHistory struct {
	Broker map[string][]string `json:"broker"`
	Topic  map[string][]string `json:"topic"`
}
type dashboardState struct {
	downloads map[string]*dashboardDownload
	mu        sync.Mutex
	queries   map[string]dashboardQuery
	history   map[string]*dashboardHistory
	monitors  map[string]map[string]any
	directory string
	loadError error
	cancel    context.CancelFunc
	done      chan struct{}
}

func newDashboardState(id string) *dashboardState {
	root := os.Getenv("DBX_PLUGIN_DATA_DIR")
	if root == "" {
		cache, _ := os.UserCacheDir()
		root = filepath.Join(cache, "dbx", pluginID)
	}
	hash := sha256.Sum256([]byte(id))
	dir := filepath.Join(root, "dashboard", fmt.Sprintf("%x", hash[:]))
	state := &dashboardState{queries: map[string]dashboardQuery{}, history: map[string]*dashboardHistory{}, monitors: map[string]map[string]any{}, directory: dir}
	b, err := os.ReadFile(filepath.Join(dir, "monitor.json"))
	if err == nil {
		state.loadError = json.Unmarshal(b, &state.monitors)
	} else if !os.IsNotExist(err) {
		state.loadError = err
	}
	return state
}

func (s *session) startDashboardCollector() {
	d := s.dashboard
	ctx, cancel := context.WithCancel(context.Background())
	d.cancel = cancel
	d.done = make(chan struct{})
	go func() {
		defer close(d.done)
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.collectDashboard(ctx)
			}
		}
	}()
}
func (s *session) stopDashboardCollector() {
	if s.dashboard != nil && s.dashboard.cancel != nil {
		s.dashboard.cancel()
		<-s.dashboard.done
	}
}

func (d *dashboardState) loadHistoryLocked(date string) (*dashboardHistory, error) {
	if _, err := time.Parse("2006-01-02", date); err != nil {
		return nil, fmt.Errorf("invalid date")
	}
	if h := d.history[date]; h != nil {
		return h, nil
	}
	h := &dashboardHistory{Broker: map[string][]string{}, Topic: map[string][]string{}}
	b, err := os.ReadFile(filepath.Join(d.directory, date+".json"))
	if err == nil {
		if err = json.Unmarshal(b, h); err != nil {
			return nil, err
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	d.history[date] = h
	return h, nil
}
func writeDashboardJSON(path string, value any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(path), ".write-*")
	if err != nil {
		return err
	}
	name := temp.Name()
	defer os.Remove(name)
	if _, err = temp.Write(b); err != nil {
		temp.Close()
		return err
	}
	if err = temp.Sync(); err != nil {
		temp.Close()
		return err
	}
	if err = temp.Close(); err != nil {
		return err
	}
	return os.Rename(name, path)
}

func (s *session) dashboardMetrics(ctx context.Context, c *admin.Client, path string, p map[string]any) (any, error) {
	d := s.dashboard
	d.mu.Lock()
	defer d.mu.Unlock()
	if strings.HasPrefix(path, "/monitor/") {
		if d.loadError != nil {
			return nil, fmt.Errorf("monitor configuration cannot be read: %w", d.loadError)
		}
		group := stringValue(p, "consumeGroupName")
		switch path {
		case "/monitor/consumerMonitorConfig.query":
			return jsonObject(d.monitors), nil
		case "/monitor/consumerMonitorConfigByGroupName.query":
			return d.monitors[group], nil
		default:
			if group == "" {
				return nil, fmt.Errorf("consumeGroupName is required")
			}
			next := map[string]map[string]any{}
			for k, v := range d.monitors {
				next[k] = v
			}
			if path == "/monitor/deleteConsumerMonitor.do" {
				delete(next, group)
			} else {
				min := intValue(p, 0, "minCount")
				max := int64Value(p, 0, "maxDiffTotal")
				if min < 0 || max < 0 {
					return nil, fmt.Errorf("monitor thresholds must be nonnegative")
				}
				next[group] = map[string]any{"minCount": min, "maxDiffTotal": max}
			}
			if err := writeDashboardJSON(filepath.Join(d.directory, "monitor.json"), next); err != nil {
				return nil, err
			}
			d.monitors = next
			return true, nil
		}
	}
	date := valueOrDefault(stringValue(p, "date"), time.Now().Format("2006-01-02"))
	history, err := d.loadHistoryLocked(date)
	if err != nil {
		return nil, err
	}
	switch path {
	case "/dashboard/broker.query":
		return jsonObject(history.Broker), nil
	case "/dashboard/topic.query":
		if topic := stringValue(p, "topicName"); topic != "" {
			return append([]string{}, history.Topic[topic]...), nil
		}
		return jsonObject(history.Topic), nil
	case "/dashboard/topicCurrent.query":
		rows := []string{}
		for _, topic := range sortedKeys(history.Topic) {
			values := history.Topic[topic]
			if len(values) > 0 {
				fields := strings.Split(values[len(values)-1], ",")
				if len(fields) == 5 {
					rows = append(rows, topic+","+fields[4])
				}
			}
		}
		return rows, nil
	}
	return nil, fmt.Errorf("unknown metrics endpoint")
}

func (s *session) collectDashboard(parent context.Context) {
	ctx, cancel := context.WithTimeout(parent, 50*time.Second)
	defer cancel()
	c, _, err := s.requireClient()
	if err != nil {
		return
	}
	now := time.Now()
	brokers := map[string]string{}
	topics := map[string]string{}
	info, err := c.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		return
	}
	for name, b := range info.BrokerAddrTable {
		for id, addr := range b.BrokerAddrs {
			kv, e := c.FetchBrokerRuntimeStats(ctx, addr)
			if e != nil {
				continue
			}
			sum := float64(0)
			values := strings.Fields(kv.Table["getTotalTps"])
			if len(values) == 0 {
				continue
			}
			for _, v := range values {
				n, _ := strconv.ParseFloat(v, 64)
				sum += n
			}
			brokers[name+":"+id] = fmt.Sprintf("%d,%.5f", now.UnixMilli(), sum/float64(len(values)))
		}
	}
	list, err := c.FetchAllTopicList(ctx)
	if err == nil {
		for _, topic := range list.TopicList {
			if ctx.Err() != nil {
				break
			}
			route, e := c.ExamineTopicRouteInfo(ctx, topic)
			if e != nil {
				continue
			}
			groups, e := s.dashboardTopicGroups(ctx, c, topic)
			if e != nil {
				continue
			}
			var inTPS, outTPS float64
			var inSum, outSum float64
			valid := false
			for _, b := range route.BrokerDatas {
				addr := b.BrokerAddrs["0"]
				if addr == "" {
					continue
				}
				stats, e := c.ViewBrokerStatsData(ctx, addr, "TOPIC_PUT_NUMS", topic)
				if e == nil {
					obj := jsonObject(stats)
					minute := nestedMap(obj, "statsMinute")
					inTPS += floatValue(minute["tps"])
					inSum += stats24HourSum(obj)
					valid = true
				}
				for _, g := range groups {
					stats, e = c.ViewBrokerStatsData(ctx, addr, "GROUP_GET_NUMS", topic+"@"+g)
					if e == nil {
						obj := jsonObject(stats)
						outTPS += floatValue(nestedMap(obj, "statsMinute")["tps"])
						outSum += stats24HourSum(obj)
						valid = true
					}
				}
			}
			if valid {
				topics[topic] = fmt.Sprintf("%d,%.5f,%.0f,%.5f,%.0f", now.UnixMilli(), inTPS, inSum, outTPS, outSum)
			}
		}
	}
	if ctx.Err() != nil {
		return
	}
	d := s.dashboard
	d.mu.Lock()
	defer d.mu.Unlock()
	date := now.Format("2006-01-02")
	h, err := d.loadHistoryLocked(date)
	if err != nil {
		log.Printf("Dashboard history read failed: %v", err)
		return
	}
	for key, point := range brokers {
		h.Broker[key] = append(h.Broker[key], point)
	}
	for key, point := range topics {
		h.Topic[key] = append(h.Topic[key], point)
	}
	if err = writeDashboardJSON(filepath.Join(d.directory, date+".json"), h); err != nil {
		log.Printf("Dashboard history write failed: %v", err)
	}
	for day := range d.history {
		if day != date {
			delete(d.history, day)
		}
	}
}
func floatValue(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case string:
		n, _ := strconv.ParseFloat(x, 64)
		return n
	}
	return 0
}

func stats24HourSum(stats map[string]any) float64 {
	for _, period := range []string{"statsDay", "statsHour", "statsMinute"} {
		if sum := floatValue(nestedMap(stats, period)["sum"]); sum != 0 {
			return sum
		}
	}
	return 0
}
