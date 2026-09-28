package main

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"
	admin "github.com/amigoer/rocketmq-admin-go"
	"github.com/amigoer/rocketmq-admin-go/protocol/remoting"
	"sort"
	"strconv"
	"strings"
	"time"
)

func studioMessage(m map[string]any) map[string]any {
	props := nestedMap(m, "properties")
	return map[string]any{"msgId": valueOrDefault(stringValue(props, "UNIQ_KEY"), stringValue(m, "msgId", "offsetMsgId")), "topic": m["topic"], "tag": props["TAGS"], "key": props["KEYS"], "keys": props["KEYS"], "brokerName": m["brokerName"], "queueId": m["queueId"], "queueOffset": m["queueOffset"], "offset": m["queueOffset"], "body": m["messageBody"], "bodyBase64": base64.StdEncoding.EncodeToString([]byte(stringValue(m, "messageBody"))), "storeTime": m["storeTimestamp"], "bornHost": m["bornHost"], "storeHost": m["storeHost"], "reconsumeTimes": m["reconsumeTimes"], "properties": props, "size": m["storeSize"]}
}
func (s *session) studioQueryMessages(ctx context.Context, c *admin.Client, p map[string]any) ([]any, error) {
	topic := stringValue(p, "topic")
	if topic == "" {
		return nil, fmt.Errorf("topic is required")
	}
	begin, end := int64Value(p, 0, "startTime"), int64Value(p, time.Now().UnixMilli(), "endTime")
	if begin > end {
		return nil, fmt.Errorf("invalid time range")
	}
	var raw any
	var err error
	switch {
	case stringValue(p, "msgId") != "":
		m, e := s.dashboardView(ctx, c, topic, stringValue(p, "msgId"))
		if e != nil {
			return nil, e
		}
		raw = []any{messageView(m)}
	case stringValue(p, "key") != "":
		m, e := queryMessagesByKey(ctx, c, s.connection.ConnectTimeout, topic, stringValue(p, "key"), 1024, begin, end)
		if e != nil {
			return nil, e
		}
		raw = messageViews(m)
	default:
		raw, err = s.dashboardMessagePage(ctx, c, "/message/queryMessageByTopic.query", map[string]any{"topic": topic, "begin": float64(begin), "end": float64(end)})
		if err != nil {
			return nil, err
		}
	}
	rows := []any{}
	tag := stringValue(p, "tag")
	for _, m := range studioRows(raw) {
		row := studioMessage(m)
		if tag != "" && tag != "*" && stringValue(row, "tag") != tag {
			continue
		}
		rows = append(rows, row)
	}
	return rows, nil
}
func (s *session) studioMessages(ctx context.Context, c *admin.Client, path string, p map[string]any) (any, error) {
	switch path {
	case "/messages", "/messages/page":
		rows, e := s.studioQueryMessages(ctx, c, p)
		if e != nil {
			return nil, e
		}
		if path == "/messages/page" {
			return studioPage(rows, p), nil
		}
		return rows, nil
	case "/messages/direct-consume":
		return s.dashboardConsumeDirect(ctx, c, p)
	case "/messages/queues":
		stats, e := s.examineTopicStats(ctx, c, stringValue(p, "topic"))
		if e != nil {
			return nil, e
		}
		rows := []any{}
		for _, key := range sortedKeys(stats) {
			q := parseMessageQueueKey(key)
			v := stats[key]
			rows = append(rows, map[string]any{"brokerName": q.BrokerName, "queueId": q.QueueID, "minOffset": v.MinOffset, "maxOffset": v.MaxOffset})
		}
		return rows, nil
	case "/messages/queue-message":
		topic := stringValue(p, "topic")
		targets, e := s.messageQueueTargets(ctx, c, topic, false)
		if e != nil {
			return nil, e
		}
		for _, t := range targets {
			if t.BrokerName != stringValue(p, "brokerName") || t.QueueID != intValue(p, -1, "queueId") {
				continue
			}
			r, e := invokeRemotingAllowCodes(ctx, t.Address, s.connection.ConnectTimeout, remoting.NewRequest(remoting.PullMessage, map[string]string{"consumerGroup": "TOOLS_CONSUMER", "topic": topic, "queueId": strconv.Itoa(t.QueueID), "queueOffset": strconv.FormatInt(int64Value(p, -1, "offset"), 10), "maxMsgNums": "1", "sysFlag": "4", "subscription": "*", "expressionType": "TAG", "subVersion": "0", "commitOffset": "0", "suspendTimeoutMillis": "0"}), 0, 19, 20, 21)
			if e != nil {
				return nil, e
			}
			messages := decodeDashboardMessages(r.Body, t.BrokerName)
			if len(messages) == 0 {
				return nil, nil
			}
			return studioMessage(messageView(messages[0])), nil
		}
		return nil, fmt.Errorf("queue not in this connection")
	case "/messages/:id/trace", "/messages/trace-by-key":
		return s.studioTrace(ctx, c, p)
	case "/dlq":
		topics, e := c.FetchAllTopicList(ctx)
		if e != nil {
			return nil, e
		}
		rows := []any{}
		sort.Strings(topics.TopicList)
		for _, topic := range topics.TopicList {
			if !strings.HasPrefix(topic, "%DLQ%") {
				continue
			}
			group := strings.TrimPrefix(topic, "%DLQ%")
			if !studioMatches(group, p) {
				continue
			}
			stats, e := s.examineTopicStats(ctx, c, topic)
			if e != nil {
				return nil, e
			}
			var count, last int64
			for _, v := range stats {
				count += maxValue(int64(0), v.MaxOffset-v.MinOffset)
				last = maxValue(last, v.LastUpdateTimestamp)
			}
			rows = append(rows, map[string]any{"groupName": group, "dlqTopic": topic, "messageCount": count, "lastEnqueueTime": studioDate(last), "retryCount": nil, "status": map[bool]string{true: "pending", false: "empty"}[count > 0], "statsAvailable": true})
		}
		return studioPage(rows, p), nil
	case "/dlq/:id/messages", "/dlq/export", "/dlq/export-excel", "/dlq/resend", "/dlq/resend-selected":
		group := stringValue(p, "id", "groupName")
		if group == "" {
			return nil, fmt.Errorf("groupName is required")
		}
		p["topic"] = "%DLQ%" + group
		rows := []any{}
		ids := stringSlice(p["msgIds"])
		if idsText := stringValue(p, "msgIds"); idsText != "" {
			ids = strings.Split(idsText, ",")
		}
		if len(ids) > 0 {
			if len(ids) > 1000 {
				return nil, fmt.Errorf("select at most 1000 messages")
			}
			for _, id := range ids {
				m, e := s.dashboardView(ctx, c, stringValue(p, "topic"), id)
				if e != nil {
					return nil, e
				}
				rows = append(rows, studioMessage(messageView(m)))
			}
		} else {
			var e error
			rows, e = s.studioQueryMessages(ctx, c, p)
			if e != nil {
				return nil, e
			}
		}
		if path == "/dlq/:id/messages" {
			return studioPage(rows, p), nil
		}
		if strings.HasPrefix(path, "/dlq/export") {
			if len(rows) > 10000 {
				return nil, fmt.Errorf("export exceeds 10000 messages; narrow time range")
			}
			if path == "/dlq/export-excel" {
				return studioXLSX(rows)
			}
			b, e := json.MarshalIndent(rows, "", "  ")
			return string(b), e
		}
		resent := 0
		failures := []string{}
		for _, raw := range rows {
			m := raw.(map[string]any)
			props := nestedMap(m, "properties")
			topic := stringValue(p, "targetTopic")
			if topic == "" {
				topic = stringValue(props, "RETRY_TOPIC", "REAL_TOPIC")
			}
			if topic == "" || strings.HasPrefix(topic, "%DLQ%") || strings.HasPrefix(topic, "%RETRY%") {
				failures = append(failures, stringValue(m, "msgId")+": original topic missing")
				continue
			}
			cleanProps := map[string]any{}
			for k, v := range props {
				if k != "UNIQ_KEY" && k != "RETRY_TOPIC" && k != "REAL_TOPIC" && k != "RECONSUME_TIME" && k != "MAX_RECONSUME_TIMES" && k != "TRAN_MSG" {
					cleanProps[k] = v
				}
			}
			_, e := s.dashboardSend(ctx, c, map[string]any{"topic": topic, "messageBody": m["body"], "key": m["key"], "tag": m["tag"], "properties": cleanProps})
			if e != nil {
				failures = append(failures, stringValue(m, "msgId")+": "+e.Error())
			} else {
				resent++
			}
		}
		outcome := "SUCCESS"
		if len(rows) == 0 {
			outcome = "NO_MESSAGES"
		} else if len(failures) > 0 {
			outcome = "PARTIAL"
			if resent == 0 {
				outcome = "FAILED"
			}
		}
		return map[string]any{"matched": len(rows), "resent": resent, "failed": len(failures), "outcome": outcome, "scanIncomplete": false, "failedQueueCount": 0, "failures": failures}, nil
	}
	return nil, fmt.Errorf("unhandled Studio message endpoint %s", path)
}
func (s *session) studioTrace(ctx context.Context, c *admin.Client, p map[string]any) (any, error) {
	id := stringValue(p, "id", "key")
	if id == "" {
		return nil, fmt.Errorf("message ID or key required")
	}
	traceTopic := valueOrDefault(stringValue(p, "traceTopic"), "RMQ_SYS_TRACE_TOPIC")
	messages, e := queryMessagesByKey(ctx, c, s.connection.ConnectTimeout, traceTopic, id, 1024, 0, time.Now().UnixMilli())
	if e != nil {
		return nil, e
	}
	graph := buildTraceGraph(messages, id)
	views := studioRows(graph["messageTraceViews"])
	if len(views) == 0 {
		return nil, nil
	}
	nodes := []any{}
	consumers := []any{}
	for _, v := range views {
		status := "finish"
		if stringValue(v, "status") == "failed" {
			status = "error"
		}
		nodes = append(nodes, map[string]any{"title": stringValue(v, "traceType") + " " + stringValue(v, "groupName"), "timestamp": v["timeStamp"], "costTime": v["costTime"], "status": status, "description": stringValue(v, "clientHost")})
		if stringValue(v, "traceType") == "SubAfter" {
			consumers = append(consumers, map[string]any{"group": v["groupName"], "deliveryStatus": status, "consumeTime": v["costTime"], "retryCount": v["retryTimes"]})
		}
	}
	return map[string]any{"nodes": nodes, "consumerStatus": consumers}, nil
}

// Excel exports use typed inline strings: message bodies never become formulas.
func studioXLSX(rows []any) (any, error) {
	var buffer bytes.Buffer
	z := zip.NewWriter(&buffer)
	files := map[string]string{
		"[Content_Types].xml":        `<?xml version="1.0"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/></Types>`,
		"_rels/.rels":                `<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>`,
		"xl/workbook.xml":            `<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="DLQ" sheetId="1" r:id="rId1"/></sheets></workbook>`,
		"xl/_rels/workbook.xml.rels": `<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/></Relationships>`,
	}
	var sheet strings.Builder
	sheet.WriteString(`<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>`)
	columns := []string{"msgId", "topic", "queueId", "offset", "storeTime", "reconsumeTimes", "keys", "body", "bodyBase64"}
	all := append([]any{map[string]any{}}, rows...)
	for i, raw := range all {
		sheet.WriteString("<row>")
		m := raw.(map[string]any)
		for _, key := range columns {
			value := key
			if i > 0 {
				value = ""
				if m[key] != nil {
					value = fmt.Sprint(m[key])
				}
			}
			if len([]rune(value)) > 32767 {
				z.Close()
				return nil, fmt.Errorf("Excel cell exceeds 32767 characters; use JSON export")
			}
			sheet.WriteString(`<c t="inlineStr"><is><t xml:space="preserve">`)
			var escaped bytes.Buffer
			xml.EscapeText(&escaped, []byte(value))
			sheet.Write(escaped.Bytes())
			sheet.WriteString(`</t></is></c>`)
		}
		sheet.WriteString("</row>")
	}
	sheet.WriteString("</sheetData></worksheet>")
	files["xl/worksheets/sheet1.xml"] = sheet.String()
	for _, name := range sortedMapKeys(files) {
		w, e := z.Create(name)
		if e != nil {
			return nil, e
		}
		if _, e = w.Write([]byte(files[name])); e != nil {
			return nil, e
		}
	}
	if e := z.Close(); e != nil {
		return nil, e
	}
	return map[string]any{"contentBase64": base64.StdEncoding.EncodeToString(buffer.Bytes()), "contentType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"}, nil
}
