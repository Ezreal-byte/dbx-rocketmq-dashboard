package main

import (
	admin "github.com/amigoer/rocketmq-admin-go"
	"sort"
	"strconv"
	"strings"
)

// Decode the versioned records used by upstream MsgTraceDecodeUtil.
func buildTraceGraph(messages []*admin.MessageExt, id string) map[string]any {
	views := []map[string]any{}
	for _, message := range messages {
		for _, record := range strings.Split(string(message.Body), "\x02") {
			f := strings.Split(record, "\x01")
			if len(f) < 2 {
				continue
			}
			v := map[string]any{"clientHost": message.BornHost, "traceType": f[0], "costTime": int64(-1), "timeStamp": int64(0), "retryTimes": int64(0), "status": "success"}
			n := func(i int) int64 {
				if i >= len(f) {
					return 0
				}
				value, _ := strconv.ParseInt(f[i], 10, 64)
				return value
			}
			status := func(i int) {
				if i < len(f) && f[i] != "true" {
					v["status"] = "failed"
				}
			}
			switch f[0] {
			case "Pub":
				if len(f) < 12 {
					continue
				}
				v["timeStamp"] = n(1)
				v["groupName"] = f[3]
				v["topic"] = f[4]
				v["msgId"] = f[5]
				v["tags"] = f[6]
				v["keys"] = f[7]
				v["storeHost"] = f[8]
				v["costTime"] = n(10)
				v["msgType"] = traceMessageType(n(11))
				if len(f) == 13 {
					status(12)
				}
				if len(f) >= 14 {
					v["offSetMsgId"] = f[12]
					status(13)
				}
			case "SubBefore":
				if len(f) < 8 {
					continue
				}
				v["timeStamp"] = n(1)
				v["groupName"] = f[3]
				v["requestId"] = f[4]
				v["msgId"] = f[5]
				v["retryTimes"] = n(6)
				v["keys"] = f[7]
			case "SubAfter":
				if len(f) < 6 {
					continue
				}
				v["requestId"] = f[1]
				v["msgId"] = f[2]
				v["costTime"] = n(3)
				status(4)
				v["keys"] = f[5]
				if len(f) >= 9 {
					v["timeStamp"] = n(7)
					v["groupName"] = f[8]
				}
			case "EndTransaction":
				if len(f) < 13 {
					continue
				}
				v["timeStamp"] = n(1)
				v["groupName"] = f[3]
				v["topic"] = f[4]
				v["msgId"] = f[5]
				v["tags"] = f[6]
				v["keys"] = f[7]
				v["storeHost"] = f[8]
				v["msgType"] = traceMessageType(n(9))
				v["transactionId"] = f[10]
				v["transactionState"] = f[11]
				v["fromTransactionCheck"] = f[12] == "true"
			default:
				continue
			}
			if v["msgId"] == id {
				views = append(views, v)
			}
		}
	}
	var producer map[string]any
	transactions := []map[string]any{}
	pairs := map[string][2]map[string]any{}
	for _, v := range views {
		switch v["traceType"] {
		case "Pub":
			producer = map[string]any{"traceNode": traceNode(v)}
			for _, key := range []string{"msgId", "tags", "keys", "offSetMsgId", "topic", "groupName"} {
				producer[key] = v[key]
			}
		case "EndTransaction":
			transactions = append(transactions, traceNode(v))
		case "SubBefore", "SubAfter":
			request := stringValue(v, "requestId")
			pair := pairs[request]
			index := 0
			if v["traceType"] == "SubAfter" {
				index = 1
			}
			pair[index] = v
			pairs[request] = pair
		}
	}
	if producer != nil {
		producer["transactionNodeList"] = transactions
	}
	groups := map[string][]map[string]any{}
	for _, pair := range pairs {
		base := pair[0]
		if base == nil {
			base = pair[1]
		}
		node := traceNode(base)
		group := valueOrDefault(stringValue(base, "groupName"), "%UNKNOWN_GROUP%")
		if pair[0] == nil {
			node["beginTimestamp"] = int64(-1)
			node["retryTimes"] = int64(-1)
		}
		if after := pair[1]; after != nil {
			node["costTime"] = after["costTime"]
			node["status"] = after["status"]
			end := int64Value(after, 0, "timeStamp")
			if end == 0 && pair[0] != nil {
				end = int64Value(pair[0], 0, "timeStamp") + maxValue(int64(0), int64Value(after, 0, "costTime"))
			}
			node["endTimestamp"] = end
		} else {
			node["status"] = "unknown"
			node["endTimestamp"] = int64(-1)
			node["costTime"] = int64(-1)
		}
		groups[group] = append(groups[group], node)
	}
	subscriptions := []any{}
	for _, group := range sortedKeys(groups) {
		nodes := groups[group]
		sort.Slice(nodes, func(i, j int) bool {
			return int64Value(nodes[i], 0, "beginTimestamp") < int64Value(nodes[j], 0, "beginTimestamp")
		})
		subscriptions = append(subscriptions, map[string]any{"subscriptionGroup": group, "consumeNodeList": nodes})
	}
	return map[string]any{"producerNode": producer, "subscriptionNodeList": subscriptions, "messageTraceViews": views}
}
func traceMessageType(n int64) string {
	values := []string{"Normal_Msg", "Trans_Msg_Half", "Trans_msg_Commit", "Delay_Msg"}
	if n < 0 || n >= int64(len(values)) {
		return "Normal_Msg"
	}
	return values[n]
}
func traceNode(v map[string]any) map[string]any {
	node := map[string]any{}
	for _, key := range []string{"requestId", "storeHost", "clientHost", "costTime", "retryTimes", "status", "transactionState", "transactionId", "fromTransactionCheck", "msgType"} {
		node[key] = v[key]
	}
	begin := int64Value(v, 0, "timeStamp")
	node["beginTimestamp"] = begin
	node["endTimestamp"] = begin + maxValue(int64(0), int64Value(v, 0, "costTime"))
	return node
}
