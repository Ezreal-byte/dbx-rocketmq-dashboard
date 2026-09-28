package main

import (
 "bytes"
 "encoding/csv"
 "fmt"
 "strings"
)

func studioResourceCSV(rows []any,topics bool)(string,error){
 headers:=[]string{"Name","Namespace","Cluster ID","Subscription Mode","Consume Type","Online Instances","Total Lag","Delay Seconds","Subscription Data Type","Delivery Order Type","Retry Max Times","Subscribed Topics","Created At","Updated At"}
 fields:=[]string{"name","namespace","clusterId","subscriptionMode","consumeType","onlineInstances","totalLag","delaySeconds","subscriptionDataType","deliveryOrderType","retryMaxTimes","subscribedTopics","gmtCreate","gmtModified"}
 if topics{headers=[]string{"Name","Namespace","Type","Cluster ID","Write Queues","Read Queues","Permission","Message Count","TPS","Consumer Groups","Remark","Created At","Updated At"};fields=[]string{"name","namespace","type","clusterId","writeQueues","readQueues","perm","messageCount","tps","consumerGroupCount","remark","gmtCreate","gmtModified"}}
 var buf bytes.Buffer;w:=csv.NewWriter(&buf);w.UseCRLF=true;if e:=w.Write(headers);e!=nil{return "",e};for _,raw:=range rows{m:=jsonObject(raw);record:=[]string{};for _,field:=range fields{v:="";if m[field]!=nil{v=fmt.Sprint(m[field])};if field=="subscribedTopics"{v=strings.Join(stringSlice(m[field]),";")};if len(v)>0&&strings.ContainsAny(v[:1],"=+-@\t\r\n'"){v="'"+v};record=append(record,v)};if e:=w.Write(record);e!=nil{return "",e}};w.Flush();return buf.String(),w.Error()
}
