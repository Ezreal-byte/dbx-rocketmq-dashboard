import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
const read=p=>fs.readFileSync(p,'utf8');
const uri=s=>'data:text/javascript;base64,'+Buffer.from(s).toString('base64');
const bridgeURL=uri(read('frontend/src/dbx/bridge.js'));
const {historySeries}=await import(uri(read('frontend/src/dbx/history.js')));
test('history retains timestamps, zeros and actual sampling gaps',()=>{
 assert.deepEqual(historySeries(['100000,0','160000,4','340000,9'],1),[[100000,0],[160000,4],[220000,null],[340000,9]]);
 assert.deepEqual(historySeries([],1),[]);
 assert.deepEqual(historySeries(['100000,'],1),[[100000,null]]);
});
const {bridgeFetch,exportJSON,exportDataURL}=await import(bridgeURL);
const source=read('frontend/src/api/remoteApi/remoteApi.js').replace("'../../dbx/bridge'",JSON.stringify(bridgeURL));
const {remoteApi}=await import(uri(source));
let calls=[];
globalThis.window={dbxPlugin:{ready:Promise.resolve(),context:{connectionId:'connection-A'},capabilities:{downloadFile:true},invoke:async(method,params)=>{calls.push({method,params});return {status:0,data:[]}},request:async(method,params)=>{calls.push({method,params});return null}}};
test('bridge preserves JSON, query, method and isolated connection identity',async()=>{
 calls=[];
 await bridgeFetch('https://dashboard.dbx.invalid/consumer/createOrUpdate.do?label=%E4%B8%AD%E6%96%87',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({subscriptionGroupConfig:{groupName:'G'},brokerNameList:['B']})});
 assert.equal(calls[0].params.connectionId,'connection-A');assert.equal(calls[0].params.query.label,'中文');assert.equal(calls[0].params.method,'POST');assert.deepEqual(calls[0].params.body.brokerNameList,['B']);
 window.dbxPlugin.context={connectionId:'connection-B'};
 await bridgeFetch('https://dashboard.dbx.invalid/cluster/list.query');assert.equal(calls[1].params.connectionId,'connection-B');
});
test('direct-consume params and ACL broker selection survive the transport',async()=>{
 calls=[];await remoteApi.resendDlqMessage('id','group','%DLQ%group');assert.equal(calls[0].params.query.topic,'%DLQ%group');assert.equal(calls[0].params.method,'POST');
 await remoteApi.deleteAcl('broker','User:test','Topic:orders','');assert.equal(calls[1].params.query.brokerName,'broker');assert.equal(calls[1].params.method,'DELETE');
});
test('exports use the native download contract, including binary chart data',async()=>{
 calls=[];await exportJSON('message.json',{value:'世界'});await exportDataURL('chart.png','data:image/png;base64,AQID');
 assert.equal(calls[0].method,'host.downloadFile');assert.equal(calls[0].params.fileName,'message.json');assert.deepEqual(JSON.parse(calls[0].params.params.content),{value:'世界'});assert.equal(calls[1].params.params.contentBase64,'AQID');
});
test('large exports stage bounded chunks before opening native download',async()=>{
 calls=[];const data={body:'世界'.repeat(70000)};await exportJSON('large.json',data);
 assert.equal(calls[0].method,'filesystem/download/stage');const chunks=calls.filter(c=>c.method==='filesystem/download/append');assert.ok(chunks.length>1);
 const value=Buffer.concat(chunks.map(c=>Buffer.from(c.params.dataBase64,'base64'))).toString('utf8');assert.deepEqual(JSON.parse(value),data);
 for(const c of chunks)assert.ok(c.params.dataBase64.length<=256*1024);
 assert.equal(calls.at(-2).method,'host.downloadFile');assert.equal(calls.at(-1).method,'filesystem/download/close');
});
test('every retained business page calls an implemented API function',()=>{
 function walk(root){return fs.readdirSync(root,{withFileTypes:true}).flatMap(e=>e.isDirectory()?walk(root+'/'+e.name):[root+'/'+e.name])}
 for(const file of walk('frontend/src').filter(p=>/\.(jsx|js)$/.test(p)&&!p.includes('/api/')&&!/\/pages\/(Login|Ops|Proxy)\//.test(p))){
  for(const [,name]of read(file).matchAll(/remoteApi\.([A-Za-z0-9_]+)\s*\(/g))assert.equal(typeof remoteApi[name],'function',`${file}: ${name}`);
 }
});
test('every retained endpoint has an explicit Go allowlist entry',()=>{
 const registered=new Set([...read('backend/dashboard.go').matchAll(/"(\/[^"]+\.(?:query|do|all|refresh|queryTopicType))"\s*:\s*\{/g)].map(m=>m[1]));
 for(const [,route]of source.matchAll(/(\/(?:cluster|dashboard|topic|consumer|producer|message|messageTrace|dlqMessage|acl|monitor)\/[A-Za-z.]+)/g)) assert.ok(registered.has(route),route);
});
test('packaged HTML embeds parseable scripts and no browser network resources',()=>{
 const html=read('ui/index.html');const scripts=[...html.matchAll(/<script>([\s\S]*?)<\/script>/g)];assert.ok(scripts.length>0);
 for(const [,script]of scripts)new vm.Script(script);
 assert.doesNotMatch(html,/<script[^>]+src=/);assert.doesNotMatch(html,/<link[^>]+href=/);assert.ok(Buffer.byteLength(html)<8*1024*1024);
});
