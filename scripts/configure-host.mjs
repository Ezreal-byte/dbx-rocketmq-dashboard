import fs from 'node:fs';
const read=p=>fs.readFileSync(p,'utf8');
const write=(p,s)=>fs.writeFileSync(p,s,'utf8');
const manifest=JSON.parse(read('manifest.json'));
manifest.name='dbx-rocketmq-console';
const provider=manifest.contributions.find(c=>c.type==='connection-provider');
const fields=[
 {key:'use_tls',label:'启用 TLS',type:'boolean',binding:'config',default:false},
 {key:'vip_channel',label:'VIP Channel',type:'boolean',binding:'config',default:false,description:'使用 Broker 管理端口减 2 的 VIP 通道'},
 {key:'proxy_addr',label:'RocketMQ Proxy 地址',type:'text',binding:'config',description:'可选 Remoting 地址（不是 gRPC 端口）；多地址以分号分隔'},
];
for(const f of fields) if(!provider.fields.some(v=>v.key===f.key)) provider.fields.splice(provider.fields.findIndex(v=>v.key==='tls_skip_verify'),0,f);
write('manifest.json',JSON.stringify(manifest,null,2)+'\n');
let p='frontend/src/pages/Consumer/consumer.jsx',s=read(p);
s=s.replace(/const \[proxyEnabled, setProxyEnabled\] = useState\(\(\) => \{[\s\S]*?\n    \}\);/,"const [proxyEnabled, setProxyEnabled] = useState(false);");
s=s.replace(/const \[selectedProxy, setSelectedProxy\] = useState\(\(\) => \{[\s\S]*?\n    \}\);/,"const [selectedProxy, setSelectedProxy] = useState(undefined);");
s=s.replace(/    useEffect\(\(\) => \{\n        localStorage.setItem\('proxyEnabled'[\s\S]*?\n    \}, \[proxyEnabled\]\);\n/, '');
s=s.replace(/    useEffect\(\(\) => \{\n        if \(selectedProxy\) \{[\s\S]*?\n    \}, \[selectedProxy\]\);\n/, '');
write(p,s);
p='tests/compose.yaml';s=read(p);s=s.replace(/JAVA_OPT_EXT: -Xms128m -Xmx128m -Xmn64m/g,'JAVA_OPT_EXT: -Xms128m -Xmx128m -Xmn64m -XX:-UseContainerSupport').replace(/JAVA_OPT_EXT: -Xms256m -Xmx256m -Xmn128m/g,'JAVA_OPT_EXT: -Xms256m -Xmx256m -Xmn128m -XX:-UseContainerSupport');write(p,s);
