import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import crypto from 'node:crypto';
import {spawnSync} from 'node:child_process';
const root=process.cwd();
const upstream=process.env.DASHBOARD_UPSTREAM||path.resolve('../rocketmq-dashboard/frontend-new');
const legacy=path.resolve('../dbx-plugin-rocketmq');
const ignored=new Set(['node_modules','.git','ui','dist','.dbx-dev','target']);
function files(dir,prefix=''){return fs.readdirSync(dir,{withFileTypes:true}).flatMap(e=>ignored.has(e.name)?[]:e.isDirectory()?files(path.join(dir,e.name),prefix+e.name+'/'):e.isFile()?[prefix+e.name]:[])}
const hash=p=>crypto.createHash('sha256').update(fs.readFileSync(p)).digest('hex');
fs.mkdirSync('docs',{recursive:true});
fs.writeFileSync('docs/legacy-source-hashes.json',JSON.stringify(Object.fromEntries(files(legacy).filter(f=>/^(backend|scripts|assets)\//.test(f)&&!f.endsWith('.exe')).map(f=>[f,hash(path.join(legacy,f))])),null,2)+'\n','utf8');
const before=new Set(files(upstream)),after=new Set(files('frontend'));
const rows=[...new Set([...before,...after])].sort().map(file=>({file,upstream:before.has(file)?hash(path.join(upstream,file)):null,local:after.has(file)?hash(path.join('frontend',file)):null}));
fs.writeFileSync('docs/frontend-source-inventory.json',JSON.stringify(rows,null,2)+'\n','utf8');
const temp=fs.mkdtempSync(path.join(os.tmpdir(),'dbx-dashboard-audit-'));
for(const [side,source]of [['a',upstream],['b',path.join(root,'frontend')]])for(const file of files(source)){const dest=path.join(temp,side,'frontend',file);fs.mkdirSync(path.dirname(dest),{recursive:true});fs.copyFileSync(path.join(source,file),dest)}
const diff=spawnSync('git',['diff','--no-index','--no-prefix','--','a/frontend','b/frontend'],{cwd:temp,encoding:'utf8',maxBuffer:32*1024*1024});
if(diff.status!==0&&diff.status!==1)throw Error(diff.stderr);
fs.writeFileSync('docs/upstream-2.1.0.patch',diff.stdout,'utf8');
if(!path.resolve(temp).startsWith(path.resolve(os.tmpdir())+path.sep))throw Error('unexpected temporary path');
fs.rmSync(temp,{recursive:true});
console.log(`Source audit: ${rows.length} frontend files, ${rows.filter(r=>r.local!==r.upstream).length} added/changed/deleted.`);
