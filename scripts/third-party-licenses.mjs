import fs from 'node:fs';
import path from 'node:path';
import {execFileSync} from 'node:child_process';
const go=process.env.GO_BIN||'go';
const rows=execFileSync(go,['list','-m','-f','{{.Path}}|{{.Version}}|{{.Dir}}','all'],{cwd:'backend',encoding:'utf8'}).trim().split(/\r?\n/);
const entries=[];
for(const line of rows){const [module,version,dir]=line.split('|');if(!dir||module==='dbx-rocketmq-dashboard')continue;
 const names=fs.readdirSync(dir).filter(n=>/^(LICENSE|LICENCE|NOTICE|COPYING)([._-].*)?$/i.test(n)&&fs.statSync(path.join(dir,n)).isFile());
 const target=path.join('licenses',module.replace(/[^a-zA-Z0-9._-]/g,'_'));fs.mkdirSync(target,{recursive:true});
 for(const name of names){const destination=path.join(target,name);if(fs.existsSync(destination))fs.chmodSync(destination,0o644);fs.copyFileSync(path.join(dir,name),destination);fs.chmodSync(destination,0o644);}
 entries.push({module,version,files:names.map(n=>path.join(target,n).replaceAll('\\','/'))});
}
fs.writeFileSync('licenses/index.json',JSON.stringify(entries,null,2)+'\n','utf8');
console.log(`Collected license records for ${entries.length} Go modules.`);
