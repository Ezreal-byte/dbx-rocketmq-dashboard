import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
const root = path.resolve('web/dist');
let html = fs.readFileSync(path.join(root,'index.html'),'utf8');
function asset(src) {
  const file = path.resolve(root,src.replace(/^\.\//,''));
  if (!file.startsWith(root+path.sep)) throw new Error('Asset outside build');
  return fs.readFileSync(file,'utf8');
}
const scripts=[];
html=html.replace(/<script\b[^>]*src="([^"]+)"[^>]*><\/script>/g,(_,src)=>{
  const js=asset(src); new vm.Script(js); scripts.push(js.replace(/<\/script/gi,'<\\/script')); return '';
});
html=html.replace(/<link\b[^>]*href="([^"]+)"[^>]*>/g,(tag,src)=>tag.includes('stylesheet')?'<style>'+asset(src)+'</style>':'');
html=html.replace('</body>',()=>scripts.map(js=>'<script>'+js+'</script>').join('')+'</body>');
if (Buffer.byteLength(html)>8*1024*1024) throw new Error('UI exceeds DBX asset limit');
fs.mkdirSync('ui',{recursive:true});
fs.writeFileSync('ui/index.html',html,'utf8');
fs.cpSync(path.join(root,'legal'),'licenses/studio-web',{recursive:true});
console.log('Studio DBX iframe:',Buffer.byteLength(html),'bytes; no external assets');
