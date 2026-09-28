// CRA stays upstream's build system. DBX's opaque-origin iframe requires
// embedded assets; it deliberately does not grant browser network access.
import fs from 'node:fs';
import path from 'node:path';
const root = path.resolve('ui');
const manifest=JSON.parse(fs.readFileSync(path.join(root,'asset-manifest.json'),'utf8'));
let html=fs.readFileSync('frontend/public/index.html','utf8').replaceAll('%PUBLIC_URL%','.');
html=html.replace('</body>',()=>manifest.entrypoints.map(src=>src.endsWith('.css')?`<link href="./${src}" rel="stylesheet">`:`<script src="./${src}"></script>`).join('')+'</body>');
const scripts=[];
function asset(src) {
    const p=path.resolve(root,src.replace(/^\.\//,''));
    if(!p.startsWith(root+path.sep)) throw new Error('Asset outside ui: '+src);
    return fs.readFileSync(p,'utf8');
}
html=html.replace(/<script\b[^>]*src="([^"]+)"[^>]*><\/script>/g,(_,src)=>{
    scripts.push(asset(src).replace(/<\/script/gi,'<\\/script'));
    return '';
});
html=html.replace(/<link\b[^>]*href="([^"]+)"[^>]*>/g,(tag,src)=>{
    if(tag.includes('stylesheet')) return '<style>'+asset(src)+'</style>';
    return ''; // The host supplies the manifest icon.
});
html=html.replace('</body>',()=>scripts.map(js=>'<script>'+js+'</script>').join('')+'</body>');
if(Buffer.byteLength(html)>8*1024*1024) throw new Error('UI exceeds DBX asset limit');
fs.writeFileSync(path.join(root,'index.html'),html,'utf8');
console.log('DBX iframe bundle:',Buffer.byteLength(html),'bytes');
