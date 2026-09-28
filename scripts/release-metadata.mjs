import fs from 'node:fs';
import crypto from 'node:crypto';
const manifest = JSON.parse(fs.readFileSync('manifest.json','utf8'));
const artifacts = fs.readdirSync('dist').filter(n => n.endsWith('.artifact.json') && n.startsWith(`${manifest.id}-${manifest.version}-`)).map(name => {
  const metadata = JSON.parse(fs.readFileSync(`dist/${name}`,'utf8'));
  const bytes = fs.readFileSync(`dist/${metadata.url}`);
  if (crypto.createHash('sha256').update(bytes).digest('hex') !== metadata.sha256 || bytes.length !== metadata.size) throw new Error(`Artifact mismatch: ${name}`);
  return metadata;
});
if (artifacts.length !== 1 || artifacts[0].target !== 'windows-x64') throw new Error('This release supports Windows x64 only');
fs.writeFileSync('dist/release-candidates.json',JSON.stringify({schemaVersion:1,plugin:{id:manifest.id,publisher:manifest.publisher,version:manifest.version,permissions:manifest.permissions},artifacts},null,2)+'\n','utf8');
console.log('Release identity and SHA-256 metadata verified.');
