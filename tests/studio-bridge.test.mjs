import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { createRequire } from 'node:module';

const require = createRequire(new URL('../web/package.json', import.meta.url));
const ts = require('typescript');
const uri = source => 'data:text/javascript;base64,' + Buffer.from(source).toString('base64');
const compile = file => ts.transpileModule(fs.readFileSync(file, 'utf8'), {
  compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2022 },
}).outputText;
const host = uri(compile('web/src/dbx/host.ts'));
const { dbxAdapter } = await import(uri(compile('web/src/dbx/adapter.ts').replace("'./host'", JSON.stringify(host))));

function setup(invoke) {
  globalThis.window = { dbxPlugin: { ready: Promise.resolve(), context: { connectionId: 'A' }, invoke } };
}

test('Studio bridge preserves JSON, query, method, identity and response envelope', async () => {
  let request;
  setup(async (...args) => { request = args; return { code: 0, data: { value: '中文' } }; });
  const result = await dbxAdapter({ url: '/groups/settings', method: 'post', params: { name: '组' }, data: '{"retryMaxTimes":9}' });
  assert.equal(request[0], 'studio/request');
  assert.deepEqual(request[1], { connectionId: 'A', method: 'POST', path: '/groups/settings', query: { name: '组' }, body: { retryMaxTimes: 9 } });
  assert.equal(request[2].timeoutMs, 60000);
  assert.deepEqual(result.data, { code: 0, data: { value: '中文' } });
});

test('Studio bridge rejects external destinations and cancelled requests before invoke', async () => {
  setup(async () => assert.fail('must not invoke'));
  for (const url of ['https://evil.test', '//evil.test', 'topics']) {
    await assert.rejects(dbxAdapter({ url }), /已登记/);
  }
  await assert.rejects(dbxAdapter({ url: '/topics', signal: { aborted: true } }), /取消/);
});

test('Studio bridge rejects stale responses when DBX changes connections', async () => {
  setup(async () => { window.dbxPlugin.context.connectionId = 'B'; return { code: 0, data: [] }; });
  await assert.rejects(dbxAdapter({ url: '/topics' }), /连接已切换/);
});

test('Studio bridge surfaces backend failures and preserves binary export bytes', async () => {
  setup(async () => ({ code: 500, message: 'Broker permission denied' }));
  await assert.rejects(dbxAdapter({ url: '/topics' }), /Broker permission denied/);
  setup(async () => ({ code: 0, data: { contentBase64: 'AAH/gA==', contentType: 'application/octet-stream' } }));
  const result = await dbxAdapter({ url: '/messages/export', responseType: 'blob' });
  assert.deepEqual([...new Uint8Array(await result.data.arrayBuffer())], [0, 1, 255, 128]);
  assert.equal(result.data.type, 'application/octet-stream');
});
