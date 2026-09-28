// Reproducible host adaptation. Run once against a fresh Dashboard 2.1.0 copy.
import fs from 'node:fs';
const read = p => fs.readFileSync(p, 'utf8').replaceAll('\r\n', '\n');
const write = (p, s) => fs.writeFileSync(p, s, 'utf8');
const edit = (p, fn) => write(p, fn(read(p)));
const id = 'io.dbx.rocketmq-dashboard-console';
edit('manifest.json', s => s.replaceAll('io.dbx.rocketmq-console', id).replaceAll('bin/dbx-plugin-rocketmq', 'bin/dbx-rocketmq-console').replaceAll('rocketmq-dbx-plugin', 'rocketmq-dashboard-dbx-plugin'));
edit('backend/types.go', s => s.replaceAll('io.dbx.rocketmq-console', id));
edit('backend/go.mod', s => s.replace('module dbx-plugin-rocketmq', 'module dbx-rocketmq-console').replace('require (', 'require (\n\tgithub.com/t8y2/dbx/plugins/sdk/go/dbx-plugin-sdk v0.0.0'));
edit('dbx-plugin.toml', s => s.replaceAll('dbx-plugin-rocketmq', 'dbx-rocketmq-console'));
edit('frontend/package.json', s => { const p = JSON.parse(s); p.name = 'dbx-rocketmq-console-ui'; p.homepage = '.'; p.browserslist.production = ['Chrome >= 109']; p.scripts.build = 'cross-env BUILD_PATH=../ui GENERATE_SOURCEMAP=false INLINE_RUNTIME_CHUNK=false react-scripts build'; return JSON.stringify(p, null, 2) + '\n'; });
edit('frontend/src/router/index.jsx', s => {
  s = s.replace("React, {useEffect}", 'React').replace(', useNavigate', '');
  for (const name of ['Login', 'Ops', 'Proxy']) s = s.replace(new RegExp(`^import ${name} from .*\\n`, 'm'), '');
  s = s.replace(/^import \{remoteApi\}.*\n/m, '').replace('    const navigate = useNavigate();\n', '');
  const a = s.indexOf('    useEffect(() => {'); const b = s.indexOf('    return (', a); s = s.slice(0, a) + s.slice(b);
  for (const route of ['login', 'ops', 'proxy']) s = s.replace(new RegExp(`                        <Route\\s+path="/${route}"[\\s\\S]*?                        />\\n`), '');
  return s;
});
edit('frontend/src/components/Navbar.jsx', s => {
  s = s.replace('useEffect, ', '').replace(', UserOutlined', '').replace(/^import \{remoteApi\}.*\n/m, '');
  s = s.replace('    const [userName, setUserName] = useState(null);\n', '');
  s = s.slice(0, s.indexOf('    const onLogout')) + s.slice(s.indexOf('    const langMenu'));
  s = s.slice(0, s.indexOf('    const userMenu')) + s.slice(s.indexOf('    const themeMenu'));
  s = s.replace("        {key: 'ops', label: t.OPS},\n", '').replace("        ...(rmqVersion ? [{key: 'proxy', label: t.PROXY}] : []),\n", '');
  const a = s.indexOf('                {userName && ('); const b = s.indexOf('                {isSmallScreen &&', a); return s.slice(0, a) + s.slice(b);
});
for (const page of ['Acl/acl', 'Consumer/consumer', 'Topic/topic']) edit(`frontend/src/pages/${page}.jsx`, s => s.replace("localStorage.getItem('userrole')", '(window.dbxDashboardInfo?.readOnly ? 2 : 1)'));
edit('frontend/src/api/remoteApi/remoteApi.js', s => {
  const a = s.indexOf('const appConfig'); const b = s.indexOf('    queryTopic:', a);
  s = s.slice(0, a) + "import {bridgeFetch, exportJSON} from '../../dbx/bridge';\nconst remoteApi = {\n    buildUrl: endpoint => new URL(endpoint, 'https://dashboard.dbx.invalid').toString(),\n    _fetch: bridgeFetch,\n\n" + s.slice(b);
  const login = s.indexOf('    login: async'); if (login >= 0) s = s.slice(0, login) + s.slice(s.indexOf('\n};', login));
  const start = s.indexOf('            const newWindow = window.open');
  if (start >= 0) { const end = s.indexOf('        } catch (error)', start); s = s.slice(0, start) + "            await exportJSON('dead-letter-message.json', data);\n            return {status: 0, data: true};\n" + s.slice(end); }
  return s;
});
edit('frontend/src/index.js', s => {
  s = s.replace("import App from './App';", "import App from './App';\nimport HostGate from './dbx/HostGate';");
  return s.replace('<App/>', '<HostGate><App/></HostGate>');
});
write('package.json', JSON.stringify({name:'dbx-rocketmq-console', version:'0.1.0', private:true, scripts:{build:'npm --prefix frontend run build', test:'node --test tests/*.test.mjs', 'package:windows':'npm run build && go run scripts/package-cross.go -target windows-x64'}}, null, 2)+'\n');
write('.gitignore', 'node_modules/\nfrontend/node_modules/\nui/\ndist/\n.dbx-dev/\n*.exe\n');
