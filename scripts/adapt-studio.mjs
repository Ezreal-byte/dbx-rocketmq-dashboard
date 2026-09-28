// One-time, anchored adaptation of the pinned upstream Studio source.
import fs from 'node:fs';
const read = p => fs.readFileSync(p, 'utf8').replaceAll('\r\n', '\n');
const write = (p, s) => fs.writeFileSync(p, s, 'utf8');
const edit = (p, fn) => write(p, fn(read(p)));
const header = s => s.slice(0, s.indexOf('*/') + 2) + '\n\n';
edit('web/src/api/client.ts', s => header(s) + `import axios from 'axios';
import { dbxAdapter } from '../dbx/adapter';

// DBX owns authentication, connection credentials and transport.
export function handleSessionUnauthorized(): void { /* No console login session. */ }
const client = axios.create({ adapter: dbxAdapter, timeout: 60000 });
export default client;
`);
edit('web/src/StudioApp.tsx', s => s.replaceAll('BrowserRouter', 'MemoryRouter'));
edit('web/src/main.tsx', s => s.replace("import StudioApp from './StudioApp';", "import StudioApp from './StudioApp';\nimport HostGate from './dbx/HostGate';").replace('<StudioApp />', '<HostGate><StudioApp /></HostGate>'));
edit('web/src/stores/dataModeStore.ts', s => header(s) + `import { create } from 'zustand';
// A DBX connection always uses real Broker data. No demo mode is available.
export const useDataModeStore = create<{ useMock: boolean; toggle: () => void }>(() => ({ useMock: false, toggle: () => {} }));
`);
edit('web/src/services/dataMode.ts', s => header(s) + 'export function isMockMode(): boolean { return false; }\n');
edit('web/src/App.tsx', s => {
  s = s.replace('lazy, Suspense, useCallback, useEffect, useState', 'lazy, Suspense').replace('Button, Result, Spin', 'Spin');
  for (const imp of ["import { getAuthStatus } from './api/auth';", "import { isMockMode } from './services/dataMode';", "import useAuthStore from './stores/authStore';"]) s = s.replace(imp + '\n', '');
  const remove = ['LoginPage','HomePage','InstancePage','K8sCertsPage','AlertsPage','SystemAlertsPage','NotificationDeliveriesPage','AuditPage','AiPage','SettingsPage','LiteTopicPage','GrafanaDashboardsPage','AlertRuleAssetsPage','UserManagementPage'];
  s = s.split('\n').filter(l => !remove.some(n => l.startsWith('const '+n+' ='))).join('\n');
  const a = s.indexOf('type AuthGateState'); const b = s.indexOf('export function LazyRouteOutlet');
  s = s.slice(0,a) + 'export function AuthGate() { return <Outlet />; }\n\n' + s.slice(b);
  const c = s.indexOf('      <Route\n        path="/login"'); const d = s.indexOf('      <Route element={<AuthGate />}>', c);
  s = s.slice(0,c)+s.slice(d);
  s = s.replace('<Route index element={<HomePage />} />', '<Route index element={<DashboardOpsPage />} />');
  s = s.replace('<Route path="instance" element={<InstancePage />} />', '<Route path="instance" element={<DashboardOpsPage />} />');
  s = s.split('\n').filter(l => !remove.some(n => l.includes('element={<'+n))).join('\n');
  // Existing connection settings remain in the DBX connection form.
  s = s.replace('<Route path="studio/ops" element={<OpsPage />} />', '<Route path="studio/ops" element={<OpsPage />} />');
  return s;
});
edit('web/src/layouts/MainLayout.tsx', s => {
  s = s.replace('Avatar, Dropdown, ', '').replace(', message }', ' }');
  for (const n of ['Sparkle','GearSix','UserGear','ShieldCheck','BellRinging','Siren','PaperPlaneTilt','Notebook','Warning']) s = s.replace('  '+n+',\n','');
  for (const l of ["import { logout as requestLogout } from '../api/auth';", "import useAuthStore from '../stores/authStore';", "import { useDataModeStore } from '../stores/dataModeStore';"]) s=s.replace(l+'\n','');
  s=s.split('\n').filter(l => !['const clearAuth =','const admin =','const username =','const useMock =','const toggleDataMode ='].some(x=>l.trim().startsWith(x))).join('\n');
  let a=s.indexOf('  // Pages fetch on mount'); let b=s.indexOf('  useEffect(() => {',a); s=s.slice(0,a)+s.slice(b);
  // Explicit scope: original RocketMQ management; DBX owns instance connections.
  a=s.indexOf('  const menuItems = useMemo('); b=s.indexOf('  const breadcrumbMap:',a);
  s=s.slice(0,a)+`  const menuItems = useMemo(() => [
    { key: '/', icon: <House size={iconSize} weight="duotone" />, label: t('nav.home') },
    { key: 'instance-group', icon: <Database size={iconSize} weight="duotone" />, label: t('nav.instance'), children: [
      { key: '/instance/topic', icon: <ListDashes size={16} />, label: t('nav.topic') },
      { key: '/instance/consumer', icon: <ChatCircleText size={16} />, label: t('nav.group') },
      { key: '/instance/acl', icon: <Key size={16} />, label: t('nav.acl') },
      { key: '/instance/message', icon: <MagnifyingGlass size={16} />, label: t('nav.message') },
      { key: '/instance/dlq', icon: <TrashSimple size={16} />, label: t('nav.dlq') },
    ].filter(item => hasInstanceCapability(instanceCapabilities, ({ '/instance/topic': 'TOPIC_MANAGEMENT', '/instance/consumer': 'CONSUMER_GROUP_MANAGEMENT', '/instance/acl': 'ACL_MANAGEMENT', '/instance/message': 'MESSAGE_QUERY', '/instance/dlq': 'DLQ_MANAGEMENT' } as Record<string, InstanceCapability>)[item.key])) },
    { key: 'cluster-ops-group', icon: <Monitor size={iconSize} weight="duotone" />, label: t('nav.clusterOps'), children: [
      { key: '/cluster', icon: <Database size={16} />, label: t('nav.rocketmqCluster') },
      { key: '/cluster/clients', icon: <PlugsConnected size={16} />, label: t('nav.clients') },
      { key: '/studio/producer', icon: <PlugsConnected size={16} />, label: t('producer.title') },
      { key: '/ops/dashboard', icon: <ChartBar size={16} />, label: t('nav.dashboard') },
    ] },
  ], [t, instanceCapabilities]);

`+s.slice(b);
  a=s.indexOf('  const userMenu = {'); b=s.indexOf('  const borderColor',a); s=s.slice(0,a)+s.slice(b);
  a=s.indexOf('              {/* User avatar */}'); b=s.indexOf('              </Dropdown>',a); s=s.slice(0,a)+s.slice(b+'              </Dropdown>'.length);
  return s;
});
edit('web/vite.config.ts', s => {
  s=s.replace('plugins: [react(), distributionLicenses()],', "base: './',\n    plugins: [react(), distributionLicenses()],");
  const a=s.indexOf('    build: {'); const b=s.indexOf('    server: {',a);
  return s.slice(0,a)+`    build: {
      target: 'chrome109', cssCodeSplit: false, assetsInlineLimit: 10000000,
      chunkSizeWarningLimit: 6000,
      rollupOptions: { output: { inlineDynamicImports: true, format: 'iife', name: 'DBXStudio' } },
    },
`+s.slice(b);
});
edit('package.json', s => {const p=JSON.parse(s);p.version='0.2.0';p.scripts.build='npm --prefix web run build && node scripts/inline-studio.mjs';return JSON.stringify(p,null,2)+'\n';});
