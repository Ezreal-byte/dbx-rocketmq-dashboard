export interface DBXHost {
  ready: Promise<unknown>;
  context?: { connectionId?: string; id?: string };
  capabilities?: { downloadFile?: boolean };
  invoke<T = unknown>(method: string, params: Record<string, unknown>, options?: { timeoutMs?: number }): Promise<T>;
  request<T = unknown>(method: string, params: Record<string, unknown>): Promise<T>;
  onContext?: (callback: () => void) => (() => void);
}

declare global {
  interface Window { dbxPlugin?: DBXHost }
}

export async function getHost() {
  const host = window.dbxPlugin;
  if (!host) throw new Error('请从 DBX 的 RocketMQ 连接打开控制台');
  await host.ready;
  const connectionId = host.context?.connectionId || host.context?.id;
  if (!connectionId) throw new Error('请先连接 RocketMQ');
  return { host, connectionId };
}
