import type { AxiosAdapter } from 'axios';
import { getHost } from './host';

export const dbxAdapter: AxiosAdapter = async (config) => {
  const { host, connectionId } = await getHost();
  if (config.signal?.aborted) throw new Error('请求已取消');
  const path = config.url || '';
  if (!path.startsWith('/') || path.startsWith('//') || path.includes('://')) {
    throw new Error('只允许已登记的 Studio 接口');
  }
  const result = await host.invoke<{ code: number; message?: string; data: unknown }>(
    'studio/request', {
      connectionId, method: (config.method || 'GET').toUpperCase(), path,
      query: config.params || {},
      body: typeof config.data === 'string' ? JSON.parse(config.data) : config.data,
    }, { timeoutMs: 60000 },
  );
  if (config.signal?.aborted || (host.context?.connectionId || host.context?.id) !== connectionId) {
    throw new Error('连接已切换，请重新查询');
  }
  if (result.code !== 0 && result.code !== 200) throw new Error(result.message || '请求失败');
  let data: unknown = result;
  if (config.responseType === 'blob') {
    const file = result.data as { contentBase64?: string; contentType?: string } | null;
    data = file?.contentBase64
      ? new Blob([Uint8Array.from(atob(file.contentBase64), c => c.charCodeAt(0))], { type: file.contentType })
      : new Blob([typeof result.data === 'string' ? result.data : JSON.stringify(result.data)], { type: 'application/json' });
  }
  return {
    data,
    status: 200, statusText: 'OK', headers: {}, config,
  };
};
