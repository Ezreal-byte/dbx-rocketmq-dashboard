import { getHost } from './host';

export async function hostDownload(blob: Blob, filename: string): Promise<void> {
  const { host, connectionId } = await getHost();
  if (!host.capabilities?.downloadFile) throw new Error('当前 DBX 宿主不支持文件下载');
  const bytes = new Uint8Array(await blob.arrayBuffer());
  if (bytes.length > 8 * 1024 * 1024) throw new Error('导出超过 8 MiB，请缩小范围');
  const downloadId = crypto.randomUUID();
  const encode = (chunk: Uint8Array) => {
    let binary = '';
    for (let i = 0; i < chunk.length; i += 8192) binary += String.fromCharCode(...chunk.subarray(i, i + 8192));
    return btoa(binary);
  };
  if (bytes.length <= 256 * 1024) {
    await host.request('host.downloadFile', { downloadId, fileName: filename, params: { connectionId, contentBase64: encode(bytes) } });
    return;
  }
  try {
    await host.invoke('filesystem/download/stage', { connectionId, downloadId, size: bytes.length });
    for (let offset = 0; offset < bytes.length; offset += 192 * 1024) {
      await host.invoke('filesystem/download/append', { connectionId, downloadId, offset, dataBase64: encode(bytes.subarray(offset, offset + 192 * 1024)) });
    }
    await host.request('host.downloadFile', { downloadId, fileName: filename, params: { connectionId } });
  } finally {
    await host.invoke('filesystem/download/close', { connectionId, downloadId });
  }
}
