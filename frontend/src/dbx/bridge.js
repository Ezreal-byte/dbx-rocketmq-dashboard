// Dashboard's request contract transported over DBX, never browser HTTP.
export async function bridgeFetch(url, options = {}) {
    const host = window.dbxPlugin;
    if (!host) throw new Error('Open this console from a DBX RocketMQ connection.');
    await host.ready;
    const connectionId = host.context?.connectionId || host.context?.id;
    if (!connectionId) throw new Error('No active DBX connection.');
    const parsed = new URL(url, 'https://dashboard.dbx.invalid');
    const query = Object.fromEntries(parsed.searchParams);
    Object.assign(query, options.params || {});
    let body = options.body;
    if (typeof body === 'string') {
        if ((options.headers?.['Content-Type'] || '').includes('application/json')) body = JSON.parse(body);
        else body = Object.fromEntries(new URLSearchParams(body));
    }
    const result = await host.invoke('dashboard/request', {
        connectionId, method: options.method || 'GET', path: parsed.pathname,
        query, body: body ?? null,
    }, {timeoutMs: 60000});
    return {ok: true, status: 200, redirected: false, json: async () => result};
}

export async function exportJSON(filename, data) {
    const text = JSON.stringify(data, null, 2);
    return hostDownload(filename, {content:text});
}

export async function exportDataURL(filename, dataURL) {
    const match = /^data:image\/(?:png|jpeg);base64,([A-Za-z0-9+/=]+)$/.exec(dataURL);
    if (!match) throw new Error('Invalid chart image.');
    return hostDownload(filename, {contentBase64:match[1]});
}

async function hostDownload(filename, content) {
    const host = window.dbxPlugin;
    await host?.ready;
    if (!host?.capabilities?.downloadFile) throw new Error('This DBX host does not support file downloads.');
    const connectionId = host.context?.connectionId || host.context?.id;
    const downloadId = crypto.randomUUID();
    const bytes = content.content !== undefined ? new TextEncoder().encode(content.content) : Uint8Array.from(atob(content.contentBase64),c=>c.charCodeAt(0));
    if (bytes.length > 8*1024*1024) throw new Error('Export exceeds 8 MiB. Select fewer messages.');
    // Small requests preserve the simple host contract; larger exports avoid its frame limit.
    if (bytes.length <= 256*1024) return host.request('host.downloadFile',{downloadId,fileName:filename,params:{connectionId,...content}});
    try {
        await host.invoke('filesystem/download/stage',{connectionId,downloadId,size:bytes.length});
        for(let offset=0;offset<bytes.length;offset+=192*1024){
            const chunk=bytes.subarray(offset,offset+192*1024);
            let binary='';for(let i=0;i<chunk.length;i+=8192)binary+=String.fromCharCode(...chunk.subarray(i,i+8192));
            await host.invoke('filesystem/download/append',{connectionId,downloadId,offset,dataBase64:btoa(binary)});
        }
        await host.request('host.downloadFile',{downloadId,fileName:filename,params:{connectionId}});
    } finally {
        await host.invoke('filesystem/download/close',{connectionId,downloadId});
    }
}
