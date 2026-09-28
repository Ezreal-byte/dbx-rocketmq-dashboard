import { Fragment, useEffect, useState, type ReactNode } from 'react';
import { Alert, Spin } from 'antd';
import { getHost } from './host';

export default function HostGate({ children }: { children: ReactNode }) {
  const [state, setState] = useState<{ id?: string; error?: string }>({});
  useEffect(() => {
    let alive = true, generation = 0;
    async function load() {
      const current = ++generation;
      setState({});
      try {
        const { host, connectionId } = await getHost();
        await host.invoke('rocketmq/info', { connectionId });
        if (alive && current === generation) setState({ id: connectionId });
      } catch (error) {
        if (alive && current === generation) setState({ error: String(error) });
      }
    }
    void load();
    const off = window.dbxPlugin?.onContext?.(load);
    return () => { alive = false; off?.(); };
  }, []);
  if (state.error) return <Alert type="error" showIcon message={state.error} />;
  if (!state.id) return <Spin style={{ margin: 32 }} />;
  return <Fragment key={state.id}>{children}</Fragment>;
}
