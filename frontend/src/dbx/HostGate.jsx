import React, {useEffect, useState} from 'react';
import {Alert, Spin} from 'antd';

export default function HostGate({children}) {
    const [state, setState] = useState({loading: true});
    useEffect(() => {
        let alive = true, generation = 0, off;
        async function load() {
            const current = ++generation;
            setState({loading: true});
            try {
                const host = window.dbxPlugin;
                if (!host) throw new Error('请从 DBX 的 RocketMQ 连接打开控制台 / Open from a DBX RocketMQ connection.');
                await host.ready;
                const id = host.context?.connectionId || host.context?.id;
                if (!id) throw new Error('请先连接 RocketMQ / Connect to RocketMQ first.');
                const info = await host.invoke('rocketmq/info', {connectionId: id});
                if (!alive || current !== generation) return;
                window.dbxDashboardInfo = info;
                setState({id, loading: false});
            } catch (error) {
                if (alive && current === generation) setState({error: String(error.message || error)});
            }
        }
        load();
        off = window.dbxPlugin?.onContext?.(load);
        return () => { alive = false; off?.(); };
    }, []);
    if (state.error) return <Alert type="error" showIcon message={state.error}/>;
    if (state.loading) return <Spin style={{margin: 32}}/>;
    return <React.Fragment key={state.id}>{children}</React.Fragment>;
}
