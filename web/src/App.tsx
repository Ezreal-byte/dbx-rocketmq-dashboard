/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { lazy, Suspense } from 'react';
import { Spin } from 'antd';
import { Routes, Route, Navigate, Outlet } from 'react-router-dom';
import { useLang } from './i18n/LangContext';
import MainLayout from './layouts/MainLayout';

const TopicPage = lazy(() => import('./pages/instance/topic'));
const ConsumerPage = lazy(() => import('./pages/instance/consumer'));
const MessagePage = lazy(() => import('./pages/instance/message'));
const AclPage = lazy(() => import('./pages/instance/acl'));
const DlqPage = lazy(() => import('./pages/instance/dlq'));
const ClusterPage = lazy(() => import('./pages/cluster'));
const ClientsPage = lazy(() => import('./pages/cluster/clients'));
const DashboardOpsPage = lazy(() => import('./pages/home/dashboard'));
const ProxyPage = lazy(() => import('./pages/studio/Proxy'));
const BrokerClusterPage = lazy(() => import('./pages/studio/BrokerCluster'));
const ProducerPage = lazy(() => import('./pages/studio/Producer'));
const OpsPage = lazy(() => import('./pages/studio/Ops'));

export function AuthGate() { return <Outlet />; }

export function LazyRouteOutlet() {
  const { t } = useLang();

  return (
    <Suspense
      fallback={
        <div
          role="status"
          aria-label={t('common.loading')}
          style={{ minHeight: 240, display: 'grid', placeItems: 'center' }}
        >
          <Spin size="large" />
        </div>
      }
    >
      <Outlet />
    </Suspense>
  );
}

function App() {
  return (
    <Routes>
      <Route element={<AuthGate />}>
        <Route path="/" element={<MainLayout />}>
          <Route element={<LazyRouteOutlet />}>
            <Route index element={<DashboardOpsPage />} />
            <Route path="instance" element={<DashboardOpsPage />} />
            <Route path="instance/topic" element={<TopicPage />} />
            <Route path="instance/:instanceId/topic" element={<TopicPage />} />
            <Route path="instance/consumer" element={<ConsumerPage />} />
            <Route path="instance/:instanceId/consumer" element={<ConsumerPage />} />
            <Route path="instance/message" element={<MessagePage />} />
            <Route path="instance/:instanceId/message" element={<MessagePage />} />
            <Route path="instance/acl" element={<AclPage />} />
            <Route path="instance/:instanceId/acl" element={<AclPage />} />
            <Route path="instance/dlq" element={<DlqPage />} />
            <Route path="instance/:instanceId/dlq" element={<DlqPage />} />
            <Route path="cluster" element={<ClusterPage />} />
            <Route path="cluster/clients" element={<ClientsPage />} />
            <Route path="ops/dashboard" element={<DashboardOpsPage />} />
            <Route path="studio/proxy" element={<ProxyPage />} />
            <Route
              path="studio/group-management"
              element={<Navigate to="/instance/consumer" replace />}
            />
            <Route path="studio/broker-cluster" element={<BrokerClusterPage />} />
            <Route
              path="studio/alert-management"
              element={<Navigate to="/ops/business-alerts" replace />}
            />
            <Route path="studio/producer" element={<ProducerPage />} />
            <Route path="studio/ops" element={<OpsPage />} />
            <Route
              path="instance/alerts"
              element={<Navigate to="/ops/business-alerts" replace />}
            />
            <Route path="*" element={<Navigate to="/" replace />} />
          </Route>
        </Route>
      </Route>
    </Routes>
  );
}

export default App;
