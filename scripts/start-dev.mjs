// Use DBX's own development host and sandbox, with the real compiled sidecar.
import path from 'node:path';
import {pathToFileURL} from 'node:url';
const runtime=process.env.DBX_PLUGIN_DEV_RUNTIME||path.resolve('../dbx/plugins/sdk/dev-host/dist/runtime.mjs');
const {startDevelopment}=await import(pathToFileURL(runtime).href);
await startDevelopment({project:process.cwd(),port:5198,backend:'.dbx-dev/bin/dbx-rocketmq-dashboard.exe',shellHtml:path.join(path.dirname(runtime),'ui/index.html')});
