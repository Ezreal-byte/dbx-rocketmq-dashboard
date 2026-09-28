#!/usr/bin/env node
// Guard rails for the manifest fields that are easy to break and expensive to
// debug at install time. Run before packaging.
//
// Usage: node scripts/verify-manifest.mjs

import fs from "node:fs";

const EXPECTED_ID = "io.dbx.rocketmq-console";
const EXPECTED_DATABASE_TYPE = "rocketmq-dbx-plugin";
const EXPECTED_BINARY = "dbx-plugin-rocketmq";

function fail(message) {
  process.stderr.write(`manifest contract violated: ${message}\n`);
  process.exit(1);
}

const manifest = JSON.parse(fs.readFileSync("manifest.json", "utf8"));
const toml = fs.readFileSync("dbx-plugin.toml", "utf8");

if (manifest.manifest_version !== 1) fail("manifest_version must be 1");
if (manifest.id !== EXPECTED_ID) fail(`id must be ${EXPECTED_ID}`);
if (!/^\d+\.\d+\.\d+$/.test(manifest.version)) fail("version must be semantic");
if (!manifest.publisher?.trim()) fail("publisher is required");
if (!manifest.icon?.startsWith("assets/")) fail("icon must live under assets/");

// Deprecated fields are rejected by the host and silently rewritten by the CLI.
for (const deprecated of ["kind", "binaries", "protocol"]) {
  const entrypoints = JSON.stringify(manifest.entrypoints);
  if (deprecated === "kind" ? manifest.entrypoints?.ui?.kind : entrypoints.includes(`"${deprecated}"`)) {
    fail(`entrypoints.${deprecated} is deprecated`);
  }
}

if (manifest.entrypoints?.backend?.transport !== "stdio-jsonl") fail("backend transport must be stdio-jsonl");
if (JSON.stringify(manifest.entrypoints?.backend?.protocol_versions) !== "[1]") fail("backend protocol_versions must be [1]");
if (manifest.entrypoints?.backend?.executable !== `bin/${EXPECTED_BINARY}`) fail(`backend executable must be bin/${EXPECTED_BINARY}`);
if (manifest.entrypoints?.ui?.root !== "ui" || manifest.entrypoints?.ui?.entry !== "ui/index.html") {
  fail("ui entrypoint must be ui/index.html under ui/");
}
if (!toml.includes(`binary = "${EXPECTED_BINARY}"`)) fail("dbx-plugin.toml binary must match the manifest executable");
if (!toml.includes('directory = "backend"')) fail("dbx-plugin.toml backend directory must be backend");

// `proxy_route` is what makes a RocketMQ connection usable behind DBX transport
// layers: a static tunnel reaches one endpoint, while a cluster needs the
// NameServer plus every advertised broker.
const provider = (manifest.contributions || []).find((entry) => entry.type === "connection-provider");
if (!provider) fail("a connection-provider contribution is required");
if (provider.proxy_route !== true) fail("connection-provider must declare proxy_route");
if (provider.database_type !== EXPECTED_DATABASE_TYPE) fail(`database_type must be ${EXPECTED_DATABASE_TYPE}`);

const providerIds = new Set((manifest.contributions || []).map((entry) => entry.id));
if (!provider.workbench || !providerIds.has(provider.workbench)) fail("provider workbench must reference a declared workbench");

// Secrets must never land in external_config.
const fields = provider.fields || [];
const secretKeys = new Set(fields.filter((field) => field.binding === "secret").map((field) => field.key));
for (const required of ["access_key", "secret_key"]) {
  if (!secretKeys.has(required)) fail(`${required} must use binding "secret"`);
}
for (const field of fields) {
  if (field.binding === "port" && field.type !== "number") fail("binding \"port\" requires type \"number\"");
  if ((field.type === "select" || field.type === "radio") && !(field.options || []).length) fail(`${field.key} needs options`);
  if (!["select", "radio"].includes(field.type) && field.options) fail(`${field.key} must not declare options`);
}

const engines = manifest.engines || {};
if (!/^>=\d+\.\d+\.\d+$/.test(engines.dbx || "")) fail("engines.dbx must be a >= floor");
if (engines.host_api !== "1") fail("engines.host_api must be 1");
if ((manifest.permissions || []).join(",") !== "host.workbench") fail("permissions must be exactly host.workbench");

process.stdout.write(`manifest contract verified (${manifest.id} ${manifest.version}, dbx ${engines.dbx})\n`);
