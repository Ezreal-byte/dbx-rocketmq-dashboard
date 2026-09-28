#!/usr/bin/env node
// Windows 7 PE compatibility audit for the native plugin sidecar.
//
// Rejects binaries that statically import APIs or runtimes missing from a
// stock Windows 7 SP1 installation. The check is implemented by parsing the
// PE import tables directly so it works in any CI image without dumpbin,
// Visual Studio or Python.
//
// Usage: node scripts/assert-win7-pe-compat.mjs <binary> [<binary> ...]

import { readFileSync } from "node:fs";

const BLOCKED_SYMBOLS = [
  "GetSystemTimePreciseAsFileTime",
  "GetDpiForWindow",
  "GetSystemMetricsForDpi",
  "SetThreadDpiAwarenessContext",
  "SetProcessDpiAwarenessContext",
  "GetThreadDpiAwarenessContext",
  "AdjustWindowRectExForDpi",
  "SystemParametersInfoForDpi",
];

const BLOCKED_DLLS = [
  "vcruntime140.dll",
  "vcruntime140_1.dll",
  "msvcp140.dll",
  "ucrtbase.dll",
  "concrt140.dll",
  "vcruntime140_threads.dll",
];

const BLOCKED_DLL_PREFIXES = ["api-ms-win-crt-", "api-ms-win-core-"];

function fail(message) {
  process.stderr.write(`${message}\n`);
  process.exit(1);
}

function readImports(buffer) {
  if (buffer.length < 0x40 || buffer.readUInt16LE(0) !== 0x5a4d) {
    fail("not a PE image: missing MZ signature");
  }
  const peOffset = buffer.readUInt32LE(0x3c);
  if (peOffset + 24 > buffer.length || buffer.readUInt32LE(peOffset) !== 0x00004550) {
    fail("not a PE image: missing PE signature");
  }

  const coffOffset = peOffset + 4;
  const sectionCount = buffer.readUInt16LE(coffOffset + 2);
  const optionalSize = buffer.readUInt16LE(coffOffset + 16);
  const optionalOffset = coffOffset + 20;
  if (optionalOffset + optionalSize > buffer.length) fail("truncated optional header");

  const magic = buffer.readUInt16LE(optionalOffset);
  const isPe32Plus = magic === 0x20b;
  if (!isPe32Plus && magic !== 0x10b) fail(`unsupported optional header magic 0x${magic.toString(16)}`);

  const imageBase = isPe32Plus ? Number(buffer.readBigUInt64LE(optionalOffset + 24)) : buffer.readUInt32LE(optionalOffset + 28);
  const dataDirectoryOffset = optionalOffset + (isPe32Plus ? 112 : 96);
  const numberOfDirectories = buffer.readUInt32LE(optionalOffset + (isPe32Plus ? 108 : 92));

  const sectionsOffset = optionalOffset + optionalSize;
  const sections = [];
  for (let index = 0; index < sectionCount; index += 1) {
    const header = sectionsOffset + index * 40;
    if (header + 40 > buffer.length) fail("truncated section table");
    sections.push({
      name: buffer.toString("ascii", header, header + 8).replace(/\0+$/, ""),
      virtualSize: buffer.readUInt32LE(header + 8),
      virtualAddress: buffer.readUInt32LE(header + 12),
      rawSize: buffer.readUInt32LE(header + 16),
      rawOffset: buffer.readUInt32LE(header + 20),
    });
  }

  const toOffset = (rva) => {
    for (const section of sections) {
      const span = Math.max(section.virtualSize, section.rawSize);
      if (rva >= section.virtualAddress && rva < section.virtualAddress + span) {
        return section.rawOffset + (rva - section.virtualAddress);
      }
    }
    // Headers are mapped 1:1 at the start of the image.
    if (rva < optionalOffset + optionalSize) return rva;
    return -1;
  };

  const readCString = (rva) => {
    const offset = toOffset(rva);
    if (offset < 0 || offset >= buffer.length) return "";
    let end = offset;
    while (end < buffer.length && buffer[end] !== 0) end += 1;
    return buffer.toString("ascii", offset, end);
  };

  const readDirectory = (index) => {
    if (index >= numberOfDirectories) return undefined;
    const entry = dataDirectoryOffset + index * 8;
    if (entry + 8 > buffer.length) return undefined;
    const rva = buffer.readUInt32LE(entry);
    const size = buffer.readUInt32LE(entry + 4);
    return rva === 0 || size === 0 ? undefined : { rva, size };
  };

  const dlls = new Map();
  const recordDll = (name) => {
    const key = name.toLowerCase();
    if (!dlls.has(key)) dlls.set(key, new Set());
    return dlls.get(key);
  };
  const recordImportedSymbol = (dll, rva, isPe32PlusImage) => {
    // Import thunk entries are ordinals when the high bit is set.
    if (isPe32PlusImage) {
      if (rva >= 0x8000000000000000) return;
    } else if (rva >= 0x80000000) {
      return;
    }
    const offset = toOffset(rva);
    if (offset < 0 || offset + 2 >= buffer.length) return;
    recordDll(dll).add(readCString(rva + 2));
  };

  const importDirectory = readDirectory(1);
  if (importDirectory) {
    for (let offset = toOffset(importDirectory.rva); offset >= 0 && offset + 20 <= buffer.length; offset += 20) {
      const originalFirstThunk = buffer.readUInt32LE(offset);
      const nameRva = buffer.readUInt32LE(offset + 12);
      const firstThunk = buffer.readUInt32LE(offset + 16);
      if (originalFirstThunk === 0 && nameRva === 0 && firstThunk === 0) break;
      const dll = readCString(nameRva);
      recordDll(dll);
      const tableRva = originalFirstThunk !== 0 ? originalFirstThunk : firstThunk;
      const entrySize = isPe32Plus ? 8 : 4;
      for (let cursor = toOffset(tableRva); cursor >= 0 && cursor + entrySize <= buffer.length; cursor += entrySize) {
        const value = isPe32Plus ? Number(buffer.readBigUInt64LE(cursor)) : buffer.readUInt32LE(cursor);
        if (value === 0) break;
        recordImportedSymbol(dll, value, isPe32Plus);
      }
    }
  }

  // Delay-load imports are resolved at runtime but still fail closed on
  // Windows 7 when the named module is missing.
  const delayDirectory = readDirectory(13);
  if (delayDirectory) {
    for (let offset = toOffset(delayDirectory.rva); offset >= 0 && offset + 32 <= buffer.length; offset += 32) {
      const attributes = buffer.readUInt32LE(offset);
      const nameValue = buffer.readUInt32LE(offset + 4);
      const nameTableValue = buffer.readUInt32LE(offset + 16);
      if (attributes === 0 && nameValue === 0 && nameTableValue === 0) break;
      const usesRva = (attributes & 1) === 1;
      const nameRva = usesRva ? nameValue : nameValue - imageBase;
      const nameTableRva = usesRva ? nameTableValue : nameTableValue - imageBase;
      const dll = readCString(nameRva);
      if (!dll) continue;
      recordDll(dll);
      const entrySize = isPe32Plus ? 8 : 4;
      for (let cursor = toOffset(nameTableRva); cursor >= 0 && cursor + entrySize <= buffer.length; cursor += entrySize) {
        const value = isPe32Plus ? Number(buffer.readBigUInt64LE(cursor)) : buffer.readUInt32LE(cursor);
        if (value === 0) break;
        recordImportedSymbol(dll, value, isPe32Plus);
      }
    }
  }

  return dlls;
}

function audit(binaryPath) {
  const buffer = readFileSync(binaryPath);
  const dlls = readImports(buffer);
  const violations = [];

  for (const [dll, symbols] of dlls) {
    if (BLOCKED_DLLS.includes(dll) || BLOCKED_DLL_PREFIXES.some((prefix) => dll.startsWith(prefix))) {
      violations.push(`imports runtime DLL ${dll}`);
    }
    for (const symbol of symbols) {
      if (BLOCKED_SYMBOLS.includes(symbol)) violations.push(`${dll} -> ${symbol}`);
    }
  }

  if (violations.length) {
    process.stderr.write(`Windows 7 incompatible imports in ${binaryPath}:\n`);
    for (const violation of violations) process.stderr.write(`  - ${violation}\n`);
    process.exit(1);
  }
  process.stdout.write(`Windows 7 PE audit passed: ${binaryPath} (${dlls.size} imported modules)\n`);
}

const targets = process.argv.slice(2);
if (!targets.length) fail("usage: node scripts/assert-win7-pe-compat.mjs <binary> [<binary> ...]");
for (const target of targets) audit(target);
