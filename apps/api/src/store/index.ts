// src/store/index.ts

import { DiskChunkStore } from "./disk.chunk.store.js";

export const chunkStore = new DiskChunkStore();

export * from "./video.store.js";
export * from "./chunk.store.js";

