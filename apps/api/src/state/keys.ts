// src/state/keys.ts

const PREFIX = "floe:v1";

const key = (suffix: string) => `${PREFIX}:${suffix}`;

export const uploadKeys = {
  session: (uploadId: string) =>
    key(`upload:${uploadId}:session`),

  chunks: (uploadId: string) =>
    key(`upload:${uploadId}:chunks`),

  meta: (uploadId: string) =>
    key(`upload:${uploadId}:meta`),

  // GC index (single source of truth)
  gcIndex: () =>
    key("upload:gc:active"),

  dedupe: (userId: string, fileHash: string) =>
    key(`upload:dedupe:${userId}:${fileHash}`),
};

export const videoKeys = {
  video: (videoId: string) =>
    key(`video:${videoId}`),

  blobIndex: (blobId: string) =>
    key(`video:blob:${blobId}`),

  all: () =>
    key("video:all"),
};

