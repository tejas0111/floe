// src/store/video.store.ts

import crypto from "crypto";
import { getRedis } from "../state/client.js";
import { videoKeys } from "../state/keys.js";

export interface Video {
  videoId: string;
  blobId: string;
  filename: string;
  sizeBytes: number;
  status: "processing" | "ready" | "failed";
  createdAt: number;
}

export async function createVideo(input: {
  blobId: string;
  filename: string;
  sizeBytes: number;
  status?: Video["status"];
}): Promise<Video> {
  const redis = getRedis();

  const video: Video = {
    videoId: crypto.randomUUID(),
    blobId: input.blobId,
    filename: input.filename,
    sizeBytes: input.sizeBytes,
    status: input.status ?? "ready",
    createdAt: Date.now(),
  };

  const tx = await redis
    .multi()
    .hset(videoKeys.video(video.videoId), {
      ...video,
      sizeBytes: String(video.sizeBytes),
      createdAt: String(video.createdAt),
    })
    .set(videoKeys.blobIndex(video.blobId), video.videoId)
    .sadd(videoKeys.all(), video.videoId)
    .exec();

  if (!tx) {
    throw new Error("REDIS_VIDEO_CREATE_FAILED");
  }

  return video;
}

export async function getVideo(videoId: string): Promise<Video | null> {
  const redis = getRedis();
  const data = await redis.hgetall<Record<string, string>>(
    videoKeys.video(videoId)
  );

  if (!data || !Object.keys(data).length) return null;

  return {
    videoId,
    blobId: data.blobId,
    filename: data.filename,
    sizeBytes: Number(data.sizeBytes),
    status: data.status as Video["status"],
    createdAt: Number(data.createdAt),
  };
}

export async function getVideoByBlobId(
  blobId: string
): Promise<Video | null> {
  const redis = getRedis();
  const videoId = await redis.get<string>(
    videoKeys.blobIndex(blobId)
  );

  if (!videoId) return null;
  return getVideo(videoId);
}

export async function listVideos(): Promise<Video[]> {
  const redis = getRedis();
  const ids = await redis.smembers(videoKeys.all());
  if (!ids.length) return [];

  const pipeline = redis.pipeline();
  for (const id of ids) {
    pipeline.hgetall(videoKeys.video(id));
  }

  const res = await pipeline.exec();

  return res
    .map((entry) => {
    const [err, data] = entry as [any, any];
      if (err || !data || !Object.keys(data).length) return null;
      return {
        videoId: data.videoId,
        blobId: data.blobId,
        filename: data.filename,
        sizeBytes: Number(data.sizeBytes),
        status: data.status,
        createdAt: Number(data.createdAt),
      } as Video;
    })
    .filter(Boolean) as Video[];
}

