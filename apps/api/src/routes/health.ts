// src/routes/health.ts

import { FastifyInstance } from "fastify";
import { getRedis } from "../state/client.js";

export default async function healthRoute(app: FastifyInstance) {
  app.get("/health", async (req, reply) => {
    const start = Date.now();
    const timestamp = new Date().toISOString();

    let redisOk = false;
    let latencyMs: number | null = null;

    try {
      const redis = getRedis();
      await redis.ping();
      redisOk = true;
      latencyMs = Date.now() - start;
    } catch (err) {
      req.log.error({ err }, "Redis Health Check Failed");
    }

    return reply.status(redisOk ? 200 : 503).send({
      status: redisOk ? "UP" : "DOWN",
      service: "floe-api-v1",
      ready: redisOk,
      timestamp,
      checks: {
        redis: {
          ok: redisOk,
          latencyMs,
          timestamp,
        },
      },
    });
  });
}
