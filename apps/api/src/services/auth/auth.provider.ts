import type { FastifyRequest } from "fastify";

import type { RateLimitScope } from "../../config/auth.config.js";
import {
  checkTieredRateLimit,
  type RateLimitDecision,
} from "./auth.rate-limit.js";
import {
  resolveRequestIdentity,
  type RequestIdentity,
} from "./auth.identity.js";

export interface AuthProvider {
  resolveIdentity(req: FastifyRequest): RequestIdentity;
  checkRateLimit(params: {
    req: FastifyRequest;
    scope: RateLimitScope;
  }): Promise<RateLimitDecision>;
}

class DefaultAuthProvider implements AuthProvider {
  resolveIdentity(req: FastifyRequest): RequestIdentity {
    return resolveRequestIdentity(req);
  }

  async checkRateLimit(params: {
    req: FastifyRequest;
    scope: RateLimitScope;
  }): Promise<RateLimitDecision> {
    return checkTieredRateLimit(params);
  }
}

export function createDefaultAuthProvider(): AuthProvider {
  return new DefaultAuthProvider();
}
