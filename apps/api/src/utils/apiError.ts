// src/utils/apiError.ts

import type { FastifyReply } from "fastify";

/**
 * Canonical API error codes.
 * MUST stay in sync with routes and services.
 */
export type ApiErrorCode =
  | "INVALID_REQUEST_BODY"
  | "INVALID_CREATE_UPLOAD_REQUEST"
  | "INVALID_FILE_SIZE"
  | "FILE_TOO_LARGE"
  | "INVALID_FILENAME"
  | "INVALID_CONTENT_TYPE"
  | "INVALID_CHUNK_SIZE"
  | "INVALID_TOTAL_CHUNKS"
  | "TOO_MANY_CHUNKS"
  | "UPLOAD_CAPACITY_REACHED"
  | "INVALID_UPLOAD_ID"
  | "UPLOAD_NOT_FOUND"
  | "UPLOAD_ALREADY_COMPLETED"
  | "UPLOAD_INCOMPLETE"
  | "UPLOAD_FINALIZATION_IN_PROGRESS"
  | "INVALID_CHUNK"
  | "CHUNK_STREAM_ERROR"
  | "CHUNK_UPLOAD_FAILED"
  | "SESSION_CREATE_FAILED"
  | "UPLOAD_FAILED"
  | "INVALID_EPOCHS"
  | "RATE_LIMITED"
  | "INTERNAL_ERROR";

export interface ApiErrorResponse {
  error: {
    code: ApiErrorCode;
    message: string;
    retryable: boolean;
    details?: Record<string, unknown>;
  };
}

export function sendApiError(
  reply: FastifyReply,
  statusCode: number,
  code: ApiErrorCode,
  message: string,
  options?: {
    retryable?: boolean;
    details?: Record<string, unknown>;
  }
) {
  const safeStatus =
    Number.isInteger(statusCode) && statusCode >= 400 && statusCode <= 599
      ? statusCode
      : 500;

  const response: ApiErrorResponse = {
    error: {
      code,
      message,
      retryable: options?.retryable ?? false,
      ...(options?.details && { details: options.details }),
    },
  };

  return reply.code(safeStatus).send(response);
}
