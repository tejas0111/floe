export type FloeApiErrorBody = {
  error?: {
    code?: string;
    message?: string;
    retryable?: boolean;
    details?: unknown;
  };
  message?: string;
};

export class FloeError extends Error {
  override readonly cause?: unknown;

  constructor(message: string, cause?: unknown) {
    super(message);
    this.name = "FloeError";
    this.cause = cause;
  }
}

export class FloeApiError extends FloeError {
  readonly status: number;
  readonly code?: string;
  readonly retryable?: boolean;
  readonly details?: unknown;
  readonly requestId?: string;
  readonly raw?: unknown;

  constructor(params: {
    message: string;
    status: number;
    code?: string;
    retryable?: boolean;
    details?: unknown;
    requestId?: string;
    raw?: unknown;
  }) {
    super(params.message);
    this.name = "FloeApiError";
    this.status = params.status;
    this.code = params.code;
    this.retryable = params.retryable;
    this.details = params.details;
    this.requestId = params.requestId;
    this.raw = params.raw;
  }
}

export function isFloeApiError(err: unknown): err is FloeApiError {
  return err instanceof FloeApiError;
}
