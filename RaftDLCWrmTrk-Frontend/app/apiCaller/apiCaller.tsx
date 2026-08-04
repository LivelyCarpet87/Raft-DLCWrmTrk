import type { GetEndpointsResponse } from "~/types/apiResponses";

const DISCOVERY_URL = "http://10.0.9.8:8000/raft/endpoints";

const UNHEALTHY_FOR = 30_000;

const state = {
  endpoints: new Array<string>(),
  unhealthyUntil: new Map<string, number>(),

  index: 0,

  etag: undefined as string | undefined,
  refreshAfter: 0,
  refreshPromise: undefined as Promise<void> | undefined,
};

function parseMaxAge(header: string | null): number | undefined {
  if (!header) return;

  const match = header.match(/max-age=(\d+)/i);
  if (!match) return;

  return Number(match[1]) * 1000;
}

async function refreshEndpoints() {
  if (state.refreshPromise) return state.refreshPromise;

  state.refreshPromise = (async () => {

    try {
      const headers = new Headers();

      if (state.etag) {
        headers.set("If-None-Match", state.etag);
      }

      let getEndpointUrl = DISCOVERY_URL
      if (state.endpoints.length > 0) {
        getEndpointUrl = generateURL("/raft/endpoints")
      }
      const res = await fetch(DISCOVERY_URL, { headers });

      if (res.status === 304) {
        const maxAge =
          parseMaxAge(res.headers.get("Cache-Control")) ?? 60_000;

        state.refreshAfter = Date.now() + maxAge;
        return;
      }

      if (!res.ok) return;

      state.etag = res.headers.get("ETag") ?? undefined;

      const maxAge =
        parseMaxAge(res.headers.get("Cache-Control")) ?? 60_000;

      state.refreshAfter = Date.now() + maxAge;

      const resp = await parseResponse<GetEndpointsResponse>(res)

      if (resp === undefined) {
        return
      }

      const endpoints = resp!.endpoints

      if (endpoints.length) {
        state.endpoints = endpoints;
        state.index %= endpoints.length;

        // Remove health entries for deleted endpoints.
        for (const endpoint of [...state.unhealthyUntil.keys()]) {
          if (!endpoints.includes(endpoint)) {
            state.unhealthyUntil.delete(endpoint);
          }
        }
      }
    } catch {
      // Retry in 30 seconds.
      state.refreshAfter = Date.now() + 30_000;
    } finally {
      state.refreshPromise = undefined;
    }
  })();

  return state.refreshPromise;
}

function maybeRefreshEndpoints() {
  if (Date.now() >= state.refreshAfter && !state.refreshPromise) {
    void refreshEndpoints();
  }
}

function nextHealthyEndpoint(): string {
  const now = Date.now();

  for (let i = 0; i < state.endpoints.length; i++) {
    const endpoint = state.endpoints[state.index];
    state.index = (state.index + 1) % state.endpoints.length;

    const unhealthyUntil = state.unhealthyUntil.get(endpoint);

    if (!unhealthyUntil || unhealthyUntil <= now) {
      return endpoint;
    }
  }

  // All unhealthy: clear quarantines and try again.
  state.unhealthyUntil.clear();

  const endpoint = state.endpoints[state.index];
  state.index = (state.index + 1) % state.endpoints.length;

  return endpoint;
}

function markUnhealthy(endpoint: string) {
  state.unhealthyUntil.set(endpoint, Date.now() + UNHEALTHY_FOR);
}

export type ErrorInfo = {
  code: string;
  message: string;
};

export type ApiResponse<T> = {
  success: boolean;
  data?: T;
  error?: ErrorInfo;
};

export class ApiError extends Error {
  constructor(
    public info: ErrorInfo,
    public retryable = false,
  ) {
    super(info.message);
    this.name = "ApiError";
  }
}

export class HttpError extends Error {
  constructor(
    public status: number,
    message = `HTTP ${status}`
  ) {
    super(message);
    this.name = "HttpError";
  }
}

async function parseResponse<T>(
  res: Response
): Promise<T | undefined> {
  try {
    const body = (await res.json()) as ApiResponse<T>;

    if (!body.success) {
      throw new ApiError(
        body.error ?? {
          code: "UNKNOWN",
          message: "Unknown error",
        }
      );
    }

    return body.data;
  } catch (err) {
    if (err instanceof ApiError) {
      throw err;
    }

    throw new HttpError(
      res.status,
      `Invalid response (${res.status})`
    );
  }
}

async function request<T>(
  path: string,
  init?: RequestInit
): Promise<T | undefined> {
  maybeRefreshEndpoints();

  const tried = new Set<string>();

  endpointLoop: while (tried.size < state.endpoints.length) {
    const endpoint = nextHealthyEndpoint();

    if (tried.has(endpoint)) {
      continue;
    }

    tried.add(endpoint);

    let url = new URL(path, endpoint).toString();

    for (let redirects = 0; redirects < 2; redirects++) {
        const res = await fetch(url, {
          ...init,
          redirect: "manual",
        });

        if (
          redirects === 0 &&
          res.status >= 300 &&
          res.status < 400
        ) {
          const location = res.headers.get("Location");

          if (location) {
            url = new URL(location, url).toString();
            continue;
          }
        }
        
        try {
            return await parseResponse<T>(res);
        } catch (err) {
            if (
                err instanceof ApiError &&
                err.info.code === "FSM_READ_ERROR"
            ) {
                markUnhealthy(endpoint);
                continue endpointLoop;
            }

            throw err;
        }
    }
  }

  throw new Error("All endpoints failed.");
}

export function fetcher<T>(
  path: string
): Promise<T | undefined> {
  return request<T>(path);
}

export function postForm<TResponse>(
  path: string,
  values: Record<string, string>
) {
  return request<TResponse>(path, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
    },
    body: new URLSearchParams(values),
  });
}

export function postMultipart<TResponse>(
  path: string,
  form: FormData
) {
  return request<TResponse>(path, {
    method: "POST",
    body: form,
  });
}

export function generateURL(path:string){
    const endpoint = nextHealthyEndpoint();
    return new URL(path, endpoint).toString();
}