import { beforeEach, describe, expect, it, vi } from "vitest";
import { buildV1TestApp } from "../utils/app.js";
import * as authMock from "../utils/authMock.js";
import * as cryptoMock from "../utils/cryptoMock.js";
import * as dbMock from "../utils/dbMock.js";
import * as integrationsMock from "../utils/integrationsMock.js";
import { ACTIVE_USER, FIXTURE_NOW } from "../utils/fixtures.js";

vi.mock("../../../src/auth.js", async () => {
  const mocked = await import("../utils/authMock.js");
  return mocked.getAuthMockModule();
});
vi.mock("../../../src/db.js", async () => {
  const mocked = await import("../utils/dbMock.js");
  return mocked.createDbModuleMock();
});
vi.mock("../../../src/integrations/index.js", async () => {
  const mocked = await import("../utils/integrationsMock.js");
  return mocked.createIntegrationsMockModule();
});
vi.mock("../../../src/integrations/crypto.js", async () => {
  const mocked = await import("../utils/cryptoMock.js");
  return mocked.createCryptoMockModule();
});

const AUTH_TOKEN = "token-owner";

function pastDate(): Date {
  return new Date(Date.now() - 60 * 60 * 1000);
}

function futureDate(): Date {
  return new Date(Date.now() + 60 * 60 * 1000);
}

describe("/v1 integrations", () => {
  beforeEach(() => {
    authMock.setAuthToken(AUTH_TOKEN, ACTIVE_USER);
    authMock.resetAuthMocks();
    authMock.setAuthToken(AUTH_TOKEN, ACTIVE_USER);
    dbMock.resetDbMock();
    integrationsMock.resetIntegrationMocks();
    cryptoMock.resetCryptoMocks();
  });

  describe("GET /v1/integrations", () => {
    it("returns disconnected entries when no integration rows exist", async () => {
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("FROM user_integrations")) {
          return dbMock.queryRows();
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "GET",
        url: "/v1/integrations",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      await app.close();

      expect(response.statusCode).toBe(200);
      expect(response.json()).toEqual({
        items: [
          { provider: "notion", status: "disconnected" },
          { provider: "google", status: "disconnected" },
        ],
      });
    });

    it("maps expiry transitions to needs_reauth and expired", async () => {
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("FROM user_integrations")) {
          return dbMock.queryRows(
            {
              provider: "notion",
              status: "connected",
              refresh_token_enc: "enc-refresh",
              token_expires_at: pastDate(),
            },
            {
              provider: "google",
              status: "connected",
              refresh_token_enc: null,
              token_expires_at: pastDate(),
            },
          );
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "GET",
        url: "/v1/integrations",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      await app.close();

      expect(response.statusCode).toBe(200);
      expect(response.json()).toEqual({
        items: [
          { provider: "notion", status: "needs_reauth" },
          { provider: "google", status: "expired" },
        ],
      });
    });
  });

  describe("provider authorize endpoints", () => {
    it("requires auth for authorize and rejects invalid providers", async () => {
      const appWithoutAuth = await buildV1TestApp();
      const missingAuth = await appWithoutAuth.inject({
        method: "GET",
        url: "/v1/integrations/notion/authorize",
      });
      const badProvider = await appWithoutAuth.inject({
        method: "GET",
        url: "/v1/integrations/invalid/authorize",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      const badProviderUrl = await appWithoutAuth.inject({
        method: "GET",
        url: "/v1/integrations/invalid/authorize-url",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      await appWithoutAuth.close();

      expect(missingAuth.statusCode).toBe(401);
      expect(badProvider.statusCode).toBe(400);
      expect(badProviderUrl.statusCode).toBe(400);
      expect(badProvider.json()).toEqual({ error: "Invalid provider" });
    });

    it("persists OAuth state and redirects for relative returnTo", async () => {
      let insertedReturnTo: string | null = null;
      dbMock.setQueryHandler(async (text, params) => {
        if (text.includes("INSERT INTO integration_oauth_states")) {
          insertedReturnTo = String(params?.[3] ?? "");
          return dbMock.queryRows();
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "GET",
        url: "/v1/integrations/notion/authorize?returnTo=/integrations/status",
        headers: {
          authorization: `Bearer ${AUTH_TOKEN}`,
          origin: "https://app.local",
          host: "app.local",
        },
      });
      await app.close();

      expect(response.statusCode).toBe(302);
      expect(response.headers.location).toContain("https://auth.example/notion?state=");
      expect(insertedReturnTo).toBe("https://app.local/integrations/status");
    });

    it("rejects cross-origin returnTo and falls back to origin", async () => {
      let insertedReturnTo: string | null = null;
      dbMock.setQueryHandler(async (text, params) => {
        if (text.includes("INSERT INTO integration_oauth_states")) {
          insertedReturnTo = String(params?.[3] ?? "");
          return dbMock.queryRows();
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const sameOrigin = await app.inject({
        method: "GET",
        url: "/v1/integrations/google/authorize?returnTo=https://app.local/dashboard",
        headers: {
          authorization: `Bearer ${AUTH_TOKEN}`,
          origin: "https://app.local",
        },
      });
      const crossOrigin = await app.inject({
        method: "GET",
        url: "/v1/integrations/google/authorize?returnTo=https://evil.example/steal",
        headers: {
          authorization: `Bearer ${AUTH_TOKEN}`,
          origin: "https://app.local",
        },
      });
      await app.close();

      expect(sameOrigin.statusCode).toBe(302);
      expect(crossOrigin.statusCode).toBe(302);
      expect(insertedReturnTo).toBe("https://app.local");
    });

    it("falls back for oversized returnTo", async () => {
      let insertedReturnTo: string | null = null;
      dbMock.setQueryHandler(async (text, params) => {
        if (text.includes("INSERT INTO integration_oauth_states")) {
          insertedReturnTo = String(params?.[3] ?? "");
          return dbMock.queryRows();
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "GET",
        url: `/v1/integrations/notion/authorize?returnTo=${"x".repeat(2000)}`,
        headers: {
          authorization: `Bearer ${AUTH_TOKEN}`,
          origin: "https://app.local",
        },
      });
      await app.close();

      expect(response.statusCode).toBe(302);
      expect(insertedReturnTo).toBe("https://app.local");
    });

    it("returns authorize-url JSON payload", async () => {
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("INSERT INTO integration_oauth_states")) {
          return dbMock.queryRows();
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "GET",
        url: "/v1/integrations/notion/authorize-url",
        headers: {
          authorization: `Bearer ${AUTH_TOKEN}`,
          origin: "https://app.local",
        },
      });
      await app.close();

      expect(response.statusCode).toBe(200);
      expect(response.json()).toEqual({
        url: expect.stringContaining("https://auth.example/notion?state="),
      });
    });
  });

  describe("GET /v1/integrations/:provider/callback", () => {
    it("requires code and state", async () => {
      const app = await buildV1TestApp();
      const response = await app.inject({ method: "GET", url: "/v1/integrations/notion/callback" });
      await app.close();

      expect(response.statusCode).toBe(400);
      expect(response.json()).toEqual({ error: "Missing code/state" });
    });

    it("rejects unknown state", async () => {
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("FROM integration_oauth_states")) {
          return dbMock.queryNoRows();
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "GET",
        url: "/v1/integrations/notion/callback?code=abc&state=missing-state",
      });
      await app.close();

      expect(response.statusCode).toBe(400);
      expect(response.json()).toEqual({ error: "Invalid OAuth state" });
    });

    it("cleans up expired states and rejects", async () => {
      let removed = false;
      dbMock.setQueryHandler(async (text, params) => {
        if (text.includes("DELETE FROM integration_oauth_states")) {
          removed = true;
          return dbMock.queryNoRows();
        }
        if (text.includes("FROM integration_oauth_states")) {
          return dbMock.queryRows({
            user_id: ACTIVE_USER.id,
            provider: "notion",
            return_to: "https://app.local",
            expires_at: pastDate(),
          });
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "GET",
        url: "/v1/integrations/notion/callback?code=abc&state=expired-state",
        headers: { host: "app.local" },
      });
      await app.close();

      expect(response.statusCode).toBe(400);
      expect(response.json()).toEqual({ error: "OAuth state expired" });
      expect(removed).toBe(true);
    });

    it("connects and redirects with integration query params", async () => {
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("FROM integration_oauth_states")) {
          return dbMock.queryRows({
            user_id: ACTIVE_USER.id,
            provider: "notion",
            return_to: "https://app.local/dashboard",
            expires_at: futureDate(),
          });
        }
        if (text.includes("INSERT INTO user_integrations")) {
          return dbMock.queryRows();
        }
        if (text.includes("DELETE FROM integration_oauth_states")) {
          return dbMock.queryRows();
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "GET",
        url: "/v1/integrations/notion/callback?code=abc&state=state-ok",
        headers: { host: "app.local" },
      });
      await app.close();

      expect(response.statusCode).toBe(303);
      expect(response.headers.location).toContain("integration=notion");
      expect(response.headers.location).toContain("integration_status=connected");
    });
  });

  describe("GET /v1/integrations/:provider/status", () => {
    it("returns disconnected when no row exists", async () => {
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("FROM user_integrations")) {
          return dbMock.queryNoRows();
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "GET",
        url: "/v1/integrations/google/status",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      await app.close();

      expect(response.statusCode).toBe(200);
      expect(response.json()).toEqual({ provider: "google", status: "disconnected" });
    });

    it("maps expired/disconnected and connected states", async () => {
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("FROM user_integrations")) {
          return dbMock.queryRows({
            status: "connected",
            refresh_token_enc: "enc-refresh",
            token_expires_at: new Date(Date.now() + 60 * 60 * 1000),
          });
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "GET",
        url: "/v1/integrations/google/status",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      await app.close();

      expect(response.statusCode).toBe(200);
      expect(response.json()).toEqual({ provider: "google", status: "connected" });
    });

    it("maps needs_reauth and expired statuses correctly", async () => {
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("FROM user_integrations")) {
          return dbMock.queryRows({
            status: "connected",
            refresh_token_enc: "enc-refresh",
            token_expires_at: new Date(Date.now() - 60 * 60 * 1000),
          });
        }
        return dbMock.queryNoRows();
      });
      const needReauthApp = await buildV1TestApp();
      const needReauth = await needReauthApp.inject({
        method: "GET",
        url: "/v1/integrations/notion/status",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      await needReauthApp.close();

      dbMock.setQueryHandler(async (text) => {
        if (text.includes("FROM user_integrations")) {
          return dbMock.queryRows({
            status: "connected",
            refresh_token_enc: null,
            token_expires_at: new Date(Date.now() - 60 * 60 * 1000),
          });
        }
        return dbMock.queryNoRows();
      });
      const expiredApp = await buildV1TestApp();
      const expired = await expiredApp.inject({
        method: "GET",
        url: "/v1/integrations/google/status",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      await expiredApp.close();

      expect(needReauth.statusCode).toBe(200);
      expect(needReauth.json()).toEqual({ provider: "notion", status: "needs_reauth" });
      expect(expired.statusCode).toBe(200);
      expect(expired.json()).toEqual({ provider: "google", status: "expired" });
    });

    it("maps disconnected row to disconnected status", async () => {
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("FROM user_integrations")) {
          return dbMock.queryRows({
            status: "disconnected",
            refresh_token_enc: "enc-refresh",
            token_expires_at: new Date(Date.now() + 60 * 60 * 1000),
          });
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "GET",
        url: "/v1/integrations/google/status",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      await app.close();

      expect(response.statusCode).toBe(200);
      expect(response.json()).toEqual({ provider: "google", status: "disconnected" });
    });
  });

  describe("POST /v1/integrations/:provider/disconnect", () => {
    it("returns disconnected when no token is stored", async () => {
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("SELECT access_token_enc")) {
          return dbMock.queryNoRows();
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "POST",
        url: "/v1/integrations/google/disconnect",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      await app.close();

      expect(response.statusCode).toBe(200);
      expect(response.json()).toEqual({ provider: "google", status: "disconnected" });
      expect(integrationsMock.getIntegrationProviderState("google").revokedTokens).toEqual([]);
    });

    it("revokes stored tokens before marking disconnected", async () => {
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("SELECT access_token_enc")) {
          return dbMock.queryRows({
            access_token_enc: "enc::access",
            refresh_token_enc: "enc::refresh",
          });
        }
        if (text.includes("UPDATE user_integrations")) {
          return dbMock.queryRows();
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "POST",
        url: "/v1/integrations/notion/disconnect",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      await app.close();

      const state = integrationsMock.getIntegrationProviderState("notion");
      expect(response.statusCode).toBe(200);
      expect(response.json()).toEqual({ provider: "notion", status: "disconnected" });
      expect(state.revokedTokens).toEqual(["access", "refresh"]);
    });

    it("swallows revoke errors but still returns disconnected", async () => {
      integrationsMock.setRevokeTokenFailure("google", true);
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("SELECT access_token_enc")) {
          return dbMock.queryRows({
            access_token_enc: "enc::access",
            refresh_token_enc: null,
          });
        }
        if (text.includes("UPDATE user_integrations")) {
          return dbMock.queryRows();
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "POST",
        url: "/v1/integrations/google/disconnect",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      await app.close();

      expect(response.statusCode).toBe(200);
      expect(response.json()).toEqual({ provider: "google", status: "disconnected" });
    });
  });
});
