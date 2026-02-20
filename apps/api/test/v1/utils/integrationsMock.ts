import type { IntegrationProvider, IntegrationTokenResult, IntegrationProviderClient } from "../../../src/integrations/index.js";
import { vi } from "vitest";

interface IntegrationClientState {
  authorizeUrls: string[];
  revokedTokens: string[];
  exchangedStates: string[];
}

const stateByProvider = new Map<IntegrationProvider, IntegrationClientState>();
const tokenPairs = new Map<string, IntegrationTokenResult>();
const revokeTokenFailureByProvider = new Set<IntegrationProvider>();

const buildAuthorizeUrlMock = vi.fn(async (provider: IntegrationProvider, state: string, redirectUri: string) => {
  const call = getClientState(provider);
  call.authorizeUrls.push(redirectUri);
  return `https://auth.example/${provider}?state=${state}`;
});

const exchangeCodeMock = vi.fn(async (provider: IntegrationProvider, code: string, redirectUri: string) => {
  const call = getClientState(provider);
  call.exchangedStates.push(code);
  const existing = tokenPairs.get(code);
  if (existing) return existing;

  return {
    accessToken: `access-${provider}-${code}`,
    refreshToken: `refresh-${provider}-${code}`,
    expiresAt: new Date(Date.now() + 60 * 60 * 1000).toISOString(),
    scope: "read write",
    providerAccountId: `${provider}-acct`,
  };
});

const revokeTokenMock = vi.fn(async (provider: IntegrationProvider, token: string) => {
  if (revokeTokenFailureByProvider.has(provider)) {
    throw new Error("forced token revoke failure");
  }
  const call = getClientState(provider);
  call.revokedTokens.push(token);
});

const parseSourceUrlMock = vi.fn((provider: IntegrationProvider, sourceUrl: string) => ({
  sourceExternalId: `${provider}-${sourceUrl}`,
  sourceUrl,
}));

const fetchDocumentMock = vi.fn();

const isIntegrationProviderMock = vi.fn((value: string): value is IntegrationProvider => (
  value === "notion" || value === "google"
));

function getClientState(provider: IntegrationProvider): IntegrationClientState {
  if (!stateByProvider.has(provider)) {
    stateByProvider.set(provider, {
      authorizeUrls: [],
      revokedTokens: [],
      exchangedStates: [],
    });
  }
  return stateByProvider.get(provider)!;
}

function makeProviderClient(provider: IntegrationProvider): IntegrationProviderClient {
  return {
    provider,
    buildAuthorizeUrl(state: string, redirectUri: string) {
      return buildAuthorizeUrlMock(provider, state, redirectUri);
    },
    exchangeCode(code: string, redirectUri: string) {
      return exchangeCodeMock(provider, code, redirectUri);
    },
    refreshAccessToken(refreshToken: string) {
      return Promise.resolve({
        accessToken: `refreshed-${refreshToken}`,
        refreshToken: `refresh-${refreshToken}-next`,
        expiresAt: new Date(Date.now() + 60 * 60 * 1000).toISOString(),
        scope: "read write",
        providerAccountId: `${provider}-acct`,
      });
    },
    fetchDocument(sourceUrl: string) {
      return fetchDocumentMock(provider, sourceUrl);
    },
    parseSourceUrl(sourceUrl: string) {
      return parseSourceUrlMock(provider, sourceUrl);
    },
    revokeToken(token: string) {
      return revokeTokenMock(provider, token);
    },
  };
}

const getProviderClientMock = vi.fn((provider: IntegrationProvider) => {
  return makeProviderClient(provider);
});

export const integrationsMock = {
  isIntegrationProvider: isIntegrationProviderMock,
  getProviderClient: getProviderClientMock,
};

export function setIntegrationTokenFixture(code: string, token: IntegrationTokenResult) {
  tokenPairs.set(code, token);
}

export function clearIntegrationTokenFixtures() {
  tokenPairs.clear();
}

export function getIntegrationProviderState(provider: IntegrationProvider) {
  return getClientState(provider);
}

export function resetIntegrationMocks() {
  stateByProvider.clear();
  clearIntegrationTokenFixtures();
  revokeTokenFailureByProvider.clear();
  buildAuthorizeUrlMock.mockClear();
  exchangeCodeMock.mockClear();
  revokeTokenMock.mockClear();
  parseSourceUrlMock.mockClear();
  fetchDocumentMock.mockClear();
  isIntegrationProviderMock.mockClear();
  getProviderClientMock.mockClear();
}

export function createIntegrationsMockModule() {
  return {
    isIntegrationProvider: integrationsMock.isIntegrationProvider,
    getProviderClient: integrationsMock.getProviderClient,
  };
}

export function setRevokeTokenFailure(provider: IntegrationProvider, shouldFail = true) {
  if (shouldFail) {
    revokeTokenFailureByProvider.add(provider);
    return;
  }
  revokeTokenFailureByProvider.delete(provider);
}
