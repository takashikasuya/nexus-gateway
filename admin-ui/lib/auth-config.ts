// Copyright 2026 nexus-gateway contributors
// SPDX-License-Identifier: Apache-2.0

/**
 * Whether Keycloak OIDC auth is enabled for the Admin UI. Auth is optional
 * and off by default (docker compose up needs no Keycloak, no login step):
 * it turns on only when all three Keycloak client env vars are configured,
 * mirroring the gateway's own opt-in JWT auth (KEYCLOAK_JWKS_URL presence,
 * see cmd/gateway/main.go).
 *
 * Kept dependency-free (no next-auth import) so it is safe to use from the
 * Edge-runtime middleware as well as from server-side route handlers.
 */
export function isAuthEnabled(): boolean {
  return Boolean(
    process.env.KEYCLOAK_ISSUER &&
      process.env.KEYCLOAK_ID &&
      process.env.KEYCLOAK_SECRET
  );
}
