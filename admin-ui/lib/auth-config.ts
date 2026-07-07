// Copyright 2026 nexus-gateway contributors
// SPDX-License-Identifier: Apache-2.0

let warnedPartialConfig = false;

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
  const values = [
    process.env.KEYCLOAK_ISSUER,
    process.env.KEYCLOAK_ID,
    process.env.KEYCLOAK_SECRET,
  ];
  const setCount = values.filter(Boolean).length;
  // Only *some* of the three set is almost certainly a misconfiguration
  // (typo, incomplete .env, partial overlay) rather than an intentional
  // no-auth setup — that would silently leave the Admin UI unauthenticated
  // while the operator believes it is protected. Warn loudly, once.
  if (setCount > 0 && setCount < values.length && !warnedPartialConfig) {
    warnedPartialConfig = true;
    console.warn(
      "[auth-config] Partial Keycloak configuration detected: exactly one or two of " +
        "KEYCLOAK_ISSUER/KEYCLOAK_ID/KEYCLOAK_SECRET are set. Auth is DISABLED as a " +
        "result (all three are required to enable it). Set all three to require " +
        "Keycloak sign-in, or unset all three to run without auth intentionally."
    );
  }
  return setCount === values.length;
}
