// Copyright 2026 nexus-gateway contributors
// SPDX-License-Identifier: Apache-2.0

import { withAuth } from "next-auth/middleware";
import { NextResponse } from "next/server";
import { isAuthEnabled } from "@/lib/auth-config";

// Force the Node.js middleware runtime (not the Edge runtime). This matters
// here specifically: KEYCLOAK_* env vars are set at container *start* time
// (docker-compose `environment:`), not at `next build` time (the Docker
// image is built once and reused for both the no-auth default and the
// opt-in Keycloak overlay). The Edge runtime's bundler can statically inline
// `process.env.*` at build time, which would freeze isAuthEnabled() to
// whatever was (not) set during the image build, defeating the overlay.
// The Node.js runtime reads process.env at true request/startup time.
export const runtime = "nodejs";

// Auth is optional and off by default (see lib/auth-config.ts). When no
// Keycloak env vars are configured, pages render directly with no login
// gate — there is no provider to redirect to anyway. When configured, the
// standard NextAuth middleware enforces a session on every matched route.
export default isAuthEnabled()
  ? withAuth({})
  : function passThrough() {
      return NextResponse.next();
    };

export const config = {
  // Protect all routes except NextAuth internals, API proxy routes (handle own auth), and static assets.
  matcher: ["/((?!api/auth|api/gateway|_next/static|_next/image|favicon.ico).*)"],
};
