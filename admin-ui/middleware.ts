// Copyright 2026 nexus-gateway contributors
// SPDX-License-Identifier: Apache-2.0

import { withAuth } from "next-auth/middleware";
import { NextResponse } from "next/server";
import { isAuthEnabled } from "@/lib/auth-config";

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
