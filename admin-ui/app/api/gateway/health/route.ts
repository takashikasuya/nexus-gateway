// Copyright 2026 nexus-gateway contributors
// SPDX-License-Identifier: Apache-2.0

import { resolveAdminApiToken } from "@/lib/auth";
import { getHealth } from "@/lib/api";
import { NextResponse } from "next/server";

export async function GET() {
  const auth = await resolveAdminApiToken();
  if (auth.unauthorized) {
    return NextResponse.json({ error: "unauthorized" }, { status: 401 });
  }
  try {
    const health = await getHealth(auth.token);
    return NextResponse.json(health);
  } catch (err) {
    return NextResponse.json({ error: String(err) }, { status: 502 });
  }
}
