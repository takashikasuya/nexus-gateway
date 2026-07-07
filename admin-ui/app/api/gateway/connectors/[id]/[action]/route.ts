// Copyright 2026 nexus-gateway contributors
// SPDX-License-Identifier: Apache-2.0

import { resolveAdminApiToken } from "@/lib/auth";
import { connectorAction } from "@/lib/api";
import { NextRequest, NextResponse } from "next/server";

export async function POST(
  req: NextRequest,
  { params }: { params: Promise<{ id: string; action: string }> }
) {
  const auth = await resolveAdminApiToken();
  if (auth.unauthorized) {
    return NextResponse.json({ error: "unauthorized" }, { status: 401 });
  }
  const { id, action } = await params;
  const image = req.nextUrl.searchParams.get("image") ?? undefined;
  try {
    await connectorAction(auth.token, id, action, image);
    return new NextResponse(null, { status: 204 });
  } catch (err) {
    return NextResponse.json({ error: String(err) }, { status: 502 });
  }
}
