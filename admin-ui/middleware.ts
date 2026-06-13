export { default } from "next-auth/middleware";

export const config = {
  // Protect all routes except NextAuth internals, API proxy routes (handle own auth), and static assets.
  matcher: ["/((?!api/auth|api/gateway|_next/static|_next/image|favicon.ico).*)"],
};
