export { default } from "next-auth/middleware";

export const config = {
  // Protect all routes except NextAuth internals and static assets.
  matcher: ["/((?!api/auth|_next/static|_next/image|favicon.ico).*)"],
};
