import type { NextAuthOptions } from "next-auth";
import KeycloakProvider from "next-auth/providers/keycloak";

export const authOptions: NextAuthOptions = {
  providers: [
    KeycloakProvider({
      clientId: process.env.KEYCLOAK_ID!,
      clientSecret: process.env.KEYCLOAK_SECRET!,
      issuer: process.env.KEYCLOAK_ISSUER!,
    }),
  ],
  callbacks: {
    async jwt({ token, account }) {
      // Persist the access_token so API routes can forward it to the Admin API.
      if (account) {
        token.accessToken = account.access_token;
        token.idToken = account.id_token;
        // Extract realm roles from the decoded access token.
        try {
          const payload = JSON.parse(
            Buffer.from(account.access_token!.split(".")[1], "base64url").toString()
          );
          token.realmRoles = (payload?.realm_access?.roles ?? []) as string[];
        } catch {
          token.realmRoles = [];
        }
      }
      return token;
    },
    async session({ session, token }) {
      session.accessToken = token.accessToken as string | undefined;
      session.realmRoles = (token.realmRoles ?? []) as string[];
      return session;
    },
  },
};

declare module "next-auth" {
  interface Session {
    accessToken?: string;
    realmRoles: string[];
  }
}
