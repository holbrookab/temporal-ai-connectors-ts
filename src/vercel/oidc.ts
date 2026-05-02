import { fromWebToken } from "@aws-sdk/credential-providers";
import type { AwsCredentialIdentity, AwsCredentialIdentityProvider } from "@smithy/types";
import { getVercelOidcToken } from "@vercel/oidc";

const VERCEL_OIDC_HEADER = "x-vercel-oidc-token";

type CachedCredentials = {
  credentials: AwsCredentialIdentity;
  expiresAt: number;
};

const cacheByRoleArn = new Map<string, CachedCredentials>();

export function getOidcTokenFromRequest(request?: Request): string | undefined {
  const token = request?.headers.get(VERCEL_OIDC_HEADER)?.trim();
  return token || undefined;
}

export async function resolveVercelOidcToken(request?: Request): Promise<string | undefined> {
  const envToken = process.env.VERCEL_OIDC_TOKEN?.trim();
  if (envToken) return envToken;

  const headerToken = getOidcTokenFromRequest(request);
  if (headerToken) return headerToken;

  try {
    const token = await getVercelOidcToken();
    return token?.trim() || undefined;
  } catch {
    return undefined;
  }
}

export function getVercelAWSCredentials(options: {
  request?: Request;
  roleArn?: string;
  env?: Record<string, string | undefined>;
} = {}): AwsCredentialIdentityProvider | undefined {
  const env = options.env ?? process.env;
  const roleArn = options.roleArn ?? env.AWS_ROLE_ARN;
  if (!roleArn) {
    if (env.VERCEL && !env.AWS_ACCESS_KEY_ID) {
      return async () => {
        throw new Error(
          "AWS credentials are not configured. Provide AWS_ROLE_ARN for Vercel OIDC or static AWS credentials.",
        );
      };
    }
    return undefined;
  }

  return async () => {
    const now = Date.now();
    const cached = cacheByRoleArn.get(roleArn);
    if (cached && cached.expiresAt > now + 60_000) {
      return cached.credentials;
    }

    const webIdentityToken = await resolveVercelOidcToken(options.request);
    if (!webIdentityToken) {
      throw new Error("Unable to resolve Vercel OIDC token for AWS role assumption");
    }

    const credentials = await fromWebToken({ roleArn, webIdentityToken })();
    if (credentials.expiration) {
      cacheByRoleArn.set(roleArn, {
        credentials,
        expiresAt: credentials.expiration.getTime(),
      });
    }
    return credentials;
  };
}

export async function resolveAWSCredentials(
  request?: Request,
): Promise<AwsCredentialIdentity | undefined> {
  const provider = getVercelAWSCredentials({ request });
  if (!provider) return undefined;
  return provider();
}
