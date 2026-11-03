/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  // mysql2 and mongodb are native-ish server packages; keep them out of
  // the bundler so their dynamic requires resolve at runtime.
  serverExternalPackages: ["mysql2", "mongodb"],
};

module.exports = nextConfig;
