/** @type {import('next').NextConfig} */
const nextConfig = {
  // output: 'standalone', // Only needed for custom Docker deployments
  experimental: {
    workerThreads: false,
  },
};

module.exports = nextConfig;
