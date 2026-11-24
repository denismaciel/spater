// Only the Start server needs the Rust listener address.
export function upstreamOrigin(): string {
  return new URL(process.env.SPATER_OPS_API_URL || 'http://127.0.0.1:8410').origin
}
