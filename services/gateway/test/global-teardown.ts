export default async function globalTeardown() {
  const containers: { stop(): Promise<unknown> }[] = (globalThis as any).__E2E_CONTAINERS__ ?? [];
  await Promise.all(containers.map((c) => c.stop()));
}
