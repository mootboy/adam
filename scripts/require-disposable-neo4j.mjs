export function requireDisposableNeo4j(environment = process.env) {
  if (environment.ADAM_TEST_NEO4J_DISPOSABLE !== "1") {
    throw new Error("Live tests require ADAM_TEST_NEO4J_DISPOSABLE=1 and a disposable Neo4j instance. Never use production.");
  }
  if (!["ADAM_TEST_NEO4J_URI", "ADAM_TEST_NEO4J_USERNAME", "ADAM_TEST_NEO4J_PASSWORD"].every((key) => environment[key])) {
    throw new Error("Disposable live tests require ADAM_TEST_NEO4J_URI, USERNAME and PASSWORD.");
  }
}

if (process.argv[1]?.endsWith("require-disposable-neo4j.mjs")) {
  try { requireDisposableNeo4j(); }
  catch (error) { console.error(error.message); process.exitCode = 1; }
}
