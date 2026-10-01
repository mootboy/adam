(ns adam.knowledge.store)

(defprotocol FileEvidenceStore
  (ensure-file-evidence-schema! [store])
  (index-file-evidence! [store projection]))

(defprotocol AggregateMemoryStore
  (read-session-memory-streams! [store user-id session-id]))

(defprotocol FileMemoryQueryStore
  (query-file-memory! [store user-id repository-id relative-path limit]))

(defprotocol CodeMemoryMigrationStore
  (code-memory-version! [store user-id])
  (complete-code-memory-rebuild! [store user-id version]))

(defprotocol MemoryIdentityMigrationStore
  (memory-identity-version! [store user-id])
  (migrate-memory-identities! [store user-id target-version]))
