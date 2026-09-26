(ns adam.knowledge.store)

(defprotocol FileEvidenceStore
  (ensure-file-evidence-schema! [store])
  (index-file-evidence! [store projection]))

(defprotocol FileMemoryQueryStore
  (query-file-memory! [store user-id repository-id relative-path limit]))

(defprotocol CodeMemoryMigrationStore
  (code-memory-version! [store user-id])
  (complete-code-memory-rebuild! [store user-id version]))
