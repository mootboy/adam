(ns adam.knowledge.store)

(defprotocol FileEvidenceStore
  (ensure-file-evidence-schema! [store])
  (index-file-evidence! [store projection]))
