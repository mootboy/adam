(ns adam.sources.claude-code.store)

(defprotocol ClaudeTranscriptStore
  (ensure-claude-schema! [store])
  (get-stream-checkpoint! [store stream-id])
  (read-stream-entries! [store stream-id])
  (write-stream-batch! [store request])
  (complete-stream! [store request])
  (mark-stream-conflict! [store conflict])
  (complete-claude-session! [store request]))
