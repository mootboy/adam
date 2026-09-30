(ns adam.memory.store)

(defprotocol MemoryStreamStore
  (ensure-memory-schema! [store])
  (get-memory-stream-checkpoint! [store stream-id])
  (write-memory-record-batch! [store request])
  (complete-memory-stream! [store request])
  (mark-memory-stream-conflict! [store conflict])
  (read-memory-records! [store stream-id]))
