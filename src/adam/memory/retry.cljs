(ns adam.memory.retry)

(def default-source-wait-ms 600000)
(def max-source-wait-ms 86400000)

(defn validate-source-wait-ms [value]
  (when-not (and (js/Number.isSafeInteger value)
                 (<= 1000 value max-source-wait-ms))
    (throw (ex-info "ADAM_MEMORY_SOURCE_WAIT_MS must be an integer from 1000 to 86400000"
                    {:reason :invalid-source-wait})))
  value)

(defn resolve-source-wait-ms [raw]
  (if (nil? raw)
    default-source-wait-ms
    (do
      (when-not (and (string? raw) (re-matches #"[1-9][0-9]*" raw))
        (throw (ex-info "ADAM_MEMORY_SOURCE_WAIT_MS must be an integer from 1000 to 86400000"
                        {:reason :invalid-source-wait})))
      (validate-source-wait-ms (js/Number raw)))))

(defn notification-age-ms [notification now-epoch-ms]
  ;; The spool is written on this host: mtime uses Adam's clock rather than a
  ;; producer's possibly skewed queuedAt. Neither basis resets on worker restart.
  (let [mtime (:file-mtime-ms notification)
        effective-queued-at (if (js/Number.isFinite mtime)
                              mtime (.parse js/Date (:queued-at notification)))]
    (max 0 (- now-epoch-ms effective-queued-at))))
