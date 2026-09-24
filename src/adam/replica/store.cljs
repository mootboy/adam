(ns adam.replica.store)

(defprotocol SessionReplicaStore
  (initialize! [store user])
  (get-checkpoint! [store session-id])
  (write-batch! [store request])
  (complete-session! [store request])
  (mark-conflict! [store conflict])
  (list-sessions! [store query])
  (read-session! [store session-id])
  (close! [store]))

(defn immutable-entry-conflict [existing incoming]
  (ex-info
   (str "immutable entry payload mismatch for " (:entry-id incoming))
   {:type :immutable-entry-conflict
    :entry-id (:entry-id incoming)
    :expected-hash (:payload-hash existing)
    :actual-hash (:payload-hash incoming)}))

(defn immutable-entry-conflict? [error]
  (= :immutable-entry-conflict (:type (ex-data error))))

(defn assert-entry-compatible! [existing incoming]
  (when (or (not= (:payload-hash existing) (:payload-hash incoming))
            (and (contains? existing :raw-json)
                 (contains? incoming :raw-json)
                 (not= (:raw-json existing) (:raw-json incoming))))
    (throw (immutable-entry-conflict existing incoming)))
  incoming)
