(ns adam.memory.model
  (:require [adam.replica.identity :as identity]))

(defn from-scan [user-uuid scan]
  (let [stream-id (identity/memory-stream-urn
                   user-uuid (:source-kind scan) (:source-session-id scan)
                   (:producer-id scan))
        stream {:id stream-id
                :user-id (identity/user-urn user-uuid)
                :session-id (identity/session-urn
                             user-uuid (:source-kind scan) (:source-session-id scan))
                :source-kind (:source-kind scan)
                :source-session-id (:source-session-id scan)
                :producer-id (:producer-id scan)
                :path (:path scan)}]
    {:stream stream
     :records
     (mapv (fn [record]
             (cond-> {:id (identity/memory-record-urn stream-id (:ordinal record))
                      :stream-id stream-id
                      :ordinal (:ordinal record)
                      :byte-offset (:byte-offset record)
                      :next-byte-offset (:next-byte-offset record)
                      :raw-json (:raw-json record)
                      :payload-hash (:payload-hash record)
                      :payload-bytes (:payload-bytes record)
                      :prefix-hash (:prefix-hash record)
                      :semantic-status (:semantic-status record)
                      :diagnostics (:diagnostics record)}
               (:event-id record) (assoc :event-id (:event-id record))
               (:event-hash record) (assoc :event-hash (:event-hash record))
               (:kind record) (assoc :kind (:kind record))))
           (:records scan))}))
