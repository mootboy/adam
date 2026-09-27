(ns adam.sources.claude-code.model
  (:require [adam.replica.identity :as identity]
            [adam.replica.model :as replica-model]))

(defn- entry-from-scan [user-uuid source-session-id scanned]
  (let [entry-urn #(identity/entry-urn user-uuid identity/claude-source-kind
                                        source-session-id %)
        value (js/JSON.parse (:raw-json scanned))
        message (aget value "message")
        role (when (and message (= "object" (goog/typeOf message)))
               (aget message "role"))]
    (cond-> {:id (entry-urn (:entry-id scanned))
             :entry-id (:entry-id scanned)
             :source-kind identity/claude-source-kind
             :source-session-id source-session-id
             :stream-id (:stream-id scanned)
             :parent-id (:parent-id scanned)
             :logical-parent-id (:logical-parent-id scanned)
             :ordinal (:ordinal scanned)
             :byte-offset (:byte-offset scanned)
             :next-byte-offset (:next-byte-offset scanned)
             :prefix-hash (:prefix-hash scanned)
             :raw-json (:raw-json scanned)
             :payload-hash (:payload-hash scanned)
             :payload-bytes (:payload-bytes scanned)}
      (:record-uuid scanned) (assoc :record-uuid (:record-uuid scanned))
      (:agent-id scanned) (assoc :agent-id (:agent-id scanned))
      (:cwd scanned) (assoc :cwd (:cwd scanned))
      (:request-id scanned) (assoc :request-id (:request-id scanned))
      (:timestamp scanned) (assoc :timestamp (:timestamp scanned))
      (string? role) (assoc :role role)
      (:parent-id scanned) (assoc :parent-urn (entry-urn (:parent-id scanned)))
      (:logical-parent-id scanned)
      (assoc :logical-parent-urn (entry-urn (:logical-parent-id scanned))))))

(defn from-scan [user-uuid scan]
  (let [source-session-id (:source-session-id scan)
        session-id (identity/session-urn user-uuid identity/claude-source-kind
                                         source-session-id)
        streams (mapv (fn [stream]
                        (assoc (select-keys stream [:path :stream-id :agent-id :entry-count
                                                   :log-hash :log-bytes :source-bytes
                                                   :incomplete-tail-bytes :has-final-newline?])
                               :id (identity/stream-urn
                                    user-uuid identity/claude-source-kind
                                    source-session-id (:stream-id stream))
                               :session-id session-id))
                      (:streams scan))]
    {:session {:id session-id
               :user-id (identity/user-urn user-uuid)
               :source-kind identity/claude-source-kind
               :source-session-id source-session-id
               :current-leaf-id (:current-leaf-id scan)
               :source-file (get-in scan [:streams 0 :path])
               :streams streams
               :writer-version replica-model/writer-version}
     :streams streams
     :entries (mapv #(entry-from-scan user-uuid source-session-id %) (:entries scan))}))
