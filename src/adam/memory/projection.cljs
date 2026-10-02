(ns adam.memory.projection
  (:require [adam.knowledge.evidence :as evidence]
            [adam.knowledge.memory :as memory]
            [adam.replica.identity :as identity]
            [adam.sources.pi-observational-memory.adapter :as adapter]))

(defn from-retained [{:keys [user-uuid source-kind source-session-id current-leaf-id
                            entries entry-file-evidence streams]}]
  (let [user-id (identity/user-urn user-uuid)
        embedded (if (= source-kind identity/pi-source-kind)
                   [(adapter/extract-memories adapter/adapter
                      (evidence/selected-stored-entries entries current-leaf-id))]
                   [])
        aggregate (memory/aggregate-projection
                   {:user-id user-id :source-kind source-kind
                    :source-session-id source-session-id
                    :embedded-projections embedded :streams streams})]
    (evidence/attach-memory-projection
      {:user-id user-id
       :session-id (identity/session-urn user-uuid source-kind source-session-id)
       :source-kind source-kind :source-session-id source-session-id
       :extractor-version evidence/extractor-version
       :entry-file-evidence entry-file-evidence
       :available-source-entries (set (map (fn [entry]
                                            [(or (:stream-id entry) "main") (:entry-id entry)]) entries))}
      user-uuid aggregate)))
