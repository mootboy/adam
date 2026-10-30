(ns adam.replica.processing
  "Data-only reconciliation processor. No filesystem, Git, database or Pi calls.
   The thread wrapper owns cached state; only commit acknowledgements are durable."
  (:require [adam.knowledge.evidence :as evidence]
            [adam.knowledge.memory :as memory]
            [adam.replica.identity :as identity]
            [adam.replica.jsonl :as jsonl]
            [adam.replica.model :as model]
            [adam.replica.sync :as sync]
            [adam.sources.pi-observational-memory.adapter :as adapter]))

(defn advance [state {:keys [op records path user-uuid checkpoint index repositories streams]
                      :as event}]
  (case op
    :start {:state {:path path :scan nil} :result nil}
    :records
    (do
      (when (and (contains? event :byte-start)
                 (not= (:byte-start event) (or (get-in state [:scan :offset]) 0)))
        (throw (js/Error. "processing record range has a gap or replay")))
      {:state (update state :scan #(reduce (partial jsonl/scan-record (:path state)) % records))
       :result nil})
    :finish
    (let [summary (jsonl/scan-summary (:path state) (:scan state))
          _ (when (and (contains? event :expected-bytes)
                       (not= (:expected-bytes event) (:log-bytes summary)))
              (throw (js/Error. "processing record range is incomplete")))
          entries (get-in state [:scan :entries])
          leaf (if (contains? event :current-leaf-id)
                 (:current-leaf-id event) (:last-entry-id summary))
          _ (when (and (some? leaf) (not (contains? (get-in state [:scan :seen]) leaf)))
              (throw (js/Error. "selected leaf is not persisted in the source snapshot")))]
      {:state (assoc (dissoc state :scan) :summary summary :entries entries :leaf leaf)
       :result {:summary summary}})
    :prepare
    (let [summary (:summary state) entries (:entries state) leaf (:leaf state)
          selected (evidence/selected-stored-entries entries leaf)
          embedded (try (adapter/extract-memories adapter/adapter selected)
                        (catch :default _
                          {:observations [] :reflections []
                           :diagnostics [{:producer "unknown" :reason :adapter-error}]}))
          locators (mapv #(hash-map :cwd (:cwd summary) :path %)
                         (evidence/explicit-file-tool-paths entries leaf))]
      {:state (assoc state :embedded embedded)
       :result {:summary summary :file-locators locators}})
    :plan
    (let [parent (when-let [bytes (:parent-header event)]
                   (try
                     (let [scan (jsonl/scan-record "parent header" nil {:bytes bytes :newline? false})
                           header (:header scan)
                           id (aget header "id")]
                       (when (not= id (get-in state [:summary :pi-session-id]))
                         {:pi-session-id id :session-id (identity/session-urn user-uuid id)}))
                     (catch :default _ nil)))
          options (cond-> {:user-uuid user-uuid :source-file (:path state)}
                    (contains? event :current-leaf-id) (assoc :current-leaf-id (:current-leaf-id event))
                    parent (assoc :parent parent))
          session (model/session-from-summary (:summary state) options)
          plan (sync/plan-sync (:summary state) (:entries state) checkpoint
                               (or (:batch-bytes event) sync/default-batch-bytes))]
      {:state (assoc state :plan plan :user-uuid user-uuid)
       :result (assoc (dissoc plan :batches) :session session
                      :batch-count (count (:batches plan))
                      :completion (or (:completion plan) checkpoint))})
    :batch
    (let [batch (get-in state [:plan :batches index])]
      (when-not batch (throw (js/Error. "unknown processing batch")))
      {:state state
       :result (update batch :entries
                       #(mapv (partial model/entry-from-scan (:user-uuid state)
                                       (get-in state [:summary :pi-session-id])) %))})
    :projection
    (let [session-id (identity/session-urn user-uuid (get-in state [:summary :pi-session-id]))
          ;; Enum strings cross the JS thread boundary; raw native records are
          ;; not reparsed or recursively keywordized on the Pi thread.
          streams (mapv #(update % :records
                                 (fn [records]
                                   (mapv (fn [record]
                                           (update record :semantic-status keyword)) records))) streams)
          aggregate (memory/aggregate-projection
                      {:user-id (identity/user-urn user-uuid)
                       :source-kind identity/pi-source-kind
                       :source-session-id (get-in state [:summary :pi-session-id])
                       :embedded-projections [(:embedded state)] :streams streams})
          projection (evidence/extract-projection
                       {:user-uuid user-uuid
                        :pi-session-id (get-in state [:summary :pi-session-id])
                        :session-id session-id :cwd (get-in state [:summary :cwd])
                        :repository (first repositories) :repositories repositories
                        :entries (:entries state) :current-leaf-id (:leaf state)
                        :memory-projection aggregate})]
      {:state state
       :result (assoc projection
                      :expected-source {:log-hash (get-in state [:summary :log-hash])
                                        :log-bytes (get-in state [:summary :log-bytes])
                                        :current-leaf-id (:leaf state)}
                      :expected-memory-streams
                      (mapv (fn [stream]
                              {:id (:id stream) :count (count (:records stream))
                               :ordinal (when (seq (:records stream))
                                          (apply max (map :ordinal (:records stream))))}) streams))})
    :release {:state nil :result nil}
    (throw (js/Error. "unknown processing event"))))
