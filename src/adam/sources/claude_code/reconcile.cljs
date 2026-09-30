(ns adam.sources.claude-code.reconcile
  (:require [adam.knowledge.evidence :as common-evidence]
            [adam.knowledge.memory :as memory]
            [adam.knowledge.repository :as repository]
            [adam.knowledge.store :as knowledge-store]
            [adam.sources.claude-code.evidence :as evidence]
            [adam.sources.claude-code.locators :as locators]
            [adam.sources.claude-code.scanner :as scanner]
            [adam.sources.claude-code.store :as claude-store]
            [adam.sources.claude-code.sync :as sync]
            [adam.replica.identity :as identity]
            ["node:fs" :refer [existsSync]]))

(declare reconcile-present!)

(defn reconcile!
  [{:keys [notification] :as options}]
  ;; A SessionStart can precede the transcript's first record, and Claude
  ;; deletes finished subagent transcripts; both are complete as far as this
  ;; notification is concerned, so it is acknowledged rather than retried.
  (if (existsSync (:transcript-path notification))
    (reconcile-present! options)
    (js/Promise.resolve {:status :missing-transcript
                         :transcript-path (:transcript-path notification)})))

(defn- with-mirrored-missing-streams!
  "Evidence is rebuilt for the whole session from one projection, so a retained
  subagent stream whose transcript Claude has removed contributes its mirrored
  raw records instead of a file scan."
  [read-stream-entries! store user-uuid scan]
  (reduce
   (fn [promise {:keys [stream-id agent-id]}]
     (.then promise
            (fn [projection-scan]
              (-> (read-stream-entries!
                   store
                   (identity/stream-urn user-uuid identity/claude-source-kind
                                        (:source-session-id scan) stream-id))
                  (.then
                   (fn [entries]
                     (-> projection-scan
                         (update :streams conj {:stream-id stream-id
                                                :agent-id agent-id
                                                :mirrored-only? true
                                                :entries entries})
                         (update :entries into entries))))))))
   (js/Promise.resolve scan)
   (:missing-streams scan)))

(defn- reconcile-present!
  [{:keys [locator-options notification user-uuid store sync!
           resolve-repository! ensure-file-schema! index-file-evidence!
           read-stream-entries!]}]
  (let [locator (locators/record! locator-options notification)
        scan (scanner/scan-session
              {:session-id (:session-id locator)
               :transcript-path (:transcript-path locator)
               :subagents (:subagents locator)})
        sync! (or sync! sync/sync-session-scan!)
        read-stream-entries! (or read-stream-entries! claude-store/read-stream-entries!)
        ensure-file-schema! (or ensure-file-schema!
                                knowledge-store/ensure-file-evidence-schema!)
        index-file-evidence! (or index-file-evidence!
                                  knowledge-store/index-file-evidence!)
        resolve-from-evidence!
        (fn [projection-scan]
          (reduce
           (fn [promise {:keys [cwd path]}]
             (.then promise
                    (fn [resolved]
                      (if resolved
                        resolved
                        (repository/resolve-from-file-path!
                         #(resolve-repository! % user-uuid) cwd path)))))
           (js/Promise.resolve nil)
           (evidence/explicit-file-locators projection-scan)))]
    (-> (sync! {:store store :user-uuid user-uuid :scan scan})
        (.then
         (fn [sync-result]
           (if (= :conflict (:status sync-result))
             sync-result
             (-> (with-mirrored-missing-streams! read-stream-entries! store user-uuid scan)
                 (.then
                  (fn [projection-scan]
                    (-> (resolve-repository! (:cwd locator) user-uuid)
                        (.then (fn [resolved]
                                 (if resolved resolved (resolve-from-evidence! projection-scan))))
                        (.then (fn [repository] [projection-scan repository])))))
                 (.then
                  (fn [[projection-scan repository]]
                    (let [session-id
                          (identity/session-urn user-uuid identity/claude-source-kind
                                                (:session-id locator))
                          base-projection
                          (if repository
                            (evidence/extract-projection
                             {:user-uuid user-uuid
                              :repository repository
                              :scan projection-scan})
                            {:extractor-version common-evidence/extractor-version
                             :adapter-version evidence/adapter-version
                             :user-id (identity/user-urn user-uuid)
                             :session-id session-id
                             :source-kind identity/claude-source-kind
                             :source-session-id (:session-id locator)
                             :repository nil :files [] :entry-file-evidence []
                             :available-source-entries
                             (set (map (juxt :stream-id :entry-id)
                                       (:entries projection-scan)))})]
                      (-> (memory/load-session-projection!
                           {:store store
                            :user-id (identity/user-urn user-uuid)
                            :session-id session-id
                            :source-kind identity/claude-source-kind
                            :source-session-id (:session-id locator)
                            :embedded-projections []})
                          (.then
                           (fn [aggregate-memory]
                             (common-evidence/attach-memory-projection
                              base-projection user-uuid aggregate-memory)))
                          (.then
                           (fn [projection]
                             (-> (ensure-file-schema! store)
                                 (.then (fn [_]
                                          (index-file-evidence! store projection))))))
                          (.then (fn [_]
                                   (assoc sync-result
                                          :projection-status :projected))))))))))))))
