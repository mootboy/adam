(ns adam.sources.claude-code.reconcile
  (:require [adam.knowledge.evidence :as common-evidence]
            [adam.knowledge.repository :as repository]
            [adam.knowledge.store :as knowledge-store]
            [adam.sources.claude-code.evidence :as evidence]
            [adam.sources.claude-code.locators :as locators]
            [adam.sources.claude-code.scanner :as scanner]
            [adam.sources.claude-code.sync :as sync]
            [adam.replica.identity :as identity]))

(defn reconcile!
  [{:keys [locator-options notification user-uuid store sync!
           resolve-repository! ensure-file-schema! index-file-evidence!
           clear-file-evidence!]}]
  (let [locator (locators/record! locator-options notification)
        scan (scanner/scan-session
              {:session-id (:session-id locator)
               :transcript-path (:transcript-path locator)
               :subagents (:subagents locator)})
        sync! (or sync! sync/sync-session-scan!)
        ensure-file-schema! (or ensure-file-schema!
                                knowledge-store/ensure-file-evidence-schema!)
        index-file-evidence! (or index-file-evidence!
                                  knowledge-store/index-file-evidence!)
        clear-file-evidence! (or clear-file-evidence!
                                 knowledge-store/clear-file-evidence!)
        resolve-from-evidence!
        (fn []
          (reduce
           (fn [promise {:keys [cwd path]}]
             (.then promise
                    (fn [resolved]
                      (if resolved
                        resolved
                        (repository/resolve-from-file-path!
                         #(resolve-repository! % user-uuid) cwd path)))))
           (js/Promise.resolve nil)
           (evidence/explicit-file-locators scan)))]
    (-> (sync! {:store store :user-uuid user-uuid :scan scan})
        (.then
         (fn [sync-result]
           (if (= :conflict (:status sync-result))
             sync-result
             (-> (resolve-repository! (:cwd locator) user-uuid)
                 (.then (fn [resolved]
                          (if resolved resolved (resolve-from-evidence!))))
                 (.then
                  (fn [repository]
                    (if-not repository
                      (-> (clear-file-evidence!
                           store
                           (identity/session-urn user-uuid identity/claude-source-kind
                                                 (:session-id locator))
                           common-evidence/extractor-version)
                          (.then (fn [_]
                                   (assoc sync-result :projection-status :cleared))))
                      (let [projection (evidence/extract-projection
                                        {:user-uuid user-uuid
                                         :repository repository
                                         :scan scan})]
                        (-> (ensure-file-schema! store)
                            (.then (fn [_]
                                     (index-file-evidence! store projection)))
                            (.then (fn [_]
                                     (assoc sync-result
                                            :projection-status :projected)))))))))))))))
