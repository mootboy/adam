(ns adam.knowledge.index
  (:require [adam.knowledge.evidence :as evidence]
            [adam.knowledge.repository :as repository]
            [adam.knowledge.store :as knowledge-store]
            [adam.replica.identity :as identity]
            [adam.replica.jsonl :as jsonl]
            [adam.sources.pi-observational-memory.adapter :as pom-adapter]))

(defn index-session!
  [{:keys [store path user-uuid repository resolve-repository current-leaf-id
           memory-adapter on-memory-diagnostics on-timing now-ms]
    :as input}]
  (let [now-ms (or now-ms #(.now js/performance))
        timings (atom {:repository-discovery-ms 0
                       :evidence-extraction-ms 0
                       :neo4j-projection-ms 0})
        measure! (fn [key started-at]
                   (swap! timings update key + (max 0 (- (now-ms) started-at))))
        extraction-started-at (now-ms)
        entries (atom [])
        summary (jsonl/scan-file path {:on-entry #(swap! entries conj
                                                        {:entry-id (:entry-id %)
                                                         :raw-json (:raw-json %)})})
        selected-leaf (if (contains? input :current-leaf-id)
                        current-leaf-id
                        (:last-entry-id summary))
        selected-entries (evidence/selected-stored-entries @entries selected-leaf)
        memory-projection
        (try
          (pom-adapter/extract-memories (or memory-adapter pom-adapter/adapter)
                                        selected-entries)
          (catch :default _
            {:observations []
             :reflections []
             :diagnostics [{:producer "unknown" :reason :adapter-error}]}))
        _ (measure! :evidence-extraction-ms extraction-started-at)
        resolve-from-evidence!
        (fn []
          (let [paths (evidence/explicit-file-tool-paths @entries selected-leaf)]
            (reduce
             (fn [promise candidate-path]
               (.then promise
                      (fn [resolved]
                        (if resolved
                          resolved
                          (repository/resolve-from-file-path!
                           resolve-repository
                           (:cwd summary)
                           candidate-path)))))
             (js/Promise.resolve nil)
             paths)))]
    (if-not (:cwd summary)
      (do
        (when on-timing (on-timing @timings))
        (js/Promise.resolve nil))
      (let [repository-started-at (now-ms)]
        (-> (if repository
              (js/Promise.resolve repository)
              (resolve-repository (:cwd summary)))
            (.then (fn [resolved]
                     (if resolved resolved (resolve-from-evidence!))))
            (.finally
             (fn []
               (measure! :repository-discovery-ms repository-started-at)))
            (.then
             (fn [resolved]
               (if resolved
                 (let [projection-started-at (now-ms)
                       projection
                       (evidence/extract-projection
                        {:user-uuid user-uuid
                         :pi-session-id (:pi-session-id summary)
                         :session-id (identity/session-urn user-uuid (:pi-session-id summary))
                         :cwd (:cwd summary)
                         :repository resolved
                         :entries @entries
                         :current-leaf-id selected-leaf
                         :memory-projection memory-projection})]
                   (measure! :evidence-extraction-ms projection-started-at)
                   (when on-memory-diagnostics
                     (on-memory-diagnostics (:memory-diagnostics projection)))
                   (let [projection-write-started-at (now-ms)]
                     (-> (knowledge-store/index-file-evidence! store projection)
                         (.finally
                          (fn []
                            (measure! :neo4j-projection-ms projection-write-started-at)))
                         (.then (fn [_] resolved)))))
                 (knowledge-store/clear-file-evidence!
                  store
                  (identity/session-urn user-uuid (:pi-session-id summary))
                  evidence/extractor-version))))
            (.finally (fn [] (when on-timing (on-timing @timings)))))))))
