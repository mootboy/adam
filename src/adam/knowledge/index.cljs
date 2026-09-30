(ns adam.knowledge.index
  (:require [adam.knowledge.evidence :as evidence]
            [adam.knowledge.memory :as memory]
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
        embedded-memory
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
             paths)))
        write-projection!
        (fn [resolved projection]
          (when on-memory-diagnostics
            (on-memory-diagnostics (:memory-diagnostics projection)))
          (let [projection-write-started-at (now-ms)]
            (-> (knowledge-store/index-file-evidence! store projection)
                (.finally
                 (fn []
                   (measure! :neo4j-projection-ms projection-write-started-at)))
                (.then (fn [_] resolved)))))
        project!
        (fn [resolved]
          (let [session-id (identity/session-urn user-uuid (:pi-session-id summary))
                memory-read-started-at (now-ms)]
            (-> (memory/load-session-projection!
                 {:store store
                  :user-id (identity/user-urn user-uuid)
                  :session-id session-id
                  :source-kind identity/pi-source-kind
                  :source-session-id (:pi-session-id summary)
                  :embedded-projections [embedded-memory]})
                (.finally
                 (fn []
                   (measure! :neo4j-projection-ms memory-read-started-at)))
                (.then
                 (fn [aggregate-memory]
                   (let [projection-started-at (now-ms)
                         projection
                         (if resolved
                           (evidence/extract-projection
                            {:user-uuid user-uuid
                             :pi-session-id (:pi-session-id summary)
                             :session-id session-id
                             :cwd (:cwd summary)
                             :repository resolved
                             :entries @entries
                             :current-leaf-id selected-leaf
                             :memory-projection aggregate-memory})
                           (evidence/attach-memory-projection
                            {:extractor-version evidence/extractor-version
                             :user-id (identity/user-urn user-uuid)
                             :session-id session-id
                             :pi-session-id (:pi-session-id summary)
                             :source-kind identity/pi-source-kind
                             :source-session-id (:pi-session-id summary)
                             :repository nil :files [] :entry-file-evidence []
                             :available-source-entries
                             (set (map (fn [entry]
                                         [evidence/pi-implicit-stream (:entry-id entry)])
                                       @entries))}
                            user-uuid aggregate-memory))]
                     (measure! :evidence-extraction-ms projection-started-at)
                     (write-projection! resolved projection)))))))]
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
            (.then project!)
            (.finally
             (fn []
               (when on-timing (on-timing @timings)))))))))
