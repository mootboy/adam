(ns adam.knowledge.index
  (:require [adam.knowledge.evidence :as evidence]
            [adam.knowledge.repository :as repository]
            [adam.knowledge.store :as knowledge-store]
            [adam.replica.identity :as identity]
            [adam.replica.jsonl :as jsonl]
            [adam.sources.pi-observational-memory.adapter :as pom-adapter]))

(defn index-session!
  [{:keys [store path user-uuid repository resolve-repository current-leaf-id
           memory-adapter on-memory-diagnostics]
    :as input}]
  (let [entries (atom [])
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
      (js/Promise.resolve nil)
      (-> (if repository
            (js/Promise.resolve repository)
            (resolve-repository (:cwd summary)))
          (.then (fn [resolved]
                   (if resolved resolved (resolve-from-evidence!))))
          (.then
           (fn [resolved]
             (when resolved
               (let [projection
                     (evidence/extract-projection
                      {:user-uuid user-uuid
                       :pi-session-id (:pi-session-id summary)
                       :session-id (identity/session-urn user-uuid (:pi-session-id summary))
                       :cwd (:cwd summary)
                       :repository resolved
                       :entries @entries
                       :current-leaf-id selected-leaf
                       :memory-projection memory-projection})]
                 (when on-memory-diagnostics
                   (on-memory-diagnostics (:memory-diagnostics projection)))
                 (-> (knowledge-store/index-file-evidence! store projection)
                     (.then (fn [_] resolved)))))))))))
