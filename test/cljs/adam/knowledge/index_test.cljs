(ns adam.knowledge.index-test
  (:require [adam.knowledge.index :as index]
            [adam.knowledge.store :as store]
            [adam.sources.pi-observational-memory.adapter :as memory-adapter]
            [cljs.test :refer [async deftest is]]
            ["node:fs" :refer [mkdtempSync rmSync writeFileSync]]
            ["node:os" :refer [tmpdir]]
            ["node:path" :refer [join]]))

(defrecord RecordingEvidenceStore [projections]
  store/FileEvidenceStore
  (ensure-file-evidence-schema! [_] (js/Promise.resolve nil))
  (index-file-evidence! [_ projection]
    (swap! projections conj projection)
    (js/Promise.resolve nil)))

(deftest workspace-session-discovers-repository-from-selected-explicit-file-evidence
  (async done
    (let [directory (mkdtempSync (join (tmpdir) "adam-evidence-index-"))
          path (join directory "session.jsonl")
          projections (atom [])
          diagnostics (atom nil)
          candidates (atom [])
          repository {:id "urn:adam:repository:user-1:hash"
                      :user-id "urn:adam:user:user-1"
                      :root "/work/repo"
                      :commit "main-head"
                      :branch "main"
                      :dirty? false
                      :worktrees [{:root "/work/repo" :commit "main-head"
                                   :branch "main" :dirty? false}
                                  {:root "/work/trees/feature" :commit "feature-head"
                                   :branch "feature/a" :dirty? true}]}
          resolve-repository
          (fn [candidate]
            (swap! candidates conj candidate)
            (js/Promise.resolve
             (when (= candidate "/work/trees/feature") repository)))]
      (writeFileSync
       path
       (str "{\"type\":\"session\",\"id\":\"session-1\",\"cwd\":\"/workspace\"}\n"
            "{\"type\":\"message\",\"id\":\"assistant-1\",\"parentId\":null,\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"toolCall\",\"id\":\"call-1\",\"name\":\"edit\",\"arguments\":{\"path\":\"/work/trees/feature/src/a.cljs\"}}]}}\n")
       "utf8")
      (-> (index/index-session!
           {:store (->RecordingEvidenceStore projections)
            :path path
            :user-uuid "user-1"
            :resolve-repository resolve-repository
            :current-leaf-id "assistant-1"
            :memory-adapter
            (reify memory-adapter/MemorySourceAdapter
              (extract-memories [_ selected]
                {:observations [{:producer "fake" :adapter-version 1
                                 :memory-id "aaaaaaaaaaaa" :content "fake memory"
                                 :timestamp "2026-01-01T00:00:00.000Z"
                                 :relevance "high" :token-count 2
                                 :recording-entry-id "memory-1"
                                 :source-entry-ids ["assistant-1"]
                                 :dropped? false}]
                 :reflections []
                 :diagnostics [{:producer "fake" :entry-id (-> selected first :entry-id)
                                :reason :test-diagnostic}]}))
            :on-memory-diagnostics #(reset! diagnostics %)})
          (.then
           (fn [resolved]
             (is (= repository resolved))
             (is (= "/workspace" (first @candidates)))
             (is (= ["src/a.cljs"]
                    (mapv :relative-path (:files (first @projections)))))
             (is (= "feature-head"
                    (get-in (first @projections)
                            [:entry-file-evidence 0 :commit])))
             (is (= [(get-in (first @projections) [:files 0 :id])]
                    (get-in (first @projections) [:observations 0 :file-ids])))
             (is (= :test-diagnostic (get-in @diagnostics [0 :reason])))
             (rmSync directory #js {:recursive true :force true})
             (done)))
          (.catch
           (fn [error]
             (rmSync directory #js {:recursive true :force true})
             (is false (.-stack error))
             (done)))))))
