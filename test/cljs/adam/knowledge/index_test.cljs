(ns adam.knowledge.index-test
  (:require [adam.knowledge.index :as index]
            [adam.knowledge.store :as store]
            [adam.sources.pi-observational-memory.adapter :as memory-adapter]
            [cljs.test :refer [async deftest is]]
            ["node:fs" :refer [mkdtempSync rmSync writeFileSync]]
            ["node:os" :refer [tmpdir]]
            ["node:path" :refer [join]]))

(defrecord RecordingEvidenceStore [projections clears streams]
  store/AggregateMemoryStore
  (read-session-memory-streams! [_ _ _] (js/Promise.resolve @streams))
  store/FileEvidenceStore
  (ensure-file-evidence-schema! [_] (js/Promise.resolve nil))
  (index-file-evidence! [_ projection]
    (swap! projections conj projection)
    (js/Promise.resolve nil))
  (clear-file-evidence! [_ session-id extractor-version]
    (swap! clears conj [session-id extractor-version])
    (js/Promise.resolve nil)))

(deftest workspace-session-discovers-repository-from-selected-explicit-file-evidence
  (async done
    (let [directory (mkdtempSync (join (tmpdir) "adam-evidence-index-"))
          path (join directory "session.jsonl")
          projections (atom [])
          diagnostics (atom nil)
          timings (atom nil)
          clock (atom -5)
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
           {:store (->RecordingEvidenceStore projections (atom []) (atom []))
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
            :on-memory-diagnostics #(reset! diagnostics %)
            :now-ms (fn [] (swap! clock + 5))
            :on-timing #(reset! timings %)})
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
             (is (every? pos?
                         ((juxt :repository-discovery-ms
                                :evidence-extraction-ms
                                :neo4j-projection-ms)
                          @timings)))
             (rmSync directory #js {:recursive true :force true})
             (done)))
          (.catch
           (fn [error]
             (rmSync directory #js {:recursive true :force true})
             (is false (.-stack error))
             (done)))))))

(deftest aggregates-mirrored-sidecar-memory-with-embedded-memory
  (async done
    (let [directory (mkdtempSync (join (tmpdir) "adam-aggregate-index-"))
          path (join directory "session.jsonl")
          projections (atom [])
          stream-event
          #js {:protocolVersion 1 :eventId "event-sidecar" :kind "observations.recorded"
               :recordedAt "2026-01-01T00:00:00.000Z"
               :producer #js {:id "sidecar-producer" :version "1.0.0"}
               :source #js {:kind "pi" :sessionId "session-aggregate"}
               :sourceCheckpoint
               #js {:streams #js [#js {:streamId "main" :committedBytes 1
                                       :prefixSha256 (apply str (repeat 64 "a"))
                                       :selectedLeafEntryId "assistant-1"}]}
               :observations
               #js [#js {:id "bbbbbbbbbbbb" :content "sidecar memory"
                          :timestamp "2026-01-01T00:00:00.000Z" :relevance "high"
                          :tokenCount 2
                          :sourceEntries #js [#js {:streamId "main"
                                                  :entryId "assistant-1"}]}
                    #js {:id "cccccccccccc" :content "unresolved memory"
                          :timestamp "2026-01-01T00:00:01.000Z" :relevance "medium"
                          :tokenCount 2
                          :sourceEntries #js [#js {:streamId "main"
                                                  :entryId "not-mirrored-yet"}]}]}
          streams
          (atom [{:producer-id "sidecar-producer"
                  :records [{:id "memory-record-1" :ordinal 0
                             :semantic-status :accepted
                             :raw-json (js/JSON.stringify stream-event)}]}])
          memory-store (->RecordingEvidenceStore projections (atom []) streams)
          repository {:id "urn:adam:repository:user-1:hash"
                      :root "/work/repo" :commit "head" :branch "main" :dirty? false
                      :worktrees [{:root "/work/repo" :commit "head"
                                   :branch "main" :dirty? false}]}]
      (writeFileSync
       path
       (str "{\"type\":\"session\",\"id\":\"session-aggregate\",\"cwd\":\"/work/repo\"}\n"
            "{\"type\":\"message\",\"id\":\"assistant-1\",\"parentId\":null,\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"toolCall\",\"id\":\"call-1\",\"name\":\"read\",\"arguments\":{\"path\":\"src/a.cljs\"}}]}}\n")
       "utf8")
      (-> (index/index-session!
           {:store memory-store :path path :user-uuid "user-1" :repository repository
            :resolve-repository (fn [_] (js/Promise.resolve repository))
            :memory-adapter
            (reify memory-adapter/MemorySourceAdapter
              (extract-memories [_ _]
                {:producer "embedded-producer" :adapter-version 1
                 :observations [{:producer "embedded-producer" :adapter-version 1
                                 :memory-id "aaaaaaaaaaaa" :content "embedded memory"
                                 :timestamp "2026-01-01T00:00:00.000Z"
                                 :relevance "high" :token-count 2
                                 :recording-entry-id "memory-entry"
                                 :source-entry-ids ["assistant-1"] :dropped? false}]
                 :reflections [] :diagnostics []}))})
          (.then
           (fn [_]
             (let [projection (first @projections)]
               (is (= [["embedded-producer" "embedded memory"]
                       ["sidecar-producer" "sidecar memory"]
                       ["sidecar-producer" "unresolved memory"]]
                      (mapv (juxt :producer :content) (:observations projection))))
               (is (= [1 1 0] (mapv #(count (:file-ids %)) (:observations projection))))
               (is (= {:producer "sidecar-producer" :memory-id "cccccccccccc"
                       :stream-id "main" :entry-id "not-mirrored-yet"
                       :reason :unresolved-reference}
                      (first (:memory-diagnostics projection)))) )
             (rmSync directory #js {:recursive true :force true})
             (done)))
          (.catch
           (fn [error]
             (rmSync directory #js {:recursive true :force true})
             (is false (.-stack error))
             (done)))))))

(deftest unresolved-repository-projects-memory-without-file-evidence
  (async done
    (let [directory (mkdtempSync (join (tmpdir) "adam-evidence-clear-"))
          path (join directory "session.jsonl")
          projections (atom [])
          clears (atom [])
          memory-store (->RecordingEvidenceStore projections clears (atom []))]
      (writeFileSync
       path
       "{\"type\":\"session\",\"id\":\"session-clear\",\"cwd\":\"/not-git\"}\n"
       "utf8")
      (-> (index/index-session!
           {:store memory-store
            :path path
            :user-uuid "user-1"
            :resolve-repository (fn [_] (js/Promise.resolve nil))
            :memory-adapter
            (reify memory-adapter/MemorySourceAdapter
              (extract-memories [_ _]
                {:producer "embedded-producer" :adapter-version 1
                 :observations [{:producer "embedded-producer" :adapter-version 1
                                 :memory-id "aaaaaaaaaaaa" :content "memory without repo"
                                 :timestamp "2026-01-01T00:00:00.000Z"
                                 :relevance "high" :token-count 1
                                 :recording-entry-id "memory-entry"
                                 :source-entry-ids ["missing-source"] :dropped? false}]
                 :reflections [] :diagnostics []}))})
          (.then
           (fn [resolved]
             (is (nil? resolved))
             (is (empty? @clears))
             (is (= "urn:adam:session:user-1:pi:session-clear"
                    (:session-id (first @projections))))
             (is (nil? (:repository (first @projections))))
             (is (= "memory without repo"
                    (get-in (first @projections) [:observations 0 :content])))
             (is (= :unresolved-reference
                    (get-in (first @projections) [:memory-diagnostics 0 :reason])))
             (rmSync directory #js {:recursive true :force true})
             (done)))
          (.catch
           (fn [error]
             (rmSync directory #js {:recursive true :force true})
             (is false (.-stack error))
             (done)))))))
