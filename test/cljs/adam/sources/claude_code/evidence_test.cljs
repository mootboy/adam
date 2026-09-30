(ns adam.sources.claude-code.evidence-test
  (:require [adam.knowledge.evidence :as common-evidence]
            [adam.sources.claude-code.evidence :as evidence]
            [adam.sources.claude-code.scanner :as scanner]
            [cljs.test :refer [deftest is]]
            ["node:path" :as node-path]))

(def user-uuid "00000000-0000-4000-8000-000000000001")
(def main-fixture (.resolve node-path "test/fixtures/claude/main.jsonl"))
(def subagent-fixture (.resolve node-path "test/fixtures/claude/subagent.jsonl"))

(def repository
  (common-evidence/build-repository
   {:user-uuid user-uuid
    :root "/work/repo"
    :remote "git@github.com:mootboy/adam.git"
    :commit "main-head"
    :branch "main"
    :dirty? false
    :worktrees [{:root "/work/repo" :commit "main-head"
                 :branch "main" :dirty? false}
                {:root "/work/tree" :commit "feature-head"
                 :branch "feature/claude" :dirty? true}]}))

(deftest derives-only-native-file-tool-and-matched-result-evidence
  (let [scan (scanner/scan-session
              {:session-id "claude-session-1"
               :transcript-path main-fixture
               :subagents [{:agent-id "agent-1"
                            :transcript-path subagent-fixture}]})
        projection (evidence/extract-projection
                    {:user-uuid user-uuid
                     :repository repository
                     :scan scan})]
    (is (= "urn:adam:session:00000000-0000-4000-8000-000000000001:claude-code:claude-session-1"
           (:session-id projection)))
    (is (= ["src/edit.cljs" "src/read.cljs" "src/subagent.cljs" "src/write.cljs"]
           (mapv :relative-path (:files projection))))
    (is (= ["a-request-1" "a-request-2" "a-write" "r-edit"
            "r-read" "r-write" "sa-read" "sa-result"]
           (mapv :entry-id (:entry-file-evidence projection))))
    (is (= {:entry-id "a-request-1"
            :stream-id "main"
            :file-id (:id (common-evidence/resolve-repository-file
                           repository "/work/repo" "src/read.cljs"))
            :commit "main-head" :branch "main" :dirty? false}
           (first (:entry-file-evidence projection))))
    (is (= #{["feature-head" "feature/claude" true]}
           (->> (:entry-file-evidence projection)
                (remove #(= "main-head" (:commit %)))
                (map (juxt :commit :branch :dirty?))
                set)))
    (is (empty? (:observations projection)))
    (is (empty? (:reflections projection)))))

(deftest refuses-to-attach-a-result-to-an-ambiguous-duplicate-tool-id
  (let [assistant-raw
        (js/JSON.stringify
         #js {:type "assistant" :uuid "assistant" :parentUuid nil
              :sessionId "claude-session-1" :cwd "/work/repo"
              :message #js {:role "assistant"
                            :content #js [#js {:type "tool_use" :id "duplicate"
                                               :name "Read"
                                               :input #js {:file_path "src/one.cljs"}}
                                          #js {:type "tool_use" :id "duplicate"
                                               :name "Read"
                                               :input #js {:file_path "src/two.cljs"}}]}})
        result-raw
        (js/JSON.stringify
         #js {:type "user" :uuid "result" :parentUuid "assistant"
              :sessionId "claude-session-1" :cwd "/work/repo"
              :sourceToolAssistantUUID "assistant"
              :message #js {:role "user"
                            :content #js [#js {:type "tool_result"
                                               :tool_use_id "duplicate"}]}})
        entries [{:entry-id "assistant" :record-uuid "assistant"
                  :stream-id "main" :parent-id nil :cwd "/work/repo"
                  :raw-json assistant-raw}
                 {:entry-id "result" :record-uuid "result"
                  :stream-id "main" :parent-id "assistant" :cwd "/work/repo"
                  :raw-json result-raw}]
        scan {:source-session-id "claude-session-1"
              :current-leaf-id "result"
              :streams [{:stream-id "main" :entries entries}]}
        projection (evidence/extract-projection
                    {:user-uuid user-uuid :repository repository :scan scan})]
    (is (= ["assistant" "assistant"]
           (mapv :entry-id (:entry-file-evidence projection))))
    (is (= [{:reason :duplicate-tool-use-id
             :tool-use-id "duplicate"
             :entry-ids ["assistant" "assistant"]}]
           (:memory-diagnostics projection)))))
