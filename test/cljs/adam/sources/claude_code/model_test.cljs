(ns adam.sources.claude-code.model-test
  (:require [adam.sources.claude-code.model :as model]
            [adam.sources.claude-code.scanner :as scanner]
            [cljs.test :refer [deftest is]]
            ["node:path" :as node-path]))

(def user-uuid "00000000-0000-4000-8000-000000000001")
(def main-fixture (.resolve node-path "test/fixtures/claude/main.jsonl"))
(def subagent-fixture (.resolve node-path "test/fixtures/claude/subagent.jsonl"))

(deftest adapts-a-scan-to-source-scoped-session-and-entry-models
  (let [scan (scanner/scan-session
              {:session-id "claude-session-1"
               :transcript-path main-fixture
               :subagents [{:agent-id "agent-1"
                            :transcript-path subagent-fixture}]})
        projection (model/from-scan user-uuid scan)
        session (:session projection)
        by-entry-id (into {} (map (juxt :entry-id identity) (:entries projection)))]
    (is (= "urn:adam:session:00000000-0000-4000-8000-000000000001:claude-code:claude-session-1"
           (:id session)))
    (is (= "claude-code" (:source-kind session)))
    (is (= "claude-session-1" (:source-session-id session)))
    (is (= "r-write" (:current-leaf-id session)))
    (is (nil? (:pi-session-id session)))
    (is (= "urn:adam:entry:00000000-0000-4000-8000-000000000001:claude-code:claude-session-1:r-read"
           (get-in by-entry-id ["r-read" :id])))
    (is (= "urn:adam:entry:00000000-0000-4000-8000-000000000001:claude-code:claude-session-1:u-root"
           (get-in by-entry-id ["r-read" :parent-urn])))
    (is (= "urn:adam:entry:00000000-0000-4000-8000-000000000001:claude-code:claude-session-1:r-edit"
           (get-in by-entry-id ["compact-1" :logical-parent-urn])))
    (is (= "agent:agent-1" (get-in by-entry-id ["sa-read" :stream-id])))
    (is (= ["urn:adam:stream:00000000-0000-4000-8000-000000000001:claude-code:claude-session-1:main"
            "urn:adam:stream:00000000-0000-4000-8000-000000000001:claude-code:claude-session-1:agent:agent-1"]
           (mapv :id (:streams projection))))))
