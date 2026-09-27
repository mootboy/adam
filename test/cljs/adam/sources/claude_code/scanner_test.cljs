(ns adam.sources.claude-code.scanner-test
  (:require [adam.replica.sync :as sync]
            [adam.sources.claude-code.scanner :as scanner]
            [cljs.test :refer [deftest is]]
            [clojure.string :as string]
            ["node:fs" :refer [appendFileSync copyFileSync mkdtempSync readFileSync rmSync writeFileSync]]
            ["node:os" :refer [tmpdir]]
            ["node:path" :as node-path]))

(def main-fixture (.resolve node-path "test/fixtures/claude/main.jsonl"))
(def subagent-fixture (.resolve node-path "test/fixtures/claude/subagent.jsonl"))

(deftest scans-authoritative-parent-and-child-streams-losslessly
  (let [scan (scanner/scan-session
              {:session-id "claude-session-1"
               :transcript-path main-fixture
               :subagents [{:agent-id "agent-1"
                            :transcript-path subagent-fixture}]})
        parent (first (:streams scan))
        child (second (:streams scan))]
    (is (= "claude-session-1" (:source-session-id scan)))
    (is (= ["main" "agent:agent-1"] (mapv :stream-id (:streams scan))))
    (is (= 13 (:entry-count parent)))
    (is (= 3 (:entry-count child)))
    (is (= "r-write" (:current-leaf-id scan)))
    (is (= "agent-1" (:agent-id child)))
    (is (= "{\"type\":\"future-operational-record\",\"sessionId\":\"claude-session-1\",\"unknown\":{\"kept\":true}}"
           (:raw-json (last (:entries parent)))))
    (is (= ["u-root" "a-request-1" "r-read"]
           (mapv :entry-id (take 3 (:entries parent)))))
    (is (re-matches #"urn:adam:claude-record:[a-f0-9]{64}"
                    (:entry-id (nth (:entries parent) 6))))))

(deftest skips-a-subagent-stream-whose-transcript-was-removed
  (let [scan (scanner/scan-session
              {:session-id "claude-session-1"
               :transcript-path main-fixture
               :subagents [{:agent-id "agent-gone"
                            :transcript-path (.join node-path (tmpdir) "adam-missing-subagent.jsonl")}
                           {:agent-id "agent-1"
                            :transcript-path subagent-fixture}]})]
    (is (= ["main" "agent:agent-1"] (mapv :stream-id (:streams scan))))
    (is (= [{:stream-id "agent:agent-gone"
             :agent-id "agent-gone"
             :transcript-path (.join node-path (tmpdir) "adam-missing-subagent.jsonl")}]
           (:missing-streams scan)))))

(deftest reconstructs-compacted-parallel-current-context
  (let [scan (scanner/scan-session
              {:session-id "claude-session-1"
               :transcript-path main-fixture
               :subagents [{:agent-id "agent-1"
                            :transcript-path subagent-fixture}]})
        selected (scanner/selected-entries scan)
        selected-ids (set (map :entry-id selected))]
    (is (contains? selected-ids "compact-1"))
    (is (contains? selected-ids "r-edit"))
    (is (contains? selected-ids "a-request-1"))
    (is (contains? selected-ids "a-request-2"))
    (is (contains? selected-ids "r-read"))
    (is (contains? selected-ids "sa-read"))
    (is (not (contains? selected-ids "abandoned")))))

(deftest defers-an-incomplete-trailing-record-without-changing-stable-identities
  (let [directory (mkdtempSync (.join node-path (tmpdir) "adam-claude-scan-"))
        path (.join node-path directory "session.jsonl")]
    (try
      (copyFileSync main-fixture path)
      (appendFileSync path "{\"type\":\"assistant\",\"uuid\":\"partial" "utf8")
      (let [partial (scanner/scan-stream
                     {:session-id "claude-session-1"
                      :transcript-path path})
            stable-ids (mapv :entry-id (:entries partial))]
        (is (= 13 (:entry-count partial)))
        (is (pos? (:incomplete-tail-bytes partial)))
        (is (< (:log-bytes partial) (:source-bytes partial)))
        (writeFileSync path
                       (str (readFileSync main-fixture "utf8")
                            "{\"type\":\"future\",\"uuid\":\"completed\",\"parentUuid\":\"r-write\",\"sessionId\":\"claude-session-1\"}\n")
                       "utf8")
        (let [complete (scanner/scan-stream
                        {:session-id "claude-session-1"
                         :transcript-path path})]
          (is (= 14 (:entry-count complete)))
          (is (= stable-ids (mapv :entry-id (take 13 (:entries complete)))))))
      (finally
        (rmSync directory #js {:recursive true :force true})))))

(deftest detects-changed-prefixes-and-file-shrinkage-from-stream-checkpoints
  (let [directory (mkdtempSync (.join node-path (tmpdir) "adam-claude-prefix-"))
        path (.join node-path directory "session.jsonl")]
    (try
      (copyFileSync main-fixture path)
      (let [initial (scanner/scan-stream
                     {:session-id "claude-session-1" :transcript-path path})
            checkpoint (sync/checkpoint-for-entry (last (:entries initial)))
            changed (string/replace-first (readFileSync path "utf8")
                                          "\"content\":\"start\""
                                          "\"content\":\"other\"")]
        (writeFileSync path changed "utf8")
        (let [rescanned (scanner/scan-stream
                         {:session-id "claude-session-1" :transcript-path path})]
          (is (= :conflict (:status (sync/plan-sync rescanned (:entries rescanned)
                                                    checkpoint)))))
        (writeFileSync path (first (string/split-lines changed)) "utf8")
        (let [shrunk (scanner/scan-stream
                      {:session-id "claude-session-1" :transcript-path path})]
          (is (= :checkpoint-out-of-range
                 (:reason (sync/plan-sync shrunk (:entries shrunk) checkpoint))))))
      (finally
        (rmSync directory #js {:recursive true :force true})))))

(deftest rejects-complete-malformed-records-and-identity-conflicts
  (let [directory (mkdtempSync (.join node-path (tmpdir) "adam-claude-invalid-"))
        path (.join node-path directory "session.jsonl")]
    (try
      (writeFileSync path "{\"type\":\"user\"\n" "utf8")
      (is (thrown-with-msg? js/Error #"invalid JSON"
                            (scanner/scan-stream
                             {:session-id "claude-session-1"
                              :transcript-path path})))
      (writeFileSync path "[]" "utf8")
      (is (thrown-with-msg? js/Error #"line must contain a JSON object"
                            (scanner/scan-stream
                             {:session-id "claude-session-1"
                              :transcript-path path})))
      (writeFileSync
       path
       (str "{\"type\":\"user\",\"uuid\":\"same\",\"parentUuid\":null,\"sessionId\":\"claude-session-1\"}\n"
            "{\"type\":\"assistant\",\"uuid\":\"same\",\"parentUuid\":null,\"sessionId\":\"claude-session-1\"}\n")
       "utf8")
      (is (thrown-with-msg? js/Error #"duplicate record identity same"
                            (scanner/scan-stream
                             {:session-id "claude-session-1"
                              :transcript-path path})))
      (finally
        (rmSync directory #js {:recursive true :force true})))))

(deftest detects-concurrent-transcript-changes
  (let [directory (mkdtempSync (.join node-path (tmpdir) "adam-claude-concurrent-"))
        path (.join node-path directory "session.jsonl")
        changed? (atom false)]
    (try
      (copyFileSync main-fixture path)
      (is (thrown-with-msg?
           js/Error #"file changed while it was being scanned"
           (scanner/scan-stream
            {:session-id "claude-session-1"
             :transcript-path path
             :on-entry (fn [_]
                         (when (compare-and-set! changed? false true)
                           (appendFileSync path
                                           "{\"type\":\"future\",\"sessionId\":\"claude-session-1\"}\n"
                                           "utf8")))})))
      (finally
        (rmSync directory #js {:recursive true :force true})))))
