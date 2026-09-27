(ns adam.sources.claude-code.sync-test
  (:require [adam.sources.claude-code.scanner :as scanner]
            [adam.sources.claude-code.store :as store]
            [adam.sources.claude-code.sync :as sync]
            [cljs.test :refer [async deftest is]]
            ["node:path" :as node-path]))

(def main-fixture (.resolve node-path "test/fixtures/claude/main.jsonl"))
(def subagent-fixture (.resolve node-path "test/fixtures/claude/subagent.jsonl"))

(defrecord FakeClaudeStore [writes completions sessions]
  store/ClaudeTranscriptStore
  (ensure-claude-schema! [_] (js/Promise.resolve nil))
  (get-stream-checkpoint! [_ _] (js/Promise.resolve nil))
  (write-stream-batch! [_ request]
    (swap! writes conj request)
    (js/Promise.resolve nil))
  (complete-stream! [_ request]
    (swap! completions conj request)
    (js/Promise.resolve nil))
  (mark-stream-conflict! [_ _] (js/Promise.resolve nil))
  (complete-claude-session! [_ request]
    (swap! sessions conj request)
    (js/Promise.resolve nil)))

(deftest synchronizes-each-transcript-stream-before-completing-the-owning-session
  (async done
    (let [scan (scanner/scan-session
                {:session-id "claude-session-1"
                 :transcript-path main-fixture
                 :subagents [{:agent-id "agent-1"
                              :transcript-path subagent-fixture}]})
          writes (atom [])
          completions (atom [])
          sessions (atom [])
          replica (->FakeClaudeStore writes completions sessions)]
      (-> (sync/sync-session-scan!
           {:store replica
            :user-uuid "00000000-0000-4000-8000-000000000001"
            :scan scan
            :batch-bytes 1024})
          (.then
           (fn [result]
             (is (= :mirrored (:status result)))
             (is (= 16 (:entries-written result)))
             (is (= #{"main" "agent:agent-1"}
                    (set (map #(get-in % [:stream :stream-id]) @writes))))
             (is (= 2 (count @completions)))
             (is (= 1 (count @sessions)))
             (is (= "r-write" (get-in @sessions [0 :session :current-leaf-id])))
             (done)))
          (.catch
           (fn [error]
             (is false (.-stack error))
             (done)))))))
