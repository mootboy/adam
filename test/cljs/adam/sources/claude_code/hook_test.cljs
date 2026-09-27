(ns adam.sources.claude-code.hook-test
  (:require [adam.sources.claude-code.hook :as hook]
            [adam.sources.claude-code.inbox :as inbox]
            [cljs.test :refer-macros [deftest is]]
            ["node:fs" :refer [mkdtempSync rmSync]]
            ["node:os" :refer [tmpdir]]
            ["node:path" :refer [join]]))

(deftest subagent-hook-enqueues-the-explicit-child-stream-locator
  (let [root (mkdtempSync (join (tmpdir) "adam-hook-test-"))
        options {:inbox-options {:config-home root
                                 :now-ms (constantly 1)
                                 :uuid-fn (constantly "notification-1")}
                 :spawn-worker! (fn [])}]
    (try
      (hook/handle!
       options
       (js/JSON.stringify
        #js {:hook_event_name "SubagentStop"
             :session_id "claude-session-1"
             :transcript_path "/tmp/session.jsonl"
             :agent_transcript_path "/tmp/subagents/agent-a1.jsonl"
             :agent_id "a1"
             :cwd "/tmp/project"}))
      (is (= {:event "SubagentStop"
              :session-id "claude-session-1"
              :transcript-path "/tmp/subagents/agent-a1.jsonl"
              :parent-transcript-path "/tmp/session.jsonl"
              :agent-id "a1"}
             (select-keys (first (inbox/pending {:config-home root}))
                          [:event :session-id :transcript-path
                           :parent-transcript-path :agent-id])))
      (finally
        (rmSync root #js {:recursive true :force true})))))

(deftest hook-durably-enqueues-before-waking-worker
  (let [root (mkdtempSync (join (tmpdir) "adam-hook-test-"))
        options {:inbox-options {:config-home root
                                 :now-ms (constantly 1)
                                 :uuid-fn (constantly "notification-1")}
                 :spawn-worker! (fn []
                                  (is (= 1 (count (inbox/pending {:config-home root})))))}]
    (try
      (let [result (hook/handle!
                    options
                    (js/JSON.stringify
                     #js {:hook_event_name "Stop"
                          :session_id "claude-session-1"
                          :transcript_path "/tmp/session.jsonl"
                          :cwd "/tmp/project"}))]
        (is (= {:status :queued :notification-id "notification-1"} result)))
      (finally
        (rmSync root #js {:recursive true :force true})))))
