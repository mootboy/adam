(ns adam.sources.claude-code.inbox-test
  (:require [adam.sources.claude-code.inbox :as inbox]
            [cljs.test :refer-macros [deftest is testing]]
            ["node:fs" :refer [mkdtempSync readFileSync rmSync statSync]]
            ["node:os" :refer [tmpdir]]
            ["node:path" :refer [join]]))

(deftest invalid-hook-locators-are-not-enqueued
  (let [root (mkdtempSync (join (tmpdir) "adam-inbox-test-"))
        options {:config-home root}]
    (try
      (doseq [input [{:hook-event-name "PreToolUse"
                      :session-id "session"
                      :transcript-path "/tmp/session.jsonl"
                      :cwd "/tmp"}
                     {:hook-event-name "Stop"
                      :session-id "session"
                      :transcript-path "relative.jsonl"
                      :cwd "/tmp"}
                     {:hook-event-name "Stop"
                      :session-id ""
                      :transcript-path "/tmp/session.jsonl"
                      :cwd "/tmp"}
                     {:hook-event-name "SubagentStop"
                      :session-id "session"
                      :transcript-path "/tmp/subagent.jsonl"
                      :parent-transcript-path "/tmp/session.jsonl"
                      :cwd "/tmp"}]]
        (is (thrown? js/Error (inbox/enqueue! options input))))
      (is (empty? (inbox/pending options)))
      (finally
        (rmSync root #js {:recursive true :force true})))))

(deftest notifications-survive-until-successfully-acknowledged
  (let [root (mkdtempSync (join (tmpdir) "adam-inbox-test-"))
        options {:config-home root
                 :now-ms (constantly 1730000000000)
                 :uuid-fn (constantly "notification-1")}]
    (try
      (let [queued (inbox/enqueue!
                    options
                    {:hook-event-name "Stop"
                     :session-id "claude-session-1"
                     :transcript-path "/tmp/session.jsonl"
                     :cwd "/tmp/project"
                     :ignored-transcript-content "must-not-be-persisted"})
            pending (inbox/pending options)
            path (:path queued)]
        (is (= 1 (count pending)))
        (is (= {:version 1
                :id "notification-1"
                :event "Stop"
                :session-id "claude-session-1"
                :transcript-path "/tmp/session.jsonl"
                :cwd "/tmp/project"
                :queued-at "2024-10-27T03:33:20.000Z"}
               (dissoc (first pending) :path)))
        (is (not (.includes (readFileSync path "utf8") "must-not-be-persisted")))
        (is (= 384 (bit-and (.-mode (statSync path)) 511)))
        (inbox/acknowledge! options queued)
        (is (empty? (inbox/pending options))))
      (finally
        (rmSync root #js {:recursive true :force true})))))
