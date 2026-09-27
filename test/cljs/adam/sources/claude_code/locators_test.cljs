(ns adam.sources.claude-code.locators-test
  (:require [adam.sources.claude-code.locators :as locators]
            [cljs.test :refer-macros [deftest is]]
            ["node:fs" :refer [mkdtempSync rmSync]]
            ["node:os" :refer [tmpdir]]
            ["node:path" :refer [join]]))

(deftest session-locators-retain-parent-and-known-subagent-streams
  (let [root (mkdtempSync (join (tmpdir) "adam-locators-test-"))
        options {:config-home root}]
    (try
      (locators/record!
       options
       {:event "SessionStart"
        :session-id "session-1"
        :transcript-path "/tmp/session.jsonl"
        :cwd "/tmp/project"})
      (locators/record!
       options
       {:event "SubagentStop"
        :session-id "session-1"
        :transcript-path "/tmp/subagents/agent-a1.jsonl"
        :parent-transcript-path "/tmp/session.jsonl"
        :agent-id "a1"
        :cwd "/tmp/project"})
      (is (= {:version 1
              :session-id "session-1"
              :transcript-path "/tmp/session.jsonl"
              :cwd "/tmp/project"
              :subagents [{:agent-id "a1"
                           :transcript-path "/tmp/subagents/agent-a1.jsonl"}]}
             (locators/load options "session-1")))
      (finally
        (rmSync root #js {:recursive true :force true})))))
