(ns adam.memory.inbox-test
  (:require [cljs.test :refer-macros [deftest is]]
            [adam.memory.inbox :as inbox]
            [adam.memory.protocol :as protocol]
            ["node:fs" :refer [mkdtempSync rmSync statSync symlinkSync mkdirSync writeFileSync]]
            ["node:os" :refer [tmpdir]]
            ["node:path" :refer [join]]))

(def locator {:source-kind "claude-code" :source-session-id "session-123"
              :producer-id "org.example.memory"})

(deftest durable-locator-only-notification
  (let [root (mkdtempSync (join (tmpdir) "adam-memory-inbox-"))
        options {:state-home root :data-home root}]
    (try
      (let [notification (inbox/enqueue! options locator)
            pending (inbox/pending options)]
        (is (= 1 (count pending)))
        (is (= locator (select-keys (first pending) (keys locator))))
        (is (= (str "claude-code/" (protocol/sha256 "session-123")
                    "/org.example.memory.jsonl") (:sidecar-locator notification)))
        (is (= 384 (bit-and 511 (.-mode (statSync (:path notification))))))
        (is (= (join root "adam" "memories" "v1" (:sidecar-locator notification))
               (inbox/sidecar-path options notification)))
        (inbox/acknowledge! options (first pending))
        (is (empty? (inbox/pending options))))
      (finally (rmSync root #js {:recursive true :force true})))))

(deftest invalid-locators-and-unbounded-content-fail-closed
  (let [root (mkdtempSync (join (tmpdir) "adam-memory-inbox-"))
        options {:state-home root :data-home root}]
    (try
      (doseq [invalid [(assoc locator :producer-id "../escape")
                       (assoc locator :source-session-id (apply str (repeat 513 "a")))
                       (assoc locator :content "not a locator")
                       (assoc locator :sidecar-locator "../../escape")]]
        (is (thrown? js/Error (inbox/enqueue! options invalid))))
      (is (thrown? js/Error (inbox/enqueue! {:state-home "relative"} locator)))
      (is (empty? (inbox/pending options)))
      (finally (rmSync root #js {:recursive true :force true})))))

(deftest symlink-spool-is-rejected
  (let [root (mkdtempSync (join (tmpdir) "adam-memory-inbox-"))
        elsewhere (join root "elsewhere")]
    (try
      (mkdirSync elsewhere)
      (symlinkSync elsewhere (join root "adam"))
      (is (thrown? js/Error (inbox/enqueue! {:state-home root} locator)))
      (finally (rmSync root #js {:recursive true :force true})))))

(deftest malformed-notification-does-not-hide-healthy-work
  (let [root (mkdtempSync (join (tmpdir) "adam-memory-inbox-"))
        options {:state-home root}]
    (try
      (inbox/enqueue! options locator)
      (writeFileSync (join root "adam" "memory-inbox" "bad.json") "{bad\n"
                     #js {:mode 384})
      (is (= 1 (count (inbox/pending options))))
      (is (= 1 (count (inbox/rejections options))))
      (finally (rmSync root #js {:recursive true :force true})))))
