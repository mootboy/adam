(ns adam.memory.inbox-test
  (:require [cljs.test :refer-macros [deftest is]]
            [adam.memory.inbox :as inbox]
            [adam.memory.protocol :as protocol]
            ["node:fs" :refer [mkdtempSync rmSync statSync symlinkSync mkdirSync writeFileSync readFileSync]]
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
        (is (js/Number.isFinite (:file-mtime-ms (first pending))))
        (is (= (:file-mtime-ms (first pending)) (:file-mtime-ms (first (inbox/pending options)))))
        (is (= (str "claude-code/" (protocol/sha256 "session-123")
                    "/org.example.memory.jsonl") (:sidecar-locator notification)))
        (is (= 384 (bit-and 511 (.-mode (statSync (:path notification))))))
        (is (= (join root "adam" "memories" "v1" (:sidecar-locator notification))
               (inbox/sidecar-path options notification)))
        (inbox/acknowledge! options (first pending))
        (is (empty? (inbox/pending options))))
      (finally (rmSync root #js {:recursive true :force true})))))

(deftest archive-coalesces-locators-and-survives-both-crash-windows
  (let [root (mkdtempSync (join (tmpdir) "adam-memory-archive-"))
        options {:state-home root :data-home root}]
    (try
      (inbox/enqueue! options locator)
      (let [notification (first (inbox/pending options))]
        ;; Crash after durable parking, before active acknowledgement.
        (inbox/park! options notification)
        (is (= 1 (count (inbox/pending options))))
        (is (= 1 (count (inbox/parked options))))
        (inbox/park! options notification)
        (is (= 1 (count (inbox/parked options))))
        (is (= 384 (bit-and 511 (.-mode (statSync (:path (first (inbox/parked options))))))))
        (inbox/acknowledge! options notification))
      (is (empty? (inbox/rejections options)) "archive is not part of the active inbox")
      (let [snapshot (first (inbox/parked options))]
        ;; Crash after durable recovery enqueue, before archive removal.
        (inbox/enqueue! options locator)
        (inbox/requeue! options snapshot)
        (is (empty? (inbox/parked options)))
        (is (= 2 (count (inbox/pending options))))
        (is (every? #(= locator (select-keys % (keys locator))) (inbox/pending options))))
      (finally (rmSync root #js {:recursive true :force true})))))

(deftest unsafe-archive-retains-active-work-without-following-links
  (let [root (mkdtempSync (join (tmpdir) "adam-memory-archive-unsafe-"))
        options {:state-home root :data-home root}
        elsewhere (join root "elsewhere")]
    (try
      (inbox/enqueue! options locator)
      (mkdirSync elsewhere #js {:mode 448})
      (symlinkSync elsewhere (join root "adam" "memory-inbox" "expired"))
      (is (thrown? js/Error (inbox/park! options (first (inbox/pending options)))))
      (is (= 1 (count (inbox/pending options))))
      (is (thrown? js/Error (inbox/parked options)))
      (finally (rmSync root #js {:recursive true :force true})))))

(deftest unsafe-archive-record-is-neither-followed-nor-overwritten
  (let [root (mkdtempSync (join (tmpdir) "adam-memory-archive-record-"))
        options {:state-home root :data-home root}
        target (join root "private-target")]
    (try
      (inbox/enqueue! options locator)
      (inbox/park! options (first (inbox/pending options)))
      (let [path (:path (first (inbox/parked options)))]
        (rmSync path)
        (writeFileSync target "untouched" #js {:mode 384})
        (symlinkSync target path)
        (is (empty? (inbox/parked options)))
        (is (= :unsafe-path (:reason (first (inbox/parked-rejections options)))))
        (is (thrown? js/Error (inbox/park! options (first (inbox/pending options)))))
        (is (thrown? js/Error (inbox/requeue! options {:path path})))
        (is (= 1 (count (inbox/pending options))))
        (is (= "untouched" (readFileSync target "utf8"))))
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
