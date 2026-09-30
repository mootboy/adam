(ns adam.memory.scanner-test
  (:require [adam.memory.protocol :as protocol]
            [adam.memory.scanner :as scanner]
            [cljs.test :refer [deftest is]]
            [clojure.string :as string]
            ["node:fs" :refer [chmodSync copyFileSync mkdtempSync readFileSync rmSync symlinkSync writeFileSync]]
            ["node:os" :refer [tmpdir]]
            ["node:path" :as node-path]))

(def fixture-root (.resolve node-path "docs/fixtures/memory-protocol-v1"))
(def valid-fixture (.join node-path fixture-root "valid-events.jsonl"))

(defn- with-private-copy [fixture f]
  (let [directory (mkdtempSync (.join node-path (tmpdir) "adam-memory-scan-"))
        path (.join node-path directory "events.jsonl")]
    (try
      (copyFileSync fixture path)
      (chmodSync path 384)
      (f path)
      (finally
        (rmSync directory #js {:recursive true :force true})))))

(deftest scans-complete-memory-events-losslessly
  (with-private-copy
    valid-fixture
    (fn [path]
      (let [scan (scanner/scan-sidecar
                  {:path path
                   :source-kind "claude-code"
                   :source-session-id "session-123"
                   :producer-id "org.example.claude-memory"})
            source-lines (string/split-lines (readFileSync path "utf8"))]
        (is (= 4 (:record-count scan)))
        (is (= source-lines (mapv :raw-json (:records scan))))
        (is (= [0 1 2 3] (mapv :ordinal (:records scan))))
        (is (every? #(re-matches #"[a-f0-9]{64}" (:payload-hash %))
                    (:records scan)))
        (is (= [:accepted :accepted :accepted :accepted]
               (mapv :semantic-status (:records scan))))
        (is (= 0 (:incomplete-tail-bytes scan)))
        (is (= (:source-bytes scan) (:log-bytes scan)))))))

(deftest preserves-malformed-records-and-defers-an-incomplete-tail
  (with-private-copy
    (.join node-path fixture-root "malformed-complete-record.jsonl")
    (fn [path]
      (let [scan (scanner/scan-sidecar
                  {:path path :source-kind "claude-code"
                   :source-session-id "session-123"
                   :producer-id "org.example.claude-memory"})]
        (is (= 1 (:record-count scan)))
        (is (= [:malformed-json] (get-in scan [:records 0 :diagnostics])))
        (is (= (first (string/split-lines (readFileSync path "utf8")))
               (get-in scan [:records 0 :raw-json]))))))
  (with-private-copy
    (.join node-path fixture-root "incomplete-tail.jsonl")
    (fn [path]
      (let [scan (scanner/scan-sidecar
                  {:path path :source-kind "claude-code"
                   :source-session-id "session-123"
                   :producer-id "org.example.claude-memory"})]
        (is (= 1 (:record-count scan)))
        (is (pos? (:incomplete-tail-bytes scan)))
        (is (< (:log-bytes scan) (:source-bytes scan)))
        (is (= :accepted (get-in scan [:records 0 :semantic-status])))))))

(deftest applies-canonical-event-replay-and-checkpoint-monotonicity
  (doseq [[file expected-statuses expected-conflict]
          [["identical-replay.jsonl" [:accepted :replay] nil]
           ["conflicting-event-id.jsonl" [:accepted :conflict]
            :immutable-event-conflict]
           ["checkpoint-regression.jsonl"
            [:accepted :skipped :skipped :skipped :accepted] nil]]]
    (with-private-copy
      (.join node-path fixture-root file)
      (fn [path]
        (let [scan (scanner/scan-sidecar
                    {:path path :source-kind "claude-code"
                     :source-session-id "session-123"
                     :producer-id "org.example.claude-memory"})]
          (is (= expected-statuses (mapv :semantic-status (:records scan))) file)
          (is (= expected-conflict (:semantic-conflict scan)) file)))))
  (with-private-copy
    (.join node-path fixture-root "canonical-jcs.jsonl")
    (fn [path]
      (let [scan (scanner/scan-sidecar
                  {:path path :source-kind "claude-code"
                   :source-session-id "session-123"
                   :producer-id "org.example.claude-memory"})]
        (is (= "7665bf74ad2cbfa0c8cdcd7cb62b6a814fe5ef39c651590d5f04f5ee2b825cc5"
               (get-in scan [:records 0 :event-hash])))))))

(deftest skips-invalid-supported-and-mismatched-envelopes-without-losing-framing
  (let [directory (mkdtempSync (.join node-path (tmpdir) "adam-memory-invalid-"))
        path (.join node-path directory "events.jsonl")
        valid-line (first (string/split-lines (readFileSync valid-fixture "utf8")))
        invalid-line (.replace valid-line "\"observations\":[" "\"missingObservations\":[")
        source-mismatch (first (string/split-lines
                                (readFileSync (.join node-path fixture-root
                                                    "source-mismatch.jsonl") "utf8")))
        producer-mismatch (first (string/split-lines
                                  (readFileSync (.join node-path fixture-root
                                                      "producer-mismatch.jsonl") "utf8")))]
    (try
      (writeFileSync path (str invalid-line "\n" source-mismatch "\n"
                               producer-mismatch "\n" valid-line "\n")
                     #js {:encoding "utf8" :mode 384})
      (let [scan (scanner/scan-sidecar
                  {:path path :source-kind "claude-code"
                   :source-session-id "session-123"
                   :producer-id "org.example.claude-memory"})]
        (is (= [[:invalid-envelope] [:source-mismatch] [:producer-mismatch] []]
               (mapv :diagnostics (:records scan))))
        (is (= [:skipped :skipped :skipped :accepted]
               (mapv :semantic-status (:records scan)))))
      (finally
        (rmSync directory #js {:recursive true :force true})))))

(deftest reports-oversized-and-invalid-utf8-physical-records
  (let [directory (mkdtempSync (.join node-path (tmpdir) "adam-memory-physical-"))
        path (.join node-path directory "events.jsonl")
        locator {:path path :source-kind "claude-code"
                 :source-session-id "session-123"
                 :producer-id "org.example.claude-memory"}]
    (try
      (writeFileSync path (str (.repeat "x" (inc (* 1024 1024))) "\n")
                     #js {:encoding "utf8" :mode 384})
      (let [scan (scanner/scan-sidecar locator)]
        (is (= :record-too-large (get-in scan [:physical-conflict :reason])))
        (is (empty? (:records scan))))
      (writeFileSync path (js/Buffer.from #js [255 10]) #js {:mode 384})
      (let [scan (scanner/scan-sidecar locator)]
        (is (= :invalid-utf8 (get-in scan [:physical-conflict :reason])))
        (is (empty? (:records scan))))
      (finally
        (rmSync directory #js {:recursive true :force true})))))

(deftest protocol-string-bounds-use-utf8-bytes
  (let [event (js/JSON.parse
               (first (string/split-lines (readFileSync valid-fixture "utf8"))))]
    (aset (aget (aget event "observations") 0) "content" (.repeat "é" 40000))
    (is (= [:invalid-envelope]
           (:diagnostics
            (protocol/inspect-event
             event {:source-kind "claude-code"
                    :source-session-id "session-123"
                    :producer-id "org.example.claude-memory"}))))))

(deftest classifies-scan-failures-by-reconciliation-disposition
  (is (= :notification-terminal (scanner/failure-class :unsafe-path)))
  (is (= :notification-terminal (scanner/failure-class :unsafe-permissions)))
  (is (= :notification-terminal (scanner/failure-class :missing-sidecar)))
  (is (= :physical-conflict (scanner/failure-class :record-too-large)))
  (is (= :physical-conflict (scanner/failure-class :invalid-utf8)))
  (is (= :transient (scanner/failure-class :concurrent-change))))

(deftest rejects-unsafe-sidecar-files
  (let [directory (mkdtempSync (.join node-path (tmpdir) "adam-memory-safety-"))
        path (.join node-path directory "events.jsonl")
        link (.join node-path directory "events-link.jsonl")
        locator {:source-kind "claude-code"
                 :source-session-id "session-123"
                 :producer-id "org.example.claude-memory"}]
    (try
      (writeFileSync path "{}\n" #js {:encoding "utf8" :mode 420})
      (is (thrown-with-msg? js/Error #"owner-only"
                            (scanner/scan-sidecar (assoc locator :path path))))
      (chmodSync path 384)
      (symlinkSync path link)
      (is (thrown-with-msg? js/Error #"symbolic links"
                            (scanner/scan-sidecar (assoc locator :path link))))
      (finally
        (rmSync directory #js {:recursive true :force true})))))
