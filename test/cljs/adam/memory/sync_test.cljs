(ns adam.memory.sync-test
  (:require [adam.memory.scanner :as scanner]
            [adam.memory.store :as store]
            [adam.memory.sync :as sync]
            [cljs.test :refer [async deftest is]]
            ["node:fs" :refer [chmodSync copyFileSync mkdtempSync rmSync writeFileSync]]
            ["node:os" :refer [tmpdir]]
            ["node:path" :as node-path]))

(def valid-fixture
  (.resolve node-path "docs/fixtures/memory-protocol-v1/valid-events.jsonl"))

(defrecord FakeMemoryStore [checkpoint writes completions conflicts]
  store/MemoryStreamStore
  (ensure-memory-schema! [_] (js/Promise.resolve nil))
  (get-memory-stream-checkpoint! [_ _] (js/Promise.resolve @checkpoint))
  (write-memory-record-batch! [_ request]
    (swap! writes conj request)
    (js/Promise.resolve nil))
  (complete-memory-stream! [_ request]
    (swap! completions conj request)
    (js/Promise.resolve nil))
  (mark-memory-stream-conflict! [_ conflict]
    (swap! conflicts conj conflict)
    (js/Promise.resolve nil))
  (read-memory-records! [_ _] (js/Promise.resolve [])))

(deftest mirrors-a-scanned-sidecar-in-byte-bounded-batches
  (async done
    (let [directory (mkdtempSync (.join node-path (tmpdir) "adam-memory-sync-"))
          path (.join node-path directory "events.jsonl")
          writes (atom [])
          completions (atom [])
          conflicts (atom [])
          replica (->FakeMemoryStore (atom nil) writes completions conflicts)]
      (copyFileSync valid-fixture path)
      (chmodSync path 384)
      (-> (sync/sync-sidecar-file!
           {:store replica
            :user-uuid "00000000-0000-4000-8000-000000000001"
            :path path
            :source-kind "claude-code"
            :source-session-id "session-123"
            :producer-id "org.example.claude-memory"
            :batch-bytes 900})
            (.then
             (fn [result]
               (is (= :mirrored (:status result)))
               (is (= 4 (:records-written result)))
               (is (< 1 (:batches-written result)))
               (is (= 4 (count (mapcat :records @writes))))
               (is (= 1 (count @completions)))
               (is (= "urn:adam:memory-stream:00000000-0000-4000-8000-000000000001:claude-code:session-123:org.example.claude-memory"
                      (get-in @completions [0 :stream :id])))
               (is (empty? @conflicts))))
            (.catch (fn [error] (is false (.-stack error))))
          (.finally
           (fn []
             (rmSync directory #js {:recursive true :force true})
             (done)))))))

(deftest mirrors-conflicting-event-records-before-isolating-the-stream
  (async done
    (let [directory (mkdtempSync (.join node-path (tmpdir) "adam-memory-event-conflict-"))
          path (.join node-path directory "events.jsonl")
          writes (atom [])
          conflicts (atom [])
          replica (->FakeMemoryStore (atom nil) writes (atom []) conflicts)]
      (copyFileSync
       (.resolve node-path "docs/fixtures/memory-protocol-v1/conflicting-event-id.jsonl")
       path)
      (chmodSync path 384)
      (-> (sync/sync-sidecar-file!
           {:store replica
            :user-uuid "00000000-0000-4000-8000-000000000001"
            :path path :source-kind "claude-code"
            :source-session-id "session-123"
            :producer-id "org.example.claude-memory"})
          (.then
           (fn [result]
             (is (= :conflict (:status result)))
             (is (= :immutable-event-conflict
                    (get-in result [:conflict :reason])))
             (is (= 2 (:records-written result)))
             (is (= 2 (count (mapcat :records @writes))))
             (is (= 1 (count @conflicts)))))
          (.catch (fn [error] (is false (.-stack error))))
          (.finally
           (fn []
             (rmSync directory #js {:recursive true :force true})
             (done)))))))

(deftest isolates-a-physical-record-conflict
  (async done
    (let [directory (mkdtempSync (.join node-path (tmpdir) "adam-memory-physical-sync-"))
          path (.join node-path directory "events.jsonl")
          conflicts (atom [])
          replica (->FakeMemoryStore (atom nil) (atom []) (atom []) conflicts)]
      (writeFileSync path (js/Buffer.from #js [255 10]) #js {:mode 384})
      (-> (sync/sync-sidecar-file!
           {:store replica
            :user-uuid "00000000-0000-4000-8000-000000000001"
            :path path :source-kind "claude-code"
            :source-session-id "session-123"
            :producer-id "org.example.claude-memory"})
          (.then
           (fn [result]
             (is (= :conflict (:status result)))
             (is (= :invalid-utf8 (get-in result [:conflict :reason])))
             (is (= 1 (count @conflicts)))
             (is (map? (:stream (first @conflicts))))))
          (.catch (fn [error] (is false (.-stack error))))
          (.finally
           (fn []
             (rmSync directory #js {:recursive true :force true})
             (done)))))))

(deftest refuses-a-mutated-committed-sidecar-prefix
  (async done
    (let [directory (mkdtempSync (.join node-path (tmpdir) "adam-memory-prefix-"))
          original-path (.join node-path directory "original.jsonl")
          mutated-path (.join node-path directory "mutated.jsonl")
          fixture-root (.resolve node-path "docs/fixtures/memory-protocol-v1")]
      (doseq [[fixture path] [["prefix-original.jsonl" original-path]
                              ["prefix-mutated.jsonl" mutated-path]]]
        (copyFileSync (.join node-path fixture-root fixture) path)
        (chmodSync path 384))
      (let [locator {:source-kind "claude-code"
                     :source-session-id "session-123"
                     :producer-id "org.example.claude-memory"}
            original (scanner/scan-sidecar (assoc locator :path original-path))
            mutated (scanner/scan-sidecar (assoc locator :path mutated-path))
            last-record (last (:records original))
            checkpoint (atom {:complete-through-ordinal (:ordinal last-record)
                              :complete-through-byte-offset (:next-byte-offset last-record)
                              :committed-prefix-hash (:prefix-hash last-record)})
            writes (atom [])
            conflicts (atom [])
            replica (->FakeMemoryStore checkpoint writes (atom []) conflicts)]
        (-> (sync/sync-sidecar-scan!
             {:store replica
              :user-uuid "00000000-0000-4000-8000-000000000001"
              :scan mutated})
            (.then
             (fn [result]
               (is (= :conflict (:status result)))
               (is (= :prefix-mismatch (get-in result [:conflict :reason])))
               (is (empty? @writes))
               (is (= 1 (count @conflicts)))))
            (.catch (fn [error] (is false (.-stack error))))
            (.finally
             (fn []
               (rmSync directory #js {:recursive true :force true})
               (done))))))))
