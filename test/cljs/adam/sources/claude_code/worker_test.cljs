(ns adam.sources.claude-code.worker-test
  (:require [adam.sources.claude-code.inbox :as inbox]
            [adam.sources.claude-code.worker :as worker]
            [cljs.test :refer-macros [async deftest is]]
            ["node:fs" :refer [mkdtempSync rmSync utimesSync writeFileSync]]
            ["node:os" :refer [tmpdir]]
            ["node:path" :refer [join]]))

(defn- input [session-id]
  {:hook-event-name "Stop"
   :session-id session-id
   :transcript-path (str "/tmp/" session-id ".jsonl")
   :cwd "/tmp/project"})

(deftest worker-recovers-an-old-incomplete-lock-file
  (async done
    (let [root (mkdtempSync (join (tmpdir) "adam-worker-test-"))
          options {:config-home root
                   :now-ms (constantly 1)
                   :uuid-fn (constantly "notification-1")}
          processed (atom 0)]
      (inbox/enqueue! options (input "session-1"))
      (writeFileSync (inbox/worker-lock-path options) "" "utf8")
      (utimesSync (inbox/worker-lock-path options) 0 0)
      (-> (worker/run-once!
           {:inbox-options options
            :process! (fn [_]
                        (swap! processed inc)
                        (js/Promise.resolve nil))})
          (.then
           (fn [result]
             (is (= :drained (:status result)))
             (is (= 1 @processed))))
          (.catch #(is false (str %)))
          (.finally
           (fn []
             (rmSync root #js {:recursive true :force true})
             (done)))))))

(deftest worker-retries-a-transient-failure-before-acknowledging
  (async done
    (let [root (mkdtempSync (join (tmpdir) "adam-worker-test-"))
          options {:config-home root
                   :now-ms (constantly 1)
                   :uuid-fn (constantly "notification-1")}
          attempts (atom 0)
          delays (atom [])]
      (inbox/enqueue! options (input "session-1"))
      (-> (worker/run-worker!
           {:inbox-options options
            :process! (fn [_]
                        (if (= 1 (swap! attempts inc))
                          (js/Promise.reject (js/Error. "Neo4j unavailable"))
                          (js/Promise.resolve nil)))
            :sleep! (fn [milliseconds]
                      (swap! delays conj milliseconds)
                      (js/Promise.resolve nil))
            :initial-backoff-ms 25
            :max-attempts 2})
          (.then
           (fn [result]
             (is (= :idle (:status result)))
             (is (= 2 @attempts))
             (is (= [25] @delays))
             (is (empty? (inbox/pending options)))))
          (.catch #(is false (str %)))
          (.finally
           (fn []
             (rmSync root #js {:recursive true :force true})
             (done)))))))

(deftest worker-recovers-a-lock-owned-by-a-dead-process
  (async done
    (let [root (mkdtempSync (join (tmpdir) "adam-worker-test-"))
          options {:config-home root
                   :now-ms (constantly 1)
                   :uuid-fn (constantly "notification-1")}
          processed (atom 0)]
      (inbox/enqueue! options (input "session-1"))
      (writeFileSync (inbox/worker-lock-path options) "999999999\n" "utf8")
      (-> (worker/run-once!
           {:inbox-options options
            :process! (fn [_]
                        (swap! processed inc)
                        (js/Promise.resolve nil))})
          (.then
           (fn [result]
             (is (= :drained (:status result)))
             (is (= 1 @processed))))
          (.catch #(is false (str %)))
          (.finally
           (fn []
             (rmSync root #js {:recursive true :force true})
             (done)))))))

(deftest only-one-worker-holds-the-inbox-lease
  (async done
    (let [root (mkdtempSync (join (tmpdir) "adam-worker-test-"))
          options {:config-home root
                   :now-ms (constantly 1)
                   :uuid-fn (constantly "notification-1")}
          release-process (atom nil)
          calls (atom 0)]
      (inbox/enqueue! options (input "session-1"))
      (let [first-run (worker/run-once!
                       {:inbox-options options
                        :process! (fn [_]
                                    (swap! calls inc)
                                    (js/Promise. (fn [resolve _]
                                                   (reset! release-process resolve))))})]
        (-> (worker/run-once!
             {:inbox-options options
              :process! (fn [_]
                          (swap! calls inc)
                          (js/Promise.resolve nil))})
            (.then
             (fn [second-result]
               (is (= {:status :busy} second-result))
               (@release-process nil)
               first-run))
            (.then
             (fn [first-result]
               (is (= :drained (:status first-result)))
               (is (= 1 @calls))))
            (.catch #(is false (str %)))
            (.finally
             (fn []
               (rmSync root #js {:recursive true :force true})
               (done))))))))

(deftest worker-coalesces-repeated-locators-before-reconciliation
  (async done
    (let [root (mkdtempSync (join (tmpdir) "adam-worker-test-"))
          next-id (atom 0)
          options {:config-home root
                   :now-ms #(swap! next-id inc)
                   :uuid-fn #(str "notification-" @next-id)}
          processed (atom [])]
      (inbox/enqueue! options (assoc (input "session-1") :hook-event-name "SessionStart"))
      (inbox/enqueue! options (input "session-1"))
      (-> (worker/drain-once!
           {:inbox-options options
            :process! (fn [notification]
                        (swap! processed conj (:event notification))
                        (js/Promise.resolve nil))})
          (.then
           (fn [result]
             (is (= ["Stop"] @processed))
             (is (= {:processed 2 :pending 0} result))))
          (.catch #(is false (str %)))
          (.finally
           (fn []
             (rmSync root #js {:recursive true :force true})
             (done)))))))

(deftest worker-retains-failed-and-later-notifications-for-retry
  (async done
    (let [root (mkdtempSync (join (tmpdir) "adam-worker-test-"))
          next-id (atom 0)
          options {:config-home root
                   :now-ms #(swap! next-id inc)
                   :uuid-fn #(str "notification-" @next-id)}]
      (inbox/enqueue! options (input "session-1"))
      (inbox/enqueue! options (input "session-2"))
      (-> (worker/drain-once!
           {:inbox-options options
            :process! (fn [_] (js/Promise.reject (js/Error. "Neo4j unavailable")))})
          (.then (fn [_] (is false "worker should reject a failed reconciliation")))
          (.catch
           (fn [error]
             (is (= "Neo4j unavailable" (.-message error)))
             (is (= ["session-1" "session-2"]
                    (mapv :session-id (inbox/pending options))))))
          (.finally
           (fn []
             (rmSync root #js {:recursive true :force true})
             (done)))))))

(deftest worker-processes-notifications-serially-and-acknowledges-success
  (async done
    (let [root (mkdtempSync (join (tmpdir) "adam-worker-test-"))
          next-id (atom 0)
          options {:config-home root
                   :now-ms #(swap! next-id inc)
                   :uuid-fn #(str "notification-" @next-id)}
          active (atom 0)
          maximum-active (atom 0)
          processed (atom [])]
      (inbox/enqueue! options (input "session-1"))
      (inbox/enqueue! options (input "session-2"))
      (-> (worker/drain-once!
           {:inbox-options options
            :process!
            (fn [notification]
              (swap! active inc)
              (swap! maximum-active max @active)
              (js/Promise.
               (fn [resolve _]
                 (js/setTimeout
                  (fn []
                    (swap! processed conj (:session-id notification))
                    (swap! active dec)
                    (resolve nil))
                  5))))})
          (.then
           (fn [result]
             (is (= ["session-1" "session-2"] @processed))
             (is (= 1 @maximum-active))
             (is (= {:processed 2 :pending 0} result))
             (is (empty? (inbox/pending options)))))
          (.catch #(is false (str %)))
          (.finally
           (fn []
             (rmSync root #js {:recursive true :force true})
             (done)))))))
