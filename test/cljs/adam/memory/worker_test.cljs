(ns adam.memory.worker-test
  (:require [adam.memory.service :as service]
            [adam.sources.claude-code.worker :as worker]
            [cljs.test :refer-macros [deftest is async]]
            ["node:fs" :refer [mkdtempSync rmSync]]
            ["node:os" :refer [tmpdir]]
            ["node:path" :refer [join]]))

(deftest transient-retry-releases-lease-so-source-can-progress
  (async done
    (let [root (mkdtempSync (join (tmpdir) "adam-memory-worker-"))
          options {:config-home root}
          calls (atom 0)
          delays (atom [])]
      (-> (worker/run-worker!
            {:inbox-options options :max-attempts 2 :initial-backoff-ms 10
             :log! (fn [_] nil)
             :drain! (fn []
                       (if (= 1 (swap! calls inc))
                         (js/Promise.reject (js/Error. "missing source"))
                         (js/Promise.resolve {:processed 1 :pending 0})))
             :sleep! (fn [delay]
                       (swap! delays conj delay)
                       (worker/with-lease! options #(js/Promise.resolve :source-mirrored)))})
          (.then (fn [result]
                   (is (= :idle (:status result)))
                   (is (= 2 @calls))
                   (is (= [10] @delays))))
          (.catch (fn [error] (is false (str error))))
          (.finally (fn [] (rmSync root #js {:recursive true :force true}) (done)))))))

(deftest release-recheck-sees-memory-notifications-after-drain
  (async done
    (let [root (mkdtempSync (join (tmpdir) "adam-memory-worker-"))
          calls (atom 0)
          checked (atom 0)]
      (-> (worker/run-worker!
            {:inbox-options {:config-home root}
             :drain! (fn [] (swap! calls inc) (js/Promise.resolve {:processed 0 :pending 0}))
             :pending? #(= 1 (swap! checked inc))})
          (.then (fn [result] (is (= :idle (:status result))) (is (= 2 @calls))))
          (.catch (fn [error] (is false (str error))))
          (.finally (fn [] (rmSync root #js {:recursive true :force true}) (done)))))))

(deftest busy-worker-does-not-process-and-bounded-retry-stops
  (async done
    (let [root (mkdtempSync (join (tmpdir) "adam-memory-worker-"))
          options {:config-home root}
          calls (atom 0)]
      (-> (worker/with-lease! options
            #(worker/run-worker! {:inbox-options options
                                  :drain! (fn [] (is false "busy worker must not drain"))}))
          (.then (fn [result]
                   (is (= {:status :busy} result))
                   (worker/run-worker!
                     {:inbox-options options :max-attempts 2
                      :log! (fn [_] nil) :sleep! (fn [_] (js/Promise.resolve nil))
                      :drain! (fn [] (swap! calls inc) (js/Promise.reject (js/Error. "offline")))})))
          (.then (fn [_] (is false "bounded retry must reject"))
                 (fn [_] (is (= 2 @calls))))
          (.finally (fn [] (rmSync root #js {:recursive true :force true}) (done)))))))

(deftest one-pass-synchronous-failure-releases-the-lease
  (async done
    (let [root (mkdtempSync (join (tmpdir) "adam-memory-worker-"))
          options {:config-home root}]
      (-> (worker/run-once! {:inbox-options options :drain! #(throw (js/Error. "invalid queue"))})
          (.then (fn [_] (is false "invalid queue must reject"))
                 (fn [_] (worker/with-lease! options #(js/Promise.resolve :released))))
          (.then (fn [result] (is (= :released result))))
          (.catch (fn [error] (is false (str error))))
          (.finally (fn [] (rmSync root #js {:recursive true :force true}) (done)))))))

(deftest transcript-failure-does-not-prevent-healthy-memory-drain
  (async done
    (let [calls (atom [])]
      (-> (service/drain-after-transcripts!
            (fn [] (swap! calls conj :transcript) (js/Promise.reject (js/Error. "offline transcript")))
            (fn [] (swap! calls conj :memory)
              (js/Promise.resolve {:acknowledged 1 :pending 0 :failed 0})))
          (.then (fn [_] (is false "retry must remain visible"))
                 (fn [_] (is (= [:transcript :memory] @calls))))
          (.finally done)))))
