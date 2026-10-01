(ns adam.memory.reconcile-test
  (:require [cljs.test :refer-macros [deftest is async]]
            [adam.memory.inbox :as inbox]
            [adam.memory.reconcile :as reconcile]
            ["node:fs" :refer [mkdtempSync rmSync mkdirSync]]
            ["node:os" :refer [tmpdir]]
            ["node:path" :refer [join]]))

(defn locator [session producer]
  {:source-kind "claude-code" :source-session-id session :producer-id producer})

(deftest coalesces-by-producer-and-projects-once-before-ack
  (async done
    (let [root (mkdtempSync (join (tmpdir) "adam-memory-drain-"))
          options {:state-home root :data-home root}
          calls (atom [])]
      (doseq [producer ["producer-a" "producer-a" "producer-b"]]
        (inbox/enqueue! options (locator "session-1" producer)))
      (-> (reconcile/drain-once!
           {:inbox-options options
            :ensure-source! (fn [source] (swap! calls conj [:source (:source-session-id source)])
                              (js/Promise.resolve nil))
            :sync-sidecar! (fn [request] (swap! calls conj [:sync (:producer-id request)])
                             (js/Promise.resolve {:status :mirrored :records-written 2}))
            :project-session! (fn [_]
                                (is (= 3 (count (inbox/pending options))))
                                (swap! calls conj [:project])
                                (js/Promise.resolve nil))})
          (.then (fn [result]
                   (is (= [[:source "session-1"] [:sync "producer-a"]
                           [:sync "producer-b"] [:project]] @calls))
                   (is (= 3 (:acknowledged result)))
                   (is (= 4 (:records-written result)))
                   (is (empty? (inbox/pending options)))))
          (.catch (fn [error] (is false (str error))))
          (.finally (fn [] (rmSync root #js {:recursive true :force true}) (done)))))))

(deftest transient-producer-failure-does-not-block-healthy-session
  (async done
    (let [root (mkdtempSync (join (tmpdir) "adam-memory-drain-"))
          options {:state-home root :data-home root}]
      (inbox/enqueue! options (locator "session-1" "producer-a"))
      (inbox/enqueue! options (locator "session-1" "producer-b"))
      (-> (reconcile/drain-once!
           {:inbox-options options
            :ensure-source! (fn [_] (js/Promise.resolve nil))
            :sync-sidecar! (fn [request]
                             (if (= "producer-a" (:producer-id request))
                               (js/Promise.reject (js/Error. "offline"))
                               (js/Promise.resolve {:status :unchanged})))
            :project-session! (fn [_] (js/Promise.resolve nil))})
          (.then (fn [result]
                   (is (= 1 (:acknowledged result)))
                   (is (= 1 (:failed result)))
                   (is (= ["producer-a"] (mapv :producer-id (inbox/pending options))))))
          (.catch (fn [error] (is false (str error))))
          (.finally (fn [] (rmSync root #js {:recursive true :force true}) (done)))))))

(deftest missing-source-retries-without-blocking-other-sessions
  (async done
    (let [root (mkdtempSync (join (tmpdir) "adam-memory-drain-"))
          options {:state-home root :data-home root}
          synchronized (atom [])]
      (inbox/enqueue! options (locator "session-1" "producer-a"))
      (inbox/enqueue! options (locator "session-2" "producer-a"))
      (mkdirSync (join root "adam" "memory-inbox" "unsafe.json"))
      (-> (reconcile/drain-once!
           {:inbox-options options
            :ensure-source! (fn [source]
                              (if (= "session-1" (:source-session-id source))
                                (throw (js/Error. "source not mirrored"))
                                (js/Promise.resolve nil)))
            :sync-sidecar! (fn [request]
                             (swap! synchronized conj (:source-session-id request))
                             (js/Promise.resolve {:status :unchanged}))
            :project-session! (fn [_] (js/Promise.resolve nil))})
          (.then (fn [result]
                   (is (= ["session-2"] @synchronized))
                   (is (= 1 (:failed result)))
                   (is (= 1 (:acknowledged result)))
                   (is (= ["session-1"] (mapv :source-session-id (inbox/pending options))))))
          (.catch (fn [error] (is false (str error))))
          (.finally (fn [] (rmSync root #js {:recursive true :force true}) (done)))))))

(deftest terminal-source-rejection-acknowledges-without-projecting
  (async done
    (let [root (mkdtempSync (join (tmpdir) "adam-memory-drain-"))
          options {:state-home root :data-home root}]
      (inbox/enqueue! options (locator "session-1" "producer-a"))
      (-> (reconcile/drain-once!
           {:inbox-options options
            :ensure-source! (fn [_] (js/Promise.resolve nil))
            :sync-sidecar! (fn [_]
                             (js/Promise.resolve {:status :not-ingested
                                                  :reason :missing-sidecar
                                                  :notification-disposition :acknowledge}))
            :project-session! (fn [_] (is false "terminal source must not reproject"))})
          (.then (fn [result]
                   (is (= 1 (:acknowledged result)))
                   (is (zero? (:sessions-projected result)))
                   (is (empty? (inbox/pending options)))))
          (.catch (fn [error] (is false (str error))))
          (.finally (fn [] (rmSync root #js {:recursive true :force true}) (done)))))))

(deftest failed-projection-retains-notifications-for-restart
  (async done
    (let [root (mkdtempSync (join (tmpdir) "adam-memory-drain-"))
          options {:state-home root :data-home root}]
      (inbox/enqueue! options (locator "session-1" "producer-a"))
      (-> (reconcile/drain-once!
           {:inbox-options options
            :ensure-source! (fn [_] (js/Promise.resolve nil))
            :sync-sidecar! (fn [_] (js/Promise.resolve {:status :mirrored}))
            :project-session! (fn [_] (throw (js/Error. "projection outage")))})
          (.then (fn [result]
                   (is (= 0 (:acknowledged result)))
                   (is (= 1 (:pending result)))
                   (is (= 1 (count (inbox/pending options))))))
          (.catch (fn [error] (is false (str error))))
          (.finally (fn [] (rmSync root #js {:recursive true :force true}) (done)))))))
