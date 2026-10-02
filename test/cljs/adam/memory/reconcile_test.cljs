(ns adam.memory.reconcile-test
  (:require [cljs.test :refer-macros [deftest is async]]
            [adam.memory.inbox :as inbox]
            [adam.memory.reconcile :as reconcile]
            ["node:fs" :refer [mkdtempSync rmSync mkdirSync readFileSync writeFileSync]]
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
          synchronized (atom [])
          logs (atom [])]
      (inbox/enqueue! options (locator "session-1" "producer-a"))
      (inbox/enqueue! options (locator "session-2" "producer-a"))
      (mkdirSync (join root "adam" "memory-inbox" "unsafe.json"))
      (-> (reconcile/drain-once!
           {:inbox-options options
            :log! #(swap! logs conj %)
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
                   (is (= ["session-1"] (mapv :source-session-id (inbox/pending options))))
                   (is (= "source not mirrored" (:message (first (filter #(= :source-retry (:reason %)) @logs)))))))
          (.catch (fn [error] (is false (str error))))
          (.finally (fn [] (rmSync root #js {:recursive true :force true}) (done)))))))

(deftest confirmed-missing-source-expires-at-the-default-age-boundary
  (async done
    (let [root (mkdtempSync (join (tmpdir) "adam-memory-expiry-"))
          options {:state-home root :data-home root}
          notification (inbox/enqueue! options (locator "session-1" "producer-a"))
          file-time (:file-mtime-ms (first (inbox/pending options)))
          clock (atom (+ file-time 599999))
          present? (atom false)
          logs (atom [])
          writes (atom [])
          drain! #(reconcile/drain-once!
                    {:inbox-options options :now-epoch-ms (fn [] @clock)
                     :log! (fn [entry] (swap! logs conj entry))
                     :ensure-source! (fn [_]
                                       (if @present? (js/Promise.resolve nil)
                                         (throw (ex-info "source absent" {:reason :missing-source-session}))))
                     :sync-sidecar! (fn [_] (swap! writes conj :sync)
                                      (js/Promise.resolve {:status :unchanged}))
                     :project-session! (fn [_] (swap! writes conj :project) (js/Promise.resolve nil))})]
      (-> (drain!)
          (.then (fn [result]
                   (is (= [1 0 0] ((juxt :pending :acknowledged :source-expired) result)))
                   (is (empty? @writes))
                   (reset! clock (+ file-time 600000))
                   ;; A new drain rereads the durable file mtime; no in-memory retry
                   ;; count is needed, including after a process restart.
                   (drain!)))
          (.then (fn [result]
                   (is (= [0 1 1 0] ((juxt :pending :acknowledged :source-expired :failed) result)))
                   (is (= :source-never-mirrored (:reason (last @logs))))
                   (is (empty? @writes) "expiry must not read the sidecar or mutate retained graph state")
                   (reset! present? true)
                   (inbox/enqueue! options (locator "session-1" "producer-a"))
                   (drain!)))
          (.then (fn [result]
                   (is (= [0 1 0] ((juxt :pending :acknowledged :source-expired) result)))
                   (is (= [:sync :project] @writes) "fresh notification works normally after source import")))
          (.catch (fn [error] (is false (.-stack error))))
          (.finally (fn [] (rmSync root #js {:recursive true :force true}) (done)))))))

(deftest fresh-notification-from-slow-producer-clock-waits-for-source-hook
  (async done
    (let [root (mkdtempSync (join (tmpdir) "adam-memory-slow-clock-"))
          options {:state-home root :data-home root}
          notification (inbox/enqueue! options (locator "session-1" "producer-a"))
          wire (js/JSON.parse (readFileSync (:path notification) "utf8"))
          present? (atom false)
          writes (atom [])]
      ;; Simulate a producer writing a fresh file with its clock 15 minutes slow.
      (aset wire "queuedAt" (.toISOString (js/Date. (- (.now js/Date) 900000))))
      (writeFileSync (:path notification) (js/JSON.stringify wire))
      (let [clock (+ (:file-mtime-ms (first (inbox/pending options))) 1)
            drain! #(reconcile/drain-once!
                      {:inbox-options options :now-epoch-ms (fn [] clock)
                       :ensure-source! (fn [_]
                                         (if @present? (js/Promise.resolve nil)
                                           (js/Promise.reject
                                             (ex-info "absent" {:reason :missing-source-session}))))
                       :sync-sidecar! (fn [_] (swap! writes conj :sync) (js/Promise.resolve {:status :unchanged}))
                       :project-session! (fn [_] (swap! writes conj :project) (js/Promise.resolve nil))})]
        (-> (drain!)
            (.then (fn [result]
                     (is (= [1 0 0] ((juxt :pending :acknowledged :source-expired) result)))
                     (is (empty? @writes))
                     (reset! present? true)
                     (drain!)))
            (.then (fn [result]
                     (is (= [0 1 0] ((juxt :pending :acknowledged :source-expired) result)))
                     (is (= [:sync :project] @writes))))
            (.catch (fn [error] (is false (.-stack error))))
            (.finally (fn [] (rmSync root #js {:recursive true :force true}) (done))))))))

(deftest old-notifications-do-not-expire-on-backend-or-projection-failure
  (async done
    (let [root (mkdtempSync (join (tmpdir) "adam-memory-expiry-error-"))
          options {:state-home root :data-home root}
          notification (inbox/enqueue! options (locator "session-1" "producer-a"))
          clock (+ (:file-mtime-ms (first (inbox/pending options))) 600000)
          source-mode (atom :offline)
          settings {:inbox-options options :now-epoch-ms (fn [] clock)
                    :ensure-source! (fn [_]
                                      (if (= :offline @source-mode)
                                        (js/Promise.reject (js/Error. "Neo4j unavailable"))
                                        (js/Promise.resolve nil)))
                    :sync-sidecar! (fn [_] (js/Promise.resolve {:status :unchanged}))
                    :project-session! (fn [_]
                                        (js/Promise.reject
                                          (ex-info "projection failed" {:reason :missing-source-session})))}]
      (-> (reconcile/drain-once! settings)
          (.then (fn [result]
                   (is (= [1 0 1] ((juxt :pending :source-expired :failed) result)))
                   (reset! source-mode :present)
                   (reconcile/drain-once! settings)))
          (.then (fn [result]
                   (is (= [1 0 1] ((juxt :pending :source-expired :failed) result)))
                   (is (= 1 (count (inbox/pending options))))))
          (.catch (fn [error] (is false (.-stack error))))
          (.finally (fn [] (rmSync root #js {:recursive true :force true}) (done)))))))

(deftest coalesced-fresh-notification-does-not-reset-older-notification-age
  (async done
    (let [root (mkdtempSync (join (tmpdir) "adam-memory-expiry-coalesce-"))
          options {:state-home root :data-home root}
          older (inbox/enqueue! options (locator "session-1" "producer-a"))
          fresh (inbox/enqueue! options (locator "session-1" "producer-a"))
          ;; Inject per-notification times so this does not depend on wall-clock
          ;; scheduling of two adjacent enqueue operations.
          pending (atom [(assoc older :file-mtime-ms 0)
                         (assoc fresh :file-mtime-ms 599000)])
          clock 600000]
      (-> (js/Promise.resolve nil)
          (.then (fn [_]
                   (with-redefs [inbox/pending (fn [_] @pending)]
                     ;; pending is read synchronously; acknowledgements use real files.
                     (reconcile/drain-once!
                       {:inbox-options options :now-epoch-ms (fn [] clock)
                        :ensure-source! (fn [_] (js/Promise.reject
                                                 (ex-info "absent" {:reason :missing-source-session})))
                        :sync-sidecar! (fn [_] (is false "absent source must not read sidecars"))
                        :project-session! (fn [_] (is false "absent source must not project"))}))))
          (.then (fn [result]
                   (is (= 1 (:source-expired result)))
                   (is (= 1 (:failed result)))
                   (is (= [(:id fresh)] (mapv :id (inbox/pending options))))))
          (.catch (fn [error] (is false (.-stack error))))
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
          options {:state-home root :data-home root}
          logs (atom [])]
      (inbox/enqueue! options (locator "session-1" "producer-a"))
      (-> (reconcile/drain-once!
           {:inbox-options options
            :ensure-source! (fn [_] (js/Promise.resolve nil))
            :sync-sidecar! (fn [_] (js/Promise.resolve {:status :mirrored}))
            :log! #(swap! logs conj %)
            :project-session! (fn [_] (throw (js/Error. "projection outage")))})
          (.then (fn [result]
                   (is (= 0 (:acknowledged result)))
                   (is (= 1 (:pending result)))
                   (is (= 1 (count (inbox/pending options))))
                   (is (= :projection-retry (:reason (first @logs))))
                   (is (= "projection outage" (:message (first @logs))))))
          (.catch (fn [error] (is false (str error))))
          (.finally (fn [] (rmSync root #js {:recursive true :force true}) (done)))))))
