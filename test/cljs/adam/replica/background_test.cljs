(ns adam.replica.background-test
  (:require [adam.replica.background :as background]
            [adam.replica.processing :as processing]
            [adam.replica.store :as store]
            ["node:fs" :refer [mkdtempSync writeFileSync rmSync]]
            ["node:os" :refer [tmpdir]]
            ["node:path" :refer [join]]
            ["node:url" :refer [pathToFileURL]]
            [cljs.test :refer [async deftest is]]))

(deftest queue-coalesces-only-unread-requests-and-respects-command-barriers
  (async done
    (let [release (js/Promise.withResolvers)
          started (js/Promise.withResolvers)
          seen (atom []) statuses (atom [])
          queue (background/create-queue
                  {:on-status #(swap! statuses conj %)
                   :run! (fn [request]
                           (swap! seen conj (select-keys request [:path :leaf]))
                           (when (= 1 (count @seen))
                             ((.-resolve started) nil)
                             (.-promise release)))})]
      ((:enqueue! queue) {:path "A" :leaf "initial"} true)
      (is (empty? @seen) "enqueue does no work before returning to Pi")
      (-> (.-promise started)
          (.then (fn [_]
                   (dotimes [n 100] ((:enqueue! queue) {:path "A" :leaf (str n)} true))
                   ((:enqueue! queue) {:path "B" :leaf "B1"} true)
                   (is (= 2 (:pending (last @statuses))))
                   (let [manual ((:enqueue! queue) {:path "A" :leaf "explicit"} false)]
                     ((:enqueue! queue) {:path "A" :leaf "after-command"} true)
                     ((.-resolve release) nil)
                     manual)))
          (.then (fn [_] ((:flush! queue))))
          (.then (fn [_]
                   (is (= [{:path "A" :leaf "initial"} {:path "A" :leaf "99"}
                           {:path "B" :leaf "B1"} {:path "A" :leaf "explicit"}
                           {:path "A" :leaf "after-command"}] @seen))
                   ((:stop! queue))
                   (-> ((:enqueue! queue) {:path "C"} false)
                       (.then (fn [_] (is false "closed queue must reject")))
                       (.catch (fn [_] (is true true))))))
          (.catch #(is false (.-stack %)))
          (.finally done)))))

(deftest queue-continues-after-synchronous-and-asynchronous-failure
  (async done
    (let [seen (atom [])
          queue (background/create-queue
                  {:run! (fn [{:keys [path]}]
                           (swap! seen conj path)
                           (case path
                             "sync" (throw (js/Error. "sync failure"))
                             "async" (js/Promise.reject (js/Error. "async failure"))
                             :ok))})
          work (mapv #(.catch ((:enqueue! queue) {:path %} false) (fn [_] :failed))
                     ["sync" "async" "healthy"])]
      (-> (js/Promise.all (to-array work))
          (.then (fn [values]
                   (is (= [:failed :failed :ok] (vec values)))
                   (is (= ["sync" "async" "healthy"] @seen))))
          (.catch #(is false (.-stack %)))
          (.finally done)))))

(deftest real-processing-client-preserves-scanner-errors-and-can-restart-a-snapshot
  (async done
    (let [url (.-href (pathToFileURL (join (.cwd js/process) "processor.js")))
          client (background/create-client url)]
      (-> (js/Promise.resolve nil)
          (.then (fn [_] ((:call! client) {:op :start :path "/nonexistent/synthetic.jsonl"})))
          (.then (fn [_]
                   ((:call! client) {:op :records
                                     :records [{:bytes (js/Buffer.from "!") :newline? true}]})))
          (.then (fn [_] (is false "malformed record must reject")))
          (.catch (fn [error]
                    (is (= "SessionJsonlError" (.-name error)))
                    (is (= 1 (aget error "lineNumber")))
                    (is (= "/nonexistent/synthetic.jsonl" (aget error "path")))
                    ((:call! client) {:op :start :path "/nonexistent/next.jsonl"})))
          (.then (fn [_] ((:call! client) {:op :release})))
          (.catch #(is false (.-stack %)))
          (.finally (fn [] (-> ((:close! client)) (.then (fn [_] (done))))))))))

(defrecord CommitStore [checkpoint writes completed conflicts fail?]
  store/SessionReplicaStore
  (initialize! [_ _] (js/Promise.resolve nil))
  (get-checkpoint! [_ _] (js/Promise.resolve @checkpoint))
  (write-batch! [_ request]
    (if (and @fail? (seq @writes))
      (js/Promise.reject (js/Error. "write unavailable"))
      (do (swap! writes into (:entries request))
          (reset! checkpoint (:checkpoint request))
          (js/Promise.resolve nil))))
  (complete-session! [_ request]
    (swap! completed inc) (reset! checkpoint (:checkpoint request))
    (js/Promise.resolve nil))
  (mark-conflict! [_ request] (swap! conflicts conj request) (js/Promise.resolve nil))
  (list-sessions! [_ _] (js/Promise.resolve []))
  (read-session! [_ _] (js/Promise.resolve nil))
  (close! [_] (js/Promise.resolve nil)))

(deftest a-crashed-worker-is-replaced-without-skipping-source-records
  (async done
    (let [dir (mkdtempSync (join (tmpdir) "adam-worker-restart-"))
          path (join dir "s.jsonl") attempts (atom 0)
          replica (->CommitStore (atom nil) (atom []) (atom 0) (atom []) (atom false))
          pipeline (background/create-pipeline
                     {:create-processor
                      (fn [] (background/create-client
                               (.-href (pathToFileURL
                                         (if (= 1 (swap! attempts inc))
                                           (join dir "missing-worker.js")
                                           (join (.cwd js/process) "processor.js"))))))})
          input {:replica replica :path path :user-uuid "fixture"}]
      (writeFileSync path (str "{\"type\":\"session\",\"id\":\"s\"}\n"
                              "{\"type\":\"message\",\"id\":\"a\",\"parentId\":null}\n"))
      (-> (js/Promise.resolve nil)
          (.then (fn [_] ((:sync-file! pipeline) input)))
          (.then (fn [_] (is false "first worker must fail to load")))
          (.catch (fn [error]
                    (is (re-find #"worker" (.-message error)))
                    (is (nil? @(:checkpoint replica)))
                    ((:sync-file! pipeline) input)))
          (.then (fn [result]
                   (is (= :mirrored (:status result)))
                   (is (= 2 @attempts))
                   (is (= ["a"] (mapv :entry-id @(:writes replica))))))
          (.catch #(is false (.-stack %)))
          (.finally (fn [] (-> ((:close! pipeline))
                              (.then (fn [_] (rmSync dir #js {:recursive true :force true}) (done))))))))))

(deftest reader-processor-writer-retries-from-only-committed-progress
  (async done
    (let [dir (mkdtempSync (join (tmpdir) "adam-pipeline-"))
          path (join dir "s.jsonl")
          checkpoint (atom nil) writes (atom []) completed (atom 0) conflicts (atom []) fail? (atom true)
          replica (->CommitStore checkpoint writes completed conflicts fail?)
          pipeline (background/create-pipeline
                     {:create-processor
                      (fn [] (let [state (atom nil)]
                               {:call! (fn [event] (.then (js/Promise.resolve nil)
                                                    (fn [_]
                                                      (let [{next-state :state result :result} (processing/advance @state event)]
                                                        (reset! state next-state) result))))
                                :close! (fn [] (js/Promise.resolve nil))}))})
          input {:replica replica :path path :user-uuid "test" :batch-bytes 1}
          contents (str "{\"type\":\"session\",\"id\":\"s\",\"cwd\":\"/repo\"}\n"
                        "{\"type\":\"message\",\"id\":\"a\",\"parentId\":null}\n"
                        "{\"type\":\"message\",\"id\":\"b\",\"parentId\":\"a\"}\n")]
      (writeFileSync path contents)
      (-> (js/Promise.resolve nil)
          (.then (fn [_] ((:sync-file! pipeline) input)))
          (.then (fn [_] (is false "second batch must fail")))
          (.catch (fn [error]
                    (is (= "write unavailable" (.-message error)))
                    (is (= 0 (:complete-through-ordinal @checkpoint)))
                    (is (zero? @completed))
                    (reset! fail? false)
                    ((:sync-file! pipeline) input)))
          (.then (fn [result]
                   (is (= :mirrored (:status result)))
                   (is (= ["a" "b"] (mapv :entry-id @writes)))
                   (is (= 1 @completed))
                   ((:sync-file! pipeline) input)))
          (.then (fn [result]
                   (is (= :unchanged (:status result)))
                   (writeFileSync path (.replace contents "\"/repo\"" "\"/changed\""))
                   ((:sync-file! pipeline) input)))
          (.then (fn [result]
                   (is (= :conflict (:status result)))
                   (is (= 1 (count @conflicts)))
                   (is (= ["a" "b"] (mapv :entry-id @writes)))))
          (.catch #(is false (.-stack %)))
          (.finally (fn [] ((:close! pipeline)) (rmSync dir #js {:recursive true :force true}) (done)))))))
