(ns adam.replica.background
  (:require [adam.replica.processing :as processing]
            [adam.replica.jsonl :as jsonl]
            [adam.replica.sync :as sync]
            [adam.replica.identity :as identity]
            [adam.replica.store :as store]
            [adam.knowledge.repository :as repository]
            [adam.knowledge.store :as knowledge-store]
            [cljs.spec.alpha :as s]
            ["node:worker_threads" :refer [Worker parentPort]]))

(s/def ::op #{:start :records :finish :prepare :plan :batch :projection :release})
(s/def ::event (s/keys :req-un [::op]))

(defn start-worker []
  (when-not parentPort (throw (js/Error. "processor requires a worker thread")))
  (let [state (atom nil)]
    (.on parentPort "message"
      (fn [packet]
        (try
          (let [event (update (js->clj (.-event packet) :keywordize-keys true) :op keyword)]
            (when-not (s/valid? ::event event) (throw (js/Error. "invalid processing event")))
            (let [{next-state :state result :result} (processing/advance @state event)]
              ;; Set only after successful calculation; failed events do not
              ;; mutate prior processing state or claim committed progress.
              (reset! state next-state)
              (.postMessage parentPort #js {:id (.-id packet) :value (clj->js result)})))
          (catch :default error
            (.postMessage parentPort
              #js {:id (.-id packet)
                   :error #js {:name (.-name error) :message (.-message error)
                               :path (aget error "path") :lineNumber (aget error "lineNumber")}})))))))

(defn create-client [url]
  (when-not url (throw (js/Error. "processing worker entry is not configured")))
  (let [worker (Worker. (js/URL. url) #js {:name "adam-processing" :execArgv #js [] :env #js {}})
        sequence (atom 0) pending (atom {}) closed? (atom false)
        reject-all! (fn [error]
                      (let [waiting (vals @pending)]
                        (reset! pending {})
                        (doseq [{:keys [reject]} waiting] (reject error))))]
    (.unref worker)
    (.on worker "message"
      (fn [packet]
        (when-let [{:keys [resolve reject op]} (get @pending (.-id packet))]
          (swap! pending dissoc (.-id packet))
          (when (empty? @pending) (.unref worker))
          (if-let [failure (.-error packet)]
            (let [error (js/Error. (.-message failure))]
              (aset error "name" (or (.-name failure) "Error"))
              (aset error "path" (.-path failure))
              (aset error "lineNumber" (.-lineNumber failure))
              (reject error))
            (let [value (js->clj (.-value packet) :keywordize-keys true)
                  value (case op
                          :plan (update value :status keyword)
                          :projection (-> value
                                          (update :available-source-entries set)
                                          (update :memory-diagnostics
                                                  #(mapv (fn [item] (update item :reason keyword)) %)))
                          value)]
              (resolve value))))))
    (.on worker "error" (fn [_] (reset! closed? true) (reject-all! (js/Error. "processing worker failed"))))
    (.on worker "exit" (fn [_] (reset! closed? true) (reject-all! (js/Error. "processing worker stopped"))))
    {:call! (fn [event]
              (js/Promise.
                (fn [resolve reject]
                  (if @closed?
                    (reject (js/Error. "processing worker is closed"))
                    (let [id (swap! sequence inc)]
                      (swap! pending assoc id {:resolve resolve :reject reject :op (:op event)})
                      (.ref worker)
                      (try (.postMessage worker #js {:id id :event (clj->js event)})
                           (catch :default error
                             (swap! pending dissoc id)
                             (when (empty? @pending) (.unref worker))
                             (reject error))))))))
     :close! (fn []
               (if (compare-and-set! closed? false true)
                 (do (reject-all! (js/Error. "processing worker is closed")) (.terminate worker))
                 ;; A failed worker may still be exiting; terminate remains safe.
                 (.terminate worker)))}))

(defn create-queue
  "Serial catch-up requests. Only unsent automatic requests coalesce; raw record
   events are backpressured separately and may never be discarded. Explicit work
   forms an ordering barrier. The queue starts on a later event-loop iteration."
  [{:keys [run! on-status on-error]}]
  (let [pending (atom []) running (atom nil) scheduled? (atom false)
        accepting? (atom true) waiters (atom []) sequence (atom 0)
        report! (fn [] (when on-status
                         (on-status {:running (some-> @running :id)
                                     :pending (count @pending)
                                     :target (some-> (or (peek @pending) @running) :request :path)})))
        settle-idle! (fn [] (when (and (nil? @running) (empty? @pending))
                             (let [waiting @waiters] (reset! waiters [])
                               (doseq [resolve waiting] (resolve nil)))))]
    (letfn [(pump! []
              (reset! scheduled? false)
              (when (and (nil? @running) (seq @pending))
                (let [item (first @pending)]
                  (swap! pending #(subvec % 1))
                  (reset! running item)
                  (report!)
                  (-> (.then (js/Promise.resolve nil) (fn [_] (run! (:request item))))
                      (.then (fn [result] (doseq [[resolve _] (:waiters item)] (resolve result)))
                             (fn [error]
                               (when on-error (on-error error))
                               (doseq [[_ reject] (:waiters item)] (reject error))))
                      (.finally (fn [] (reset! running nil) (report!) (pump!) (settle-idle!))))))
              (settle-idle!))
            (schedule! []
              (when (and (nil? @running) (compare-and-set! scheduled? false true))
                (js/setImmediate pump!)))]
      {:enqueue! (fn [request coalesce?]
                   (js/Promise.
                     (fn [resolve reject]
                       (if-not @accepting?
                         (reject (js/Error. "reconciliation queue is closed"))
                         (let [id (swap! sequence inc)
                               item {:id id :request request :coalesce? coalesce? :waiters (if coalesce? [] [[resolve reject]])}
                               ;; Do not move requests across an explicit command.
                               matching (when coalesce?
                                          (loop [i (dec (count @pending))]
                                            (when (>= i 0)
                                              (let [candidate (nth @pending i)]
                                                (when (:coalesce? candidate)
                                                  (if (= (:path request) (get-in candidate [:request :path]))
                                                    i (recur (dec i))))))))]
                           (if (some? matching)
                             (swap! pending update matching
                                    #(assoc item :waiters (into (:waiters %) (:waiters item))))
                             (swap! pending conj item))
                           (when coalesce? (resolve nil))
                           (report!) (schedule!))))))
       :flush! (fn [] (if (and (nil? @running) (empty? @pending))
                       (js/Promise.resolve nil)
                       (js/Promise. (fn [resolve _] (swap! waiters conj resolve)))))
       :stop! (fn [] (reset! accepting? false))
       :cancel! (fn []
                  (reset! accepting? false)
                  (let [items @pending] (reset! pending [])
                    (doseq [item items [_ reject] (:waiters item)]
                      (reject (js/Error. "reconciliation deferred until restart"))))
                  (report!) (settle-idle!))})))

(defn create-pipeline
  "Reader → record events → pure processor → guarded writer. The queue serializes
   calls to sync-file!, while each record/batch delivery applies backpressure."
  [{:keys [processor-url create-processor now-ms]
    :or {now-ms #(.now js/performance)}}]
  (let [client (atom nil) closed? (atom false) schema (atom nil)
        check! #(when @closed? (throw (js/Error. "reconciliation is stopping")))
        get-client! (fn []
                      (check!)
                      (or @client
                          (let [created (if create-processor (create-processor) (create-client processor-url))]
                            (reset! client created) created)))]
    {:sync-file!
     (fn [{:keys [replica path user-uuid current-leaf-id resolve-repository
                  on-projection on-projection-error on-timing] :as input}]
       (let [started (now-ms) timings (atom {:replica-sync-ms 0 :repository-discovery-ms 0
                                           :evidence-extraction-ms 0 :neo4j-projection-ms 0})
             measure! (fn [key start] (swap! timings update key + (max 0 (- (now-ms) start))))
             owned (get-client!)
             call! (fn [event] (check!) ((:call! owned) event))
             delivered (atom 0)
             projection!
             (fn []
               (when (satisfies? knowledge-store/FileEvidenceStore replica)
                 (let [extraction-start (now-ms)]
                   (-> (call! {:op :prepare})
                       (.finally #(measure! :evidence-extraction-ms extraction-start))
                       (.then
                         (fn [{:keys [summary file-locators]}]
                           (let [repo-start (now-ms)]
                             (-> (if (:cwd summary)
                                   (repository/resolve-file-repositories!
                                     resolve-repository (:cwd summary) nil file-locators)
                                   (js/Promise.resolve []))
                                 (.finally #(measure! :repository-discovery-ms repo-start))
                                 (.then
                                   (fn [repositories]
                                     (let [graph-start (now-ms)
                                           session-id (identity/session-urn user-uuid (:pi-session-id summary))]
                                       (-> (or @schema
                                               (let [pending (-> (knowledge-store/ensure-file-evidence-schema! replica)
                                                                 (.catch (fn [error] (reset! schema nil)
                                                                           (js/Promise.reject error))))]
                                                 (reset! schema pending) pending))
                                           (.then (fn [_]
                                                    (check!)
                                                    (if (satisfies? knowledge-store/AggregateMemoryStore replica)
                                                      (knowledge-store/read-session-memory-streams!
                                                        replica (identity/user-urn user-uuid) session-id)
                                                      [])))
                                           (.finally #(measure! :neo4j-projection-ms graph-start))
                                           (.then
                                             (fn [streams]
                                               (let [extract-start (now-ms)]
                                                 (-> (call! {:op :projection :repositories repositories
                                                             :user-uuid user-uuid :streams streams})
                                                     (.finally #(measure! :evidence-extraction-ms extract-start))))))
                                           (.then
                                             (fn [projection]
                                               (check!)
                                               (let [write-start (now-ms)]
                                                 (-> (knowledge-store/index-file-evidence! replica projection)
                                                     (.finally #(measure! :neo4j-projection-ms write-start))
                                                     (.then
                                                       (fn [_]
                                                         (when on-projection
                                                           (on-projection (first repositories) projection))))))))))))))))))))]
         (-> (call! {:op :start :path path})
             (.then (fn [_]
                      (jsonl/read-records! path
                        (fn [records]
                          (let [begin @delivered
                                size (reduce + 0 (map #(+ (.-length (:bytes %))
                                                           (if (:newline? %) 1 0)) records))]
                            (-> (call! {:op :records :byte-start begin :records records})
                                (.then (fn [_] (swap! delivered + size)))))))))
             (.then (fn [read-result]
                      (call! (cond-> {:op :finish :expected-bytes (:bytes read-result)}
                               (contains? input :current-leaf-id)
                               (assoc :current-leaf-id current-leaf-id)))))
             (.then
               (fn [{:keys [summary]}]
                 (-> (js/Promise.all
                       #js [(store/get-checkpoint! replica
                              (identity/session-urn user-uuid (:pi-session-id summary)))
                            (when-let [parent (:parent-session summary)]
                              (jsonl/read-header-bytes! parent))])
                     (.then
                       (fn [resolved]
                         (call! (cond-> {:op :plan :user-uuid user-uuid
                                         :checkpoint (aget resolved 0) :parent-header (aget resolved 1)
                                         :batch-bytes (:batch-bytes input)}
                                  (contains? input :current-leaf-id)
                                  (assoc :current-leaf-id current-leaf-id)))))
                     (.then
                       (fn [plan]
                         (sync/apply-plan! {:replica replica :session (:session plan)
                                           :source-file path :summary summary :plan plan
                                           :check-active! check!
                                           :read-batch! #(call! {:op :batch :index %})}))))))
             (.finally #(measure! :replica-sync-ms started))
             (.then
               (fn [result]
                 (if (= :conflict (:status result))
                   ;; Recorded source conflicts are terminal for this snapshot,
                   ;; not retryable projection failures that block every rebuild.
                   result
                   (-> (.then (js/Promise.resolve nil) (fn [_] (projection!)))
                       (.catch (fn [error]
                                 (when on-projection-error (on-projection-error error))
                                 (when (:strict-projection? input) (throw error))))
                       (.then (fn [_] result))))))
             (.catch
               (fn [error]
                 ;; A fresh source snapshot can rebuild the disposable processing
                 ;; state. A failed job must not poison the next source/session.
                 (when (= owned @client) (reset! client nil))
                 ((:close! owned))
                 (js/Promise.reject error)))
             (.finally
               (fn []
                 (when on-timing (on-timing @timings))
                 (-> ((:call! owned) {:op :release}) (.catch (fn [_] nil))))))))
     :close! (fn []
               (reset! closed? true)
               (if-let [owned @client]
                 (do (reset! client nil) ((:close! owned)))
                 (js/Promise.resolve nil)))}))
