(ns adam.memory.sync
  (:require [adam.memory.model :as model]
            [adam.memory.scanner :as scanner]
            [adam.memory.store :as store]
            [adam.replica.sync :as replica-sync]))

(defn- sync-summary [scan]
  {:entry-count (:record-count scan)
   :header-next-byte-offset 0
   :header-prefix-hash (:header-prefix-hash scan)
   :log-hash (:log-hash scan)
   :log-bytes (:log-bytes scan)})

(defn- completion-request [stream scan checkpoint]
  {:stream stream
   :checkpoint checkpoint
   :log-bytes (:log-bytes scan)
   :source-bytes (:source-bytes scan)
   :incomplete-tail-bytes (:incomplete-tail-bytes scan)
   :largest-record-bytes (:largest-record-bytes scan)
   :has-final-newline? (:has-final-newline? scan)
   :semantic-conflict (:semantic-conflict scan)})

(defn- conflict! [replica stream scan conflict]
  (let [detail (assoc conflict
                      :stream stream
                      :stream-id (:id stream)
                      :source-file (:path scan))]
    (-> (store/mark-memory-stream-conflict! replica detail)
        (.then (fn [_]
                 {:status :conflict
                  :records-written 0
                  :batches-written 0
                  :conflict detail})))))

(defn sync-sidecar-scan!
  [{:keys [store user-uuid scan batch-bytes]
    :or {batch-bytes replica-sync/default-batch-bytes}}]
  (let [{:keys [stream records]} (model/from-scan user-uuid scan)
        summary (sync-summary scan)]
    (-> (store/ensure-memory-schema! store)
        (.then (fn [_] (store/get-memory-stream-checkpoint! store (:id stream))))
        (.then
         (fn [checkpoint]
           (if (:conflicted checkpoint)
             {:status :conflict
              :records-written 0
              :batches-written 0
              :conflict {:reason :already-conflicted :stream-id (:id stream)}}
             (let [plan (replica-sync/plan-sync summary records checkpoint batch-bytes)]
               (case (:status plan)
                 :conflict
                 (conflict! store stream scan (dissoc plan :status))

                 :unchanged
                 (-> (store/complete-memory-stream!
                      store (completion-request stream scan checkpoint))
                     (.then
                      (fn [_]
                        {:status :unchanged
                         :records-written 0
                         :batches-written 0})))

                 :pending
                 (let [batches (:batches plan)]
                   (-> (reduce
                        (fn [promise batch]
                          (.then
                           promise
                           (fn [_]
                             (store/write-memory-record-batch!
                              store {:stream stream
                                     :records (:entries batch)
                                     :checkpoint (:checkpoint batch)}))))
                        (js/Promise.resolve nil)
                        batches)
                       (.then
                        (fn [_]
                          (store/complete-memory-stream!
                           store (completion-request stream scan (:completion plan)))))
                       (.then
                        (fn [_]
                          (let [written (reduce + 0 (map #(count (:entries %)) batches))]
                            (if-let [reason (:semantic-conflict scan)]
                              (-> (store/mark-memory-stream-conflict!
                                   store {:stream stream
                                          :stream-id (:id stream)
                                          :source-file (:path scan)
                                          :reason reason})
                                  (.then
                                   (fn [_]
                                     {:status :conflict
                                      :records-written written
                                      :batches-written (count batches)
                                      :conflict {:reason reason
                                                 :stream-id (:id stream)}})))
                              {:status :mirrored
                               :records-written written
                               :batches-written (count batches)}))))))))))))))

(defn sync-sidecar-file! [{:keys [path store user-uuid] :as options}]
  (try
    (sync-sidecar-scan!
     (-> options
         (dissoc :path :source-kind :source-session-id :producer-id)
         (assoc :scan (scanner/scan-sidecar
                       {:path path
                        :source-kind (:source-kind options)
                        :source-session-id (:source-session-id options)
                        :producer-id (:producer-id options)}))))
    (catch :default error
      (let [{:keys [type reason]} (ex-data error)]
        (if (and (= :memory-sidecar-error type)
                 (contains? #{:record-too-large :invalid-utf8} reason))
          (let [{:keys [stream]}
                (model/from-scan
                 user-uuid {:path path
                            :source-kind (:source-kind options)
                            :source-session-id (:source-session-id options)
                            :producer-id (:producer-id options)
                            :records []})
                conflict {:stream stream
                          :stream-id (:id stream)
                          :source-file path
                          :reason reason}]
            (-> (store/ensure-memory-schema! store)
                (.then (fn [_] (store/mark-memory-stream-conflict! store conflict)))
                (.then (fn [_]
                         {:status :conflict
                          :records-written 0
                          :batches-written 0
                          :conflict conflict}))))
          (js/Promise.reject error))))))
