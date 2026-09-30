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

(defn- conflict!
  [replica stream scan conflict conflict-class records-written batches-written]
  (let [detail (assoc conflict
                      :stream stream
                      :stream-id (:id stream)
                      :source-file (:path scan)
                      :conflict-class conflict-class)]
    (-> (store/mark-memory-stream-conflict! replica detail)
        (.then (fn [_]
                 {:status :conflict
                  :records-written records-written
                  :batches-written batches-written
                  :conflict detail})))))

(defn- write-batches! [replica stream batches]
  (reduce
   (fn [promise batch]
     (.then promise
            (fn [_]
              (store/write-memory-record-batch!
               replica {:stream stream
                        :records (:entries batch)
                        :checkpoint (:checkpoint batch)}))))
   (js/Promise.resolve nil)
   batches))

(defn sync-sidecar-scan!
  [{:keys [store user-uuid scan batch-bytes]
    :or {batch-bytes replica-sync/default-batch-bytes}}]
  (let [{:keys [stream records]} (model/from-scan user-uuid scan)
        summary (sync-summary scan)]
    (-> (store/ensure-memory-schema! store)
        (.then (fn [_] (store/get-memory-stream-checkpoint! store (:id stream))))
        (.then
         (fn [checkpoint]
           (if (and (:conflicted checkpoint)
                    (= :physical (:conflict-class checkpoint)))
             {:status :conflict
              :records-written 0
              :batches-written 0
              :conflict {:reason :already-conflicted
                         :conflict-class :physical
                         :stream-id (:id stream)}}
             (let [plan (replica-sync/plan-sync summary records checkpoint batch-bytes)
                   physical-conflict (:physical-conflict scan)]
               (case (:status plan)
                 :conflict
                 (conflict! store stream scan (dissoc plan :status)
                            :physical 0 0)

                 :unchanged
                 (if physical-conflict
                   (conflict! store stream scan physical-conflict :physical 0 0)
                   (-> (store/complete-memory-stream!
                        store (completion-request stream scan checkpoint))
                       (.then
                        (fn [_]
                          {:status :unchanged
                           :records-written 0
                           :batches-written 0}))))

                 :pending
                 (let [batches (:batches plan)
                       written (reduce + 0 (map #(count (:entries %)) batches))]
                   (-> (write-batches! store stream batches)
                       (.then
                        (fn [_]
                          (if physical-conflict
                            (conflict! store stream scan physical-conflict
                                       :physical written (count batches))
                            (-> (store/complete-memory-stream!
                                 store (completion-request stream scan (:completion plan)))
                                (.then
                                 (fn [_]
                                   (if-let [reason (:semantic-conflict scan)]
                                     (conflict! store stream scan {:reason reason}
                                                :semantic written (count batches))
                                     {:status :mirrored
                                      :records-written written
                                      :batches-written (count batches)})))))))))))))))))

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
      (let [{:keys [type reason failure-class]} (ex-data error)
            missing? (= "ENOENT" (.-code error))
            reason (if missing? :missing-sidecar reason)
            notification-terminal?
            (or missing?
                (and (= :memory-sidecar-error type)
                     (= :notification-terminal failure-class)))]
        (if notification-terminal?
          (js/Promise.resolve
           {:status :not-ingested
            :notification-disposition :acknowledge
            :reason reason
            :source-file path
            :records-written 0
            :batches-written 0})
          (js/Promise.reject error))))))
