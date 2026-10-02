(ns adam.memory.reconcile
  (:require [adam.memory.inbox :as inbox]
            [adam.memory.sync :as sync]))

(defn- invoke! [f input]
  ;; Normalize synchronous scanner/identity failures to the same retry path.
  (.then (js/Promise.resolve nil) (fn [_] (f input))))

(defn- grouped [notifications key-fn]
  (->> notifications (group-by key-fn) (sort-by key) (mapv val)))

(defn drain-once!
  "Drain one snapshot under a caller-owned serialized worker lease. Dependency
  callbacks establish source ownership and rebuild the aggregate projection;
  success alone permits acknowledgement. Failed groups remain available to the
  next drain while healthy producers/sessions continue. No models run here."
  [{:keys [inbox-options ensure-source! sync-sidecar! project-session! log!
           store user-uuid now-ms]
    :or {log! (fn [_] nil)}}]
  (let [now-ms (or now-ms #(.now js/performance))
        started-at (now-ms)
        notifications (inbox/pending inbox-options)
        sessions (grouped notifications (juxt :source-kind :source-session-id))
        counters (atom {:acknowledged 0 :failed 0 :records-written 0
                        :sessions-projected 0 :rejected 0 :streams-synchronized 0
                        :conflicts 0 :unresolved-citations 0})
        sync-sidecar! (or sync-sidecar! sync/sync-sidecar-file!)
        acknowledge! (fn [group]
                       (doseq [notification group]
                         (inbox/acknowledge! inbox-options notification)
                         (swap! counters update :acknowledged inc)))
        failed! (fn [group reason]
                  (swap! counters update :failed + (count group))
                  (log! {:source-kind (:source-kind (first group))
                         :source-session-id (:source-session-id (first group))
                         :producer-id (:producer-id (first group))
                         :reason reason}))]
    (when-not (and (fn? ensure-source!) (fn? project-session!))
      (throw (js/Error. "Memory reconciliation requires source and projection boundaries")))
    (doseq [rejection (inbox/rejections inbox-options)]
      (log! (select-keys rejection [:reason]))
      (try
        (inbox/acknowledge! inbox-options rejection)
        (swap! counters update :rejected inc)
        (catch :default _
          ;; A directory or concurrently replaced unsafe entry is not recursively
          ;; removed. Its rejection must not prevent healthy notifications.
          (log! {:reason :unsafe-rejection-retained}))))
    (-> (reduce
         (fn [promise session-group]
           (.then promise
                  (fn [_]
                    (let [source (select-keys (first session-group)
                                              [:source-kind :source-session-id])
                          successful (atom [])]
                      (-> (invoke! ensure-source! source)
                          (.then
                           (fn [_]
                             (reduce
                              (fn [promise producer-group]
                                (.then promise
                                       (fn [_]
                                         (let [notification (last producer-group)]
                                           (-> (invoke!
                                                sync-sidecar!
                                                (assoc (select-keys notification
                                                                    [:source-kind :source-session-id :producer-id])
                                                       :store store :user-uuid user-uuid
                                                       :path (inbox/sidecar-path inbox-options notification)))
                                               (.then
                                                (fn [result]
                                                  (swap! counters update :streams-synchronized inc)
                                                  (when (= :conflict (:status result))
                                                    (swap! counters update :conflicts inc))
                                                  (swap! counters update :records-written +
                                                         (or (:records-written result) 0))
                                                  (if (= :acknowledge (:notification-disposition result))
                                                    (do (log! (assoc source :producer-id (:producer-id notification)
                                                                    :reason (:reason result)))
                                                        (acknowledge! producer-group))
                                                    (if (contains? #{:mirrored :unchanged :conflict} (:status result))
                                                      (swap! successful into producer-group)
                                                      (throw (js/Error. "Unexpected memory synchronization result"))))))
                                               (.catch (fn [_] (failed! producer-group :sidecar-retry))))))))
                              (js/Promise.resolve nil)
                              (grouped session-group :producer-id))))
                          (.then
                           (fn [_]
                             (when (seq @successful)
                               (-> (invoke! project-session! source)
                                   (.then
                                    (fn [projection-result]
                                      (swap! counters update :unresolved-citations +
                                             (or (:unresolved-citations projection-result) 0))
                                      (acknowledge! @successful)
                                      (swap! counters update :sessions-projected inc)))
                                   (.catch (fn [_] (failed! @successful :projection-retry)))))))
                          (.catch (fn [_] (failed! session-group :source-retry))))))))
         (js/Promise.resolve nil)
         sessions)
        (.then (fn [_]
                 (assoc @counters :pending (count (inbox/pending inbox-options))
                                  :duration-ms (max 0 (- (now-ms) started-at))))))))
