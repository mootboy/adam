(ns adam.memory.reconcile
  (:require [adam.memory.inbox :as inbox]
            [adam.memory.retry :as retry]
            [adam.memory.sync :as sync]))

(defn- invoke! [f input]
  ;; Normalize synchronous scanner/identity failures to the same retry path.
  (.then (js/Promise.resolve nil) (fn [_] (f input))))

(defn- grouped [notifications key-fn]
  (->> notifications (group-by key-fn) (sort-by key) (mapv val)))

(defn- drain-active!
  "Drain one snapshot under a caller-owned serialized worker lease. Dependency
  callbacks establish source ownership and rebuild the aggregate projection;
  success alone permits acknowledgement. Failed groups remain available to the
  next drain while healthy producers/sessions continue. No models run here."
  [{:keys [inbox-options ensure-source! sync-sidecar! project-session! log!
           store user-uuid now-ms now-epoch-ms source-wait-ms]
    :or {log! (fn [_] nil) source-wait-ms retry/default-source-wait-ms}}]
  (let [source-wait-ms (retry/validate-source-wait-ms source-wait-ms)
        now-epoch-ms (or now-epoch-ms #(.now js/Date))
        now-ms (or now-ms #(.now js/performance))
        started-at (now-ms)
        notifications (inbox/pending inbox-options)
        sessions (grouped notifications (juxt :source-kind :source-session-id))
        counters (atom {:acknowledged 0 :failed 0 :source-expired 0 :records-written 0
                        :sessions-projected 0 :rejected 0 :streams-synchronized 0
                        :conflicts 0 :unresolved-citations 0})
        sync-sidecar! (or sync-sidecar! sync/sync-sidecar-file!)
        acknowledge! (fn [group]
                       (doseq [notification group]
                         (inbox/acknowledge! inbox-options notification)
                         (swap! counters update :acknowledged inc)))
        failed! (fn [group reason error]
                  (swap! counters update :failed + (count group))
                  (let [message (or (.-message error) "Unknown Adam reconciliation error")]
                    (log! {:source-kind (:source-kind (first group))
                           :source-session-id (:source-session-id (first group))
                           :producer-id (:producer-id (first group))
                           :reason reason
                           :message (subs message 0 (min 512 (count message)))})))]
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
                          successful (atom [])
                          source-confirmed? (atom false)]
                      (-> (invoke! ensure-source! source)
                          (.then
                           (fn [_]
                             (reset! source-confirmed? true)
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
                                               (.catch (fn [error] (failed! producer-group :sidecar-retry error))))))))
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
                                   (.catch (fn [error] (failed! @successful :projection-retry error)))))))
                          (.catch
                            (fn [error]
                              (if (and (not @source-confirmed?)
                                       (= :missing-source-session (:reason (ex-data error))))
                                (let [now (now-epoch-ms)
                                      expired (filterv #(>= (retry/notification-age-ms % now) source-wait-ms)
                                                       session-group)
                                      expired-ids (set (map :id expired))
                                      remaining (filterv #(not (contains? expired-ids (:id %))) session-group)]
                                  (doseq [group (grouped expired :producer-id)]
                                    (let [parked-count (atom 0)]
                                      (doseq [notification group]
                                        (try
                                          (inbox/park! inbox-options notification)
                                          (acknowledge! [notification])
                                          (swap! counters update :source-expired inc)
                                          (swap! parked-count inc)
                                          (catch :default park-error
                                            (failed! [notification] :source-expiry-retry park-error))))
                                      (when (pos? @parked-count)
                                        (log! (assoc (select-keys (first group) [:source-kind :source-session-id :producer-id])
                                                     :reason :source-never-mirrored
                                                     :notifications @parked-count
                                                     :age-ms (apply max (map #(retry/notification-age-ms % now) group)))))))
                                  (when (seq remaining) (failed! remaining :source-retry error)))
                                (failed! session-group :source-retry error)))))))))
         (js/Promise.resolve nil)
         sessions)
        (.then (fn [_]
                 (assoc @counters :pending (count (inbox/pending inbox-options))
                                  :duration-ms (max 0 (- (now-ms) started-at))))))))

(defn- recover-expired!
  [{:keys [inbox-options ensure-source! log!] :or {log! (fn [_] nil)}}]
  (let [counts (atom {:recovered 0 :recovery-failed 0})
        failed! (fn [notification error]
                  (swap! counts update :recovery-failed inc)
                  (let [message (or (.-message error) "Unknown parked recovery error")]
                    (log! (assoc (select-keys notification [:source-kind :source-session-id :producer-id])
                                 :reason :parked-recovery-retry
                                 :message (subs message 0 (min 512 (count message)))))))]
    (-> (js/Promise.resolve nil)
        (.then (fn [_]
                 (doseq [rejection (inbox/parked-rejections inbox-options)]
                   ;; Unknown/unsafe private snapshots remain for operator repair.
                   (swap! counts update :recovery-failed inc)
                   (log! {:reason :parked-rejection-retained :cause (:reason rejection)}))
                 (reduce
                   (fn [promise notification]
                     (.then promise
                       (fn [_]
                         (-> (invoke! ensure-source! (select-keys notification [:source-kind :source-session-id]))
                             (.then (fn [_]
                                      (inbox/requeue! inbox-options notification)
                                      (swap! counts update :recovered inc)
                                      (log! (assoc (select-keys notification [:source-kind :source-session-id :producer-id])
                                                   :reason :parked-source-recovered))))
                             (.catch (fn [error]
                                       (when-not (= :missing-source-session (:reason (ex-data error)))
                                         (failed! notification error))))))))
                   (js/Promise.resolve nil)
                   (inbox/parked inbox-options))))
        (.then (fn [_] (assoc @counts :parked-pending (count (inbox/parked inbox-options)))))
        (.catch (fn [error] (failed! {} error) @counts)))))

(defn drain-once!
  "Drain active work. Explicit repair can first revive parked locators whose
  source ownership is confirmed. Callers own the notification lease throughout;
  ordinary drains never enumerate the archive or revive missing-source work."
  [{:keys [recover-expired? source-wait-ms ensure-source! project-session!] :as options}]
  (retry/validate-source-wait-ms (if (contains? options :source-wait-ms)
                                  source-wait-ms retry/default-source-wait-ms))
  (when-not (and (fn? ensure-source!) (fn? project-session!))
    (throw (js/Error. "Memory reconciliation requires source and projection boundaries")))
  (if recover-expired?
    (let [now-ms (or (:now-ms options) #(.now js/performance))
          started-at (now-ms)]
      (-> (recover-expired! options)
          (.then (fn [recovery]
                   (-> (drain-active! options)
                       (.then (fn [result]
                                (let [remaining (try (count (inbox/parked (:inbox-options options)))
                                                     (catch :default _ nil))]
                                  (cond-> (-> (merge result (dissoc recovery :parked-pending))
                                              (update :failed + (:recovery-failed recovery))
                                              (assoc :duration-ms (max 0 (- (now-ms) started-at))))
                                    (some? remaining) (assoc :parked-pending remaining))))))))))
    (-> (drain-active! options)
        (.then #(assoc % :recovered 0 :recovery-failed 0)))))
