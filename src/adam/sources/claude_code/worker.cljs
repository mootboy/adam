(ns adam.sources.claude-code.worker
  (:require [adam.sources.claude-code.inbox :as inbox]
            ["node:fs" :refer [closeSync mkdirSync openSync readFileSync rmSync statSync writeFileSync]]
            ["node:path" :as node-path]))

(defn- process-alive? [pid]
  (try
    (.kill js/process pid 0)
    true
    (catch :default error
      (if (= "ESRCH" (.-code error)) false true))))

(defn- stale-lease? [path]
  (try
    (let [pid (js/Number.parseInt (readFileSync path "utf8") 10)]
      (if (and (js/Number.isSafeInteger pid) (pos? pid))
        (not (process-alive? pid))
        (> (- (.now js/Date) (.-mtimeMs (statSync path))) (* 5 60 1000))))
    (catch :default _ false)))

(defn- create-lease-file! [path]
  (try
    (let [fd (openSync path "wx" 384)]
      (try
        (writeFileSync fd (str js/process.pid "\n") "utf8")
        (finally (closeSync fd)))
      path)
    (catch :default error
      (if (= "EEXIST" (.-code error)) nil (throw error)))))

(defn- acquire-lease! [inbox-options]
  (let [path (inbox/worker-lock-path inbox-options)
        recovery-path (str path ".recovery")]
    (mkdirSync (.dirname node-path path) #js {:recursive true :mode 448})
    (or (create-lease-file! path)
        (when (stale-lease? path)
          ;; Serialize stale-lock replacement so two hook wake-ups cannot both
          ;; remove and replace the same lease.
          (when (or (create-lease-file! recovery-path)
                    (when (stale-lease? recovery-path)
                      (rmSync recovery-path #js {:force true})
                      (create-lease-file! recovery-path)))
            (try
              (when (stale-lease? path)
                (rmSync path #js {:force true}))
              (create-lease-file! path)
              (finally
                (rmSync recovery-path #js {:force true}))))))))

(defn- release-lease! [path]
  (rmSync path #js {:force true}))

(declare drain-once!)

(defn- coalesced-groups [notifications]
  (let [{:keys [order groups]}
        (reduce
         (fn [{:keys [order groups]} notification]
           (let [key [(:session-id notification)
                      (:transcript-path notification)
                      (:agent-id notification)]]
             {:order (if (contains? groups key) order (conj order key))
              :groups (update groups key (fnil conj []) notification)}))
         {:order [] :groups {}}
         notifications)]
    (mapv groups order)))

(defn- default-sleep! [milliseconds]
  (js/Promise. (fn [resolve _] (js/setTimeout resolve milliseconds))))

(defn run-worker!
  [{:keys [inbox-options sleep! initial-backoff-ms max-backoff-ms max-attempts]
    :or {initial-backoff-ms 1000 max-backoff-ms 60000}
    :as options}]
  (if-let [lease-path (acquire-lease! inbox-options)]
    (let [sleep! (or sleep! default-sleep!)]
      (letfn [(attempt [attempt-number]
                (-> (drain-once! options)
                    (.then
                     (fn [result]
                       (if (pos? (:pending result))
                         (attempt 1)
                         (assoc result :status :idle))))
                    (.catch
                     (fn [error]
                       (if (and max-attempts (>= attempt-number max-attempts))
                         (js/Promise.reject error)
                         (let [delay (min max-backoff-ms
                                          (* initial-backoff-ms
                                             (js/Math.pow 2 (dec attempt-number))))]
                           (-> (sleep! delay)
                               (.then (fn [_] (attempt (inc attempt-number)))))))))))]
        (-> (attempt 1)
            (.finally #(release-lease! lease-path))
            (.then
             (fn [result]
               ;; Closing the lease before the final check prevents an enqueue/wake
               ;; race from stranding a notification.
               (if (seq (inbox/pending inbox-options))
                 (run-worker! options)
                 result))))))
    (js/Promise.resolve {:status :busy})))

(defn run-once!
  [{:keys [inbox-options] :as options}]
  (if-let [lease-path (acquire-lease! inbox-options)]
    (-> (drain-once! options)
        (.then #(assoc % :status :drained))
        (.finally #(release-lease! lease-path)))
    (js/Promise.resolve {:status :busy})))

(defn drain-once!
  [{:keys [inbox-options process!]}]
  (let [notifications (inbox/pending inbox-options)
        groups (coalesced-groups notifications)]
    (-> (reduce
         (fn [promise group]
           (.then promise
                  (fn [processed]
                    (-> (process! (last group))
                        (.then
                         (fn [_]
                           (doseq [notification group]
                             (inbox/acknowledge! inbox-options notification))
                           (+ processed (count group))))))))
         (js/Promise.resolve 0)
         groups)
        (.then
         (fn [processed]
           {:processed processed
            :pending (count (inbox/pending inbox-options))})))))
