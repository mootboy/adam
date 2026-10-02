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

(defn- default-log! [message]
  (js/console.error (str (.toISOString (js/Date.)) " adam worker: " message)))

(defn run-worker!
  [{:keys [inbox-options sleep! log! initial-backoff-ms max-backoff-ms max-attempts
           drain! pending? attempt-number]
    :or {initial-backoff-ms 1000 max-backoff-ms 60000 attempt-number 1}
    :as options}]
  (if-let [lease-path (acquire-lease! inbox-options)]
    (-> (.then (js/Promise.resolve nil) (fn [_] ((or drain! #(drain-once! options)))))
        ;; Never hold the shared lease during backoff: Pi may need to mirror the
        ;; missing source session before a memory notification can succeed.
        (.finally #(release-lease! lease-path))
        (.then (fn [result]
                 (if (or (pos? (:pending result))
                         (if pending? (pending?) (seq (inbox/pending inbox-options))))
                   (run-worker! (assoc options :attempt-number 1))
                   (assoc result :status :idle)))
               (fn [error]
                  (if (and max-attempts (>= attempt-number max-attempts))
                    (js/Promise.reject error)
                    (let [delay (min max-backoff-ms
                                     (* initial-backoff-ms (js/Math.pow 2 (min 20 (dec attempt-number)))))]
                      ((or log! default-log!)
                       (str "attempt " attempt-number " failed, retrying in " delay " ms: " (.-message error)))
                      (-> ((or sleep! default-sleep!) delay)
                          (.then (fn [_] (run-worker! (assoc options :attempt-number (inc attempt-number)))))))))))
    (js/Promise.resolve {:status :busy})))

(defn with-lease!
  "Run one host operation under the same lease as detached reconciliation."
  [inbox-options work!]
  (if-let [lease-path (acquire-lease! inbox-options)]
    (-> (.then (js/Promise.resolve nil) (fn [_] (work!)))
        (.finally #(release-lease! lease-path)))
    (js/Promise.reject (ex-info "Adam reconciliation is already running" {:reason :worker-busy}))))

(defn run-once!
  [{:keys [inbox-options drain!] :as options}]
  (if-let [lease-path (acquire-lease! inbox-options)]
    (-> (.then (js/Promise.resolve nil)
               (fn [_] (if drain! (drain!) (drain-once! options))))
        (.then #(assoc % :status :drained))
        (.finally #(release-lease! lease-path)))
    (js/Promise.resolve {:status :busy})))

(defn drain-once!
  [{:keys [inbox-options process! log!]}]
  (let [log! (or log! default-log!)
        notifications (inbox/pending inbox-options)
        groups (coalesced-groups notifications)
        failures (atom [])]
    ;; One stream's failure keeps its notifications for retry but must not
    ;; block the streams queued behind it.
    (-> (reduce
         (fn [promise group]
           (.then promise
                  (fn [processed]
                    (let [notification (last group)]
                      (-> (.then (js/Promise.resolve nil) (fn [_] (process! notification)))
                          (.then
                           (fn [result]
                             (when (= :missing-transcript (:status result))
                               (log! (str "acknowledged " (:event notification)
                                          " for a missing transcript: "
                                          (:transcript-path result))))
                             (doseq [notification group]
                               (inbox/acknowledge! inbox-options notification))
                             (+ processed (count group))))
                          (.catch
                           (fn [error]
                             (log! (str "reconciliation of " (:transcript-path notification)
                                        " failed: " (.-message error)))
                             (swap! failures conj error)
                             processed)))))))
         (js/Promise.resolve 0)
         groups)
        (.then
         (fn [processed]
           (if-let [error (first @failures)]
             (js/Promise.reject error)
             {:processed processed
              :pending (count (inbox/pending inbox-options))}))))))
