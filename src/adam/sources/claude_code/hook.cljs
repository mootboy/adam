(ns adam.sources.claude-code.hook
  (:require [adam.sources.claude-code.inbox :as inbox]
            ["node:child_process" :refer [spawn]]
            ["node:fs" :refer [closeSync mkdirSync openSync]]
            ["node:path" :as node-path]))

(def ^:private max-input-bytes (* 64 1024))

(defn- parse-input [raw-input]
  (when (> (js/Buffer.byteLength raw-input "utf8") max-input-bytes)
    (throw (js/Error. "Claude hook input exceeds 64 KiB")))
  (let [value (js/JSON.parse raw-input)]
    (when (or (nil? value)
              (not= "object" (goog/typeOf value))
              (js/Array.isArray value))
      (throw (js/Error. "Claude hook input must be a JSON object")))
    (let [event (aget value "hook_event_name")
          parent-path (aget value "transcript_path")
          agent-path (aget value "agent_transcript_path")]
      {:hook-event-name event
       :session-id (aget value "session_id")
       :transcript-path (if (= "SubagentStop" event) agent-path parent-path)
       :parent-transcript-path (when (= "SubagentStop" event) parent-path)
       :cwd (aget value "cwd")
       :agent-id (aget value "agent_id")})))

(declare handle!)

(defn- spawn-worker! [worker-path]
  ;; The detached worker has no terminal, so its stderr diagnostics go to an
  ;; owner-only append log next to the inbox.
  (let [log-path (inbox/worker-log-path {})
        log-fd (try
                 (mkdirSync (.dirname node-path log-path) #js {:recursive true :mode 448})
                 (openSync log-path "a" 384)
                 (catch :default _ nil))
        child (spawn js/process.execPath #js [worker-path]
                     #js {:detached true
                          :stdio #js ["ignore" "ignore" (or log-fd "ignore")]
                          :env js/process.env})]
    (when log-fd (closeSync log-fd))
    (.once child "error" (fn [_] nil))
    (.unref child)))

(defn start [raw-input worker-path]
  (handle! {:inbox-options {}
            :spawn-worker! #(spawn-worker! worker-path)}
           raw-input))

(defn handle!
  [{:keys [inbox-options spawn-worker!]} raw-input]
  (let [notification (inbox/enqueue! inbox-options (parse-input raw-input))]
    (spawn-worker!)
    {:status :queued
     :notification-id (:id notification)}))
