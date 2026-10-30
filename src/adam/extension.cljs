(ns adam.extension
  (:require [adam.replica.register :as replica]))

(defn start [pi processor-url notification-worker-url]
  (replica/register! pi {:processor-url processor-url :notification-worker-url notification-worker-url})
  nil)
