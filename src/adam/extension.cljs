(ns adam.extension
  (:require [adam.replica.register :as replica]))

(defn start [pi]
  (replica/register! pi)
  nil)
