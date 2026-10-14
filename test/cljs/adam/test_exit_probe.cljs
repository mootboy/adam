(ns adam.test-exit-probe
  ;; Deliberately excluded from normal/live ns-regexps. A dedicated compiled
  ;; fixture proves the stock cljs.test runner's exit status, without Adam IO.
  (:require [cljs.test :refer [async deftest is]]))

(deftest assertion-outcome
  (is (not= "assertion" (aget js/process.env "ADAM_TEST_EXIT_PROBE"))))

(deftest asynchronous-error-outcome
  (async done
    (case (aget js/process.env "ADAM_TEST_EXIT_PROBE")
      "active-hang" (js/setInterval (fn []) 1000)
      "incomplete" nil ;; no done and no active handles: must not silently exit 0
      "uncaught-rejection" (js/setTimeout #(js/Promise.reject (js/Error. "intentional uncaught runner probe")) 0)
      (-> (js/Promise.resolve)
          (.then (fn [_]
                   (if (= "captured-error" (aget js/process.env "ADAM_TEST_EXIT_PROBE"))
                     (is (throw (js/Error. "intentional content-free runner probe")))
                     (is true))))
          (.finally done)))))
