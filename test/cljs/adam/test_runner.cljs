(ns adam.test-runner
  {:dev/always true}
  (:require [shadow.test.node :as runner]
            [shadow.test.env :as env]))

(goog-define DEFAULT_TIMEOUT_MS 180000)

(defn main [& arguments]
  ;; The stock runner explicitly exits after :end-run-tests. Natural Node exit
  ;; instead means async tests never completed (e.g. missing done/no handles).
  (.once js/process "beforeExit"
    (fn [_]
      (js/console.error "ClojureScript runner ended without completion")
      (set! (.-exitCode js/process) 1)))
  ;; Whole-run budget, not per-test. Live builds have a larger default.
  (let [raw (aget js/process.env "ADAM_TEST_RUN_TIMEOUT_MS")
        budget (if raw (js/Number raw) DEFAULT_TIMEOUT_MS)
        started (.now js/Date)]
    (when-not (and (or (nil? raw) (re-matches #"[1-9][0-9]*" raw))
                   (js/Number.isSafeInteger budget) (<= 10 budget 3600000))
      (js/console.error "Invalid ADAM_TEST_RUN_TIMEOUT_MS (10..3600000 ms)")
      (.exit js/process 1))
    ;; Unref never keeps an incomplete test alive on its own.
    (doto (js/setTimeout
            (fn []
              (js/console.error (str "ClojureScript runner completion timeout: elapsed "
                                     (- (.now js/Date) started) " ms; budget " budget " ms"))
              (.exit js/process 1)) budget)
      (.unref)))
  ;; Compile this macro in the configured runner namespace, after all selected
  ;; test namespaces, rather than using stock node/main's earlier snapshot.
  (env/reset-test-data! (env/get-test-data))
  (let [options (runner/parse-args arguments)]
    (when (or (empty? (env/get-test-vars))
              (and (seq (:test-syms options))
                   (empty? (runner/find-matching-test-vars (:test-syms options)))))
      (js/console.error "No ClojureScript tests registered or selected")
      (.exit js/process 1))
    (runner/execute-cli options)))
