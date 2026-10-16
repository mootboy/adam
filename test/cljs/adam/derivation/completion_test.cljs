(ns adam.derivation.completion-test
  (:require [adam.derivation.pi :as pi]
            [adam.derivation.read :as read]
            [cljs.test :refer [deftest is]]))

(deftest registered-command-completions-are-context-aware-and-side-effect-free
  (let [commands (atom {}) calls (atom 0)
        forbidden (fn [& _] (swap! calls inc) (throw (js/Error. "completion must not load state")))
        extension #js {:registerCommand (fn [name definition] (swap! commands assoc name definition))
                       :registerTool (fn [& _])}]
    (pi/register-command! extension {:configured? true :get-db! forbidden :load-config! forbidden :provider-fn forbidden})
    (read/register-pi! extension {:load-configuration! forbidden :load-snapshot! forbidden} true)
    (doseq [command ["adam:derive" "adam:derivations"]]
      (let [complete (aget (get @commands command) "getArgumentCompletions")]
        (is (= "function" (goog/typeOf complete)))
        (when complete
          (doseq [[prefix expected] [["--dep" ["--depth"]]
                                     ["preferences --depth re" ["preferences --depth reflections"]]
                                     ["  preferences  --depth\tobs" ["  preferences  --depth\tobservations"]]
                                     ["preferences --language clojure" ["preferences --language clojure" "preferences --language clojurescript"]]
                                     ["preferences --repository " []]
                                     ["preferences --session urn:" []]
                                     ["preferences --depth bogus --" []]
                                     ["preferences --bogus " []]
                                     ["preferences --depth full --depth " []]]]
            (is (= expected (mapv #(aget % "value") (or (complete prefix) #js []))) (str command " " prefix)))
          (let [values (set (map #(aget % "value") (complete "preferences --depth full --")))]
            (is (not (contains? values "preferences --depth full --depth")))
            (is (contains? values "preferences --depth full --repository")))
          (let [values (map #(aget % "label") (complete "preferences --repository https://example.com/r --"))]
            (is (not-any? #{"--repository" "--session" "--language"} values))
            (is (some #{"--depth"} values)))
          (if (= command "adam:derive")
            (is (= #{"--depth" "--repository" "--session" "--language"}
                   (set (map #(aget % "value") (complete "")))))
            (do
              (is (= ["preferences --limit 2" "preferences --limit 20"]
                     (mapv #(aget % "value") (complete "preferences --limit 2"))))
              (is (nil? (complete "preferences --cursor ")))
              (is (nil? (complete "preferences --evidence ")))
              (is (not-any? #(= "--history" (aget % "label")) (complete "preferences --history --"))))))))
    (is (zero? @calls))))
