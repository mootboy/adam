(ns adam.derivation.completion-test
  (:require [adam.derivation.completion :as completion]
            [adam.derivation.pi :as pi]
            [adam.derivation.read :as read]
            [cljs.test :refer [async deftest is]]))

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

(deftest repository-values-share-a-local-first-cache-across-commands
  (async done
    (let [time (atom 0) cwd (atom "/checkout") calls (atom []) graph (js/Promise.withResolvers)
          cache (completion/repository-cache
                 {:now-ms #(deref time) :get-cwd #(deref cwd)
                  :load-checkout! (fn [directory] (swap! calls conj [:git directory])
                                    (js/Promise.resolve "git@EXAMPLE.com:current.git"))
                  :load-authorized! (fn [_] (swap! calls conj :graph)
                                      (if (zero? @time) (.-promise graph) (js/Promise.reject (js/Error. "offline"))))})
          commands (atom {}) extension #js {:registerCommand (fn [name definition] (swap! commands assoc name definition))
                                            :registerTool (fn [& _])}
          _ (pi/register-command! extension {:complete-arguments! #((:complete! cache) false %)})
          _ (read/register-pi! extension {:complete-arguments! #((:complete! cache) true %)} true)
          complete (fn [name prefix] ((aget (get @commands name) "getArgumentCompletions") prefix))
          values #(mapv (fn [item] (aget item "value")) %)]
      (is (seq (complete "adam:derive" "--dep")))
      (is (empty? @calls))
      (-> (js/Promise.resolve (complete "adam:derive" "preferences --repository "))
          (.then (fn [items]
                   (is (= ["preferences --repository https://example.com/current"] (values items)))
                   (is (= #{[:git "/checkout"] :graph} (set @calls)))
                   ((.-resolve graph) ["https://alice:SECRET@example.com/granted.git" "example.com/current"
                                      "git@example.com:bad\u202e" "git@example.com:bad space" "git@example.com:工程"])
                   ((:warm! cache))))
          (.then (fn [_] (complete "adam:derive" "preferences --repository git@")))
          (.then (fn [items]
                   (is (= ["preferences --repository git@example.com:工程"] (values items)))
                   (complete "adam:derivations" "preferences --repository https://example.com/g")))
          (.then (fn [items]
                   (is (= ["preferences --repository https://example.com/granted"] (values items)))
                   (is (= 2 (count @calls)))
                   (reset! time 31000)
                   (complete "adam:derive" "preferences --repository ")))
          (.then (fn [items]
                   (is (= ["preferences --repository https://example.com/current"] (values items))
                       "expired authorized hints are not retained after a failed reload")
                   (reset! cwd "/another")
                   (complete "adam:derivations" "--repository ")))
          (.then (fn [_]
                   (is (= 2 (count (filter #{:graph} @calls))) "cwd changes do not refetch the same authorized list")
                   (is (some #{[:git "/another"]} @calls))
                   ((:close! cache))
                   (is (nil? (complete "adam:derive" "--repository ")))))
          (.catch (fn [error] (is false (.-stack error))))
          (.finally done)))))
