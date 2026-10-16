(ns adam.derivation.completion
  (:require [adam.derivation.config :as config]
            [adam.derivation.core :as core]
            [adam.knowledge.evidence :as repository]
            [cljs.spec.alpha :as s]
            [clojure.string :as string]))

;; Shared by the two Pi command adapters. Completion is advisory and entirely
;; local for static flags/values. Repository hints use a separate lazy cache;
;; neither path reads recipe configuration, source content or invokes a model.
(def ^:private common-options
  {"--depth" "Evidence depth" "--repository" "Git origin filter"
   "--session" "Source-scoped session URN filter" "--language" "Linked-file language filter"})
(def ^:private read-options
  {"--history" "Include saved history" "--limit" "Select 1–20 runs"
   "--evidence" "Inspect a saved conclusion ID" "--cursor" "Continue a read page"})
(def ^:private selectors #{"--repository" "--session" "--language"})
(def ^:private values
  {"--depth" (sort core/depths)
   "--language" (sort (keys core/language-extensions))
   "--limit" (map str (range 1 21))})
(s/def ::prefix (s/and string? #(<= (count %) 8192)))
(s/def ::origin-option
  (s/and ::core/repository
         #(not (re-find #"[\s\u007f-\u009f\u2028-\u202e\u2066-\u2069]" %))))

(defn- valid-value? [flag value]
  (case flag
    "--depth" (contains? core/depths value)
    "--language" (s/valid? ::core/language value)
    "--limit" (and (re-matches #"[0-9]+" value) (<= 1 (js/Number value) 20))
    ;; Do not invent or look up free-form locators.
    (not (string/starts-with? value "--"))))

(defn- preceding-state [tokens options]
  (loop [remaining (if (and (seq tokens) (not (string/starts-with? (first tokens) "--"))) (rest tokens) tokens)
         used #{}]
    (if (empty? remaining)
      {:used used}
      (let [[flag value & more] remaining]
        (when (and (contains? options flag) (not (contains? used flag))
                   (not (and (selectors flag) (some selectors used))))
          (cond
            (= flag "--history") (recur (rest remaining) (conj used flag))
            (nil? value) {:used used :pending flag}
            (valid-value? flag value) (recur more (conj used flag))))))))

(defn- context [read? prefix]
  (when (s/valid? ::prefix prefix)
    (let [options (cond-> common-options read? (merge read-options))
          current (or (re-find #"\S+$" prefix) "")
          before (subs prefix 0 (- (count prefix) (count current)))]
      {:options options :current current :before before
       :state (preceding-state (re-seq #"\S+" before) options)})))

(defn arguments
  ([read? prefix] (arguments read? prefix []))
  ([read? prefix origins]
   (when-let [{:keys [options current before state]} (context read? prefix)]
     (let [candidates (when state
                        (if-let [pending (:pending state)]
                          (if (= pending "--repository") origins (get values pending))
                          (remove #(or (contains? (:used state) %)
                                       (and (selectors %) (some selectors (:used state)))) (sort (keys options)))))
           matches (filter #(string/starts-with? % current) candidates)]
       (when (seq matches)
         (clj->js (mapv (fn [candidate]
                         {:value (str before candidate) :label candidate
                          :description (get options (or (:pending state) candidate))}) matches)))))))

(defn- origin-option [value]
  (when (s/valid? (config/text-spec 4096) value)
    (let [normalized (or (repository/normalize-git-remote value)
                         (when (re-matches #"[^/:\s@?#]+/[^\s?#]+" value)
                           (repository/normalize-git-remote (str "https://" value))))
          https (when normalized (str "https://" normalized))
          ;; Preserve lookup identity for SCP paths whose Unicode bytes would
          ;; otherwise be URL-encoded by an HTTPS rewrite.
          option (when normalized
                   (if (= normalized (repository/normalize-git-remote https)) https
                       (let [[host path] (string/split normalized #"/" 2)]
                         (str "git@" host ":" path))))]
      (when (and (s/valid? ::origin-option option)
                 (= normalized (repository/normalize-git-remote option))) option))))

(defn repository-cache
  "One principal-bound cache shared by both commands. Graph loading never delays
   checkout suggestions beyond a 250 ms grace period: pending results can appear on a subsequent completion.
   Only origin hints are cached, not memory content or authorization decisions."
  [{:keys [get-cwd load-checkout! load-authorized! now-ms ttl-ms]
    :or {now-ms #(.now js/performance) ttl-ms 30000}}]
  (let [checkout (atom nil) authorized (atom nil) closed? (atom false)
        cached! (fn [state key loader]
                  (let [previous @state now (now-ms)]
                    (if (and (= key (:key previous))
                             (or (:pending? previous) (< (- now (:at previous)) ttl-ms)))
                      previous
                      (let [token (js/Object.) controller (js/AbortController.)
                            work (-> (js/Promise.resolve)
                                     (.then #(when-not @closed? (loader (.-signal controller))))
                                     (.then #(vec (distinct (keep origin-option %))))
                                     (.catch (fn [_] []))
                                     (.then (fn [origins]
                                              (when (and (not @closed?) (identical? token (:token @state)))
                                                (swap! state assoc :pending? false :value origins))
                                              origins)))
                            snapshot {:key key :at now :pending? true :value [] :token token
                                      :controller controller :promise work}]
                        (reset! state snapshot)
                        snapshot))))
        load! (fn []
                (let [cwd (get-cwd)]
                  [(cached! checkout cwd (fn [_] (-> (js/Promise.resolve)
                                                      (.then #(load-checkout! cwd))
                                                      (.then #(vector %)))))
                   (cached! authorized :principal load-authorized!)]))]
    {:complete! (fn [read? prefix]
                  (when-not @closed?
                    (if (= "--repository" (get-in (context read? prefix) [:state :pending]))
                      (let [[local graph] (load!)
                            delay (js/Promise.withResolvers)
                            timer (js/setTimeout #((.-resolve delay) nil) 250)]
                        (-> (js/Promise.all #js [(:promise local)
                                                (js/Promise.race #js [(:promise graph) (.-promise delay)])])
                            (.then (fn [result]
                                     (when-not @closed?
                                       (arguments read? prefix (distinct (concat (aget result 0) (:value @authorized)))))))
                            (.finally #(js/clearTimeout timer))))
                      (arguments read? prefix))))
     :warm! (fn [] (if @closed? (js/Promise.resolve nil)
                      (js/Promise.all (to-array (map :promise (load!))))))
     :close! (fn []
               (reset! closed? true)
               (doseq [state [checkout authorized]]
                 (when-let [controller (:controller @state)] (.abort controller))
                 (reset! state nil)))}))
