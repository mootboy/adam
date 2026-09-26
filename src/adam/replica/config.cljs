(ns adam.replica.config
  (:require [clojure.string :as string]))

(def environment-keys
  ["ADAM_NEO4J_URI"
   "ADAM_NEO4J_USERNAME"
   "ADAM_NEO4J_PASSWORD"
   "ADAM_NEO4J_DATABASE"])

(def ^:private required-keys
  ["ADAM_NEO4J_URI" "ADAM_NEO4J_USERNAME" "ADAM_NEO4J_PASSWORD"])

(def ^:private missing-config-reason
  "ADAM_NEO4J_URI, ADAM_NEO4J_USERNAME, and ADAM_NEO4J_PASSWORD are required")

(defn- present? [value]
  (and (string? value) (not (string/blank? value))))

(def ^:private encrypted-schemes
  #{"bolt+s" "bolt+ssc" "neo4j+s" "neo4j+ssc"})

(def ^:private supported-schemes
  (into encrypted-schemes ["bolt" "neo4j"]))

(defn- loopback-host? [host]
  (let [normalized (string/lower-case host)]
    (or (= normalized "localhost")
        (= normalized "::1")
        (= normalized "[::1]")
        (string/starts-with? normalized "127."))))

(defn- uri-error [uri]
  (try
    (let [url (js/URL. uri)
          scheme (string/replace (.-protocol url) #":$" "")]
      (cond
        (not (contains? supported-schemes scheme))
        "ADAM_NEO4J_URI must be a valid bolt or neo4j URI"

        (and (not (contains? encrypted-schemes scheme))
             (not (loopback-host? (.-hostname url))))
        "unencrypted Neo4j transport is allowed only for loopback hosts"

        :else nil))
    (catch :default _
      "ADAM_NEO4J_URI must be a valid bolt or neo4j URI")))

(defn process-environment []
  (into {}
        (map (fn [key] [key (aget js/process.env key)]))
        environment-keys))

(defn resolve-config [environment]
  (if-not (every? #(present? (get environment %)) required-keys)
    {:enabled? false
     :reason missing-config-reason}
    (let [config {:enabled? true
                  :uri (string/trim (get environment "ADAM_NEO4J_URI"))
                  :username (string/trim (get environment "ADAM_NEO4J_USERNAME"))
                  :password (get environment "ADAM_NEO4J_PASSWORD")
                  :database (let [database (get environment "ADAM_NEO4J_DATABASE")]
                              (if (present? database) (string/trim database) "neo4j"))}]
      (if-let [reason (uri-error (:uri config))]
        {:enabled? false
         :reason reason}
        config))))

(defn resolve-process-config []
  (resolve-config (process-environment)))
