(ns adam.replica.identity
  (:require [clojure.string :as string]
            ["node:crypto" :refer [createHash]]))

(defn- sha-256 [value]
  (-> (createHash "sha256")
      (.update value "utf8")
      (.digest "hex")))

(defn user-urn [user-uuid]
  (str "urn:adam:user:" user-uuid))

(def pi-source-kind "pi")
(def claude-source-kind "claude-code")

(defn session-urn
  ([user-uuid pi-session-id]
   (session-urn user-uuid pi-source-kind pi-session-id))
  ([user-uuid source-kind source-session-id]
   (str "urn:adam:session:" user-uuid ":" source-kind ":" source-session-id)))

(defn entry-urn
  ([user-uuid pi-session-id entry-id]
   (entry-urn user-uuid pi-source-kind pi-session-id entry-id))
  ([user-uuid source-kind source-session-id entry-id]
   (str "urn:adam:entry:" user-uuid ":" source-kind ":" source-session-id
        ":" entry-id)))

(defn observation-urn [user-uuid source-kind source-session-id memory-id]
  (str "urn:adam:observation:" user-uuid ":" source-kind ":" source-session-id
       ":" memory-id))

(defn reflection-urn [user-uuid source-kind source-session-id memory-id]
  (str "urn:adam:reflection:" user-uuid ":" source-kind ":" source-session-id
       ":" memory-id))

(defn git-email-identity [email]
  (when (string? email)
    (let [display-value (string/trim email)]
      (when-not (string/blank? display-value)
        (let [normalized-value (string/lower-case display-value)]
          {:id (str "urn:adam:identity:git-email:" (sha-256 normalized-value))
           :kind "git-email"
           :value display-value
           :normalized-value normalized-value
           :display-value display-value})))))
