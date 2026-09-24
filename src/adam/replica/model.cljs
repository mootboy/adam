(ns adam.replica.model
  (:require [adam.replica.identity :as identity]))

(def writer-version "adam-v1")

(defn session-from-summary
  [summary {:keys [user-uuid source-file current-leaf-id parent identity]
            :as options}]
  (let [pi-session-id (:pi-session-id summary)
        selected-leaf (if (contains? options :current-leaf-id)
                        current-leaf-id
                        (:last-entry-id summary))]
    (cond-> {:id (identity/session-urn user-uuid pi-session-id)
             :user-id (identity/user-urn user-uuid)
             :pi-session-id pi-session-id
             :header-json (:header-json summary)
             :current-leaf-id selected-leaf
             :source-file source-file
             :writer-version writer-version}
      (:cwd summary) (assoc :cwd (:cwd summary))
      (some? (:version summary)) (assoc :version (:version summary))
      (:created-at summary) (assoc :created-at (:created-at summary))
      (:parent-session summary) (assoc :parent-session (:parent-session summary))
      (:name summary) (assoc :name (:name summary))
      parent (assoc :parent-pi-session-id (:pi-session-id parent)
                    :parent-session-id (:session-id parent))
      identity (assoc :identity identity))))

(defn entry-from-scan [user-uuid pi-session-id scanned-entry]
  (cond-> {:id (identity/entry-urn user-uuid pi-session-id (:entry-id scanned-entry))
           :entry-id (:entry-id scanned-entry)
           :type (:type scanned-entry)
           :parent-id (:parent-id scanned-entry)
           :ordinal (:ordinal scanned-entry)
           :raw-json (:raw-json scanned-entry)
           :payload-hash (:payload-hash scanned-entry)
           :payload-bytes (:payload-bytes scanned-entry)}
    (:role scanned-entry) (assoc :role (:role scanned-entry))
    (:timestamp scanned-entry) (assoc :timestamp (:timestamp scanned-entry))
    (some? (:parent-id scanned-entry))
    (assoc :parent-urn
           (identity/entry-urn user-uuid pi-session-id (:parent-id scanned-entry)))))
