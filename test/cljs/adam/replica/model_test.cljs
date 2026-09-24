(ns adam.replica.model-test
  (:require [adam.replica.model :as model]
            [cljs.test :refer [deftest is]]))

(deftest builds-user-scoped-session-and-entry-records
  (let [summary {:pi-session-id "session-1"
                 :header-json "{\"type\":\"session\"}"
                 :cwd "/repo"
                 :version 3
                 :created-at "2026-01-01T00:00:00.000Z"
                 :parent-session "/sessions/parent.jsonl"
                 :name "work"
                 :last-entry-id "entry-1"}
        parent {:pi-session-id "parent-1"
                :session-id "urn:adam:session:user-1:parent-1"}
        session (model/session-from-summary
                 summary
                 {:user-uuid "user-1"
                  :source-file "/sessions/child.jsonl"
                  :parent parent})
        entry (model/entry-from-scan
               "user-1"
               "session-1"
               {:entry-id "entry-1"
                :type "message"
                :role "assistant"
                :parent-id nil
                :timestamp "2026-01-01T00:00:01.000Z"
                :ordinal 0
                :raw-json "{\"id\":\"entry-1\"}"
                :payload-hash "payload-hash"
                :payload-bytes 22})]
    (is (= "urn:adam:session:user-1:session-1" (:id session)))
    (is (= "urn:adam:user:user-1" (:user-id session)))
    (is (= "entry-1" (:current-leaf-id session)))
    (is (= "urn:adam:session:user-1:parent-1" (:parent-session-id session)))
    (is (= "adam-v1" (:writer-version session)))
    (is (= {:id "urn:adam:entry:user-1:session-1:entry-1"
            :entry-id "entry-1"
            :type "message"
            :role "assistant"
            :parent-id nil
            :timestamp "2026-01-01T00:00:01.000Z"
            :ordinal 0
            :raw-json "{\"id\":\"entry-1\"}"
            :payload-hash "payload-hash"
            :payload-bytes 22}
           entry))))

(deftest explicit-current-leaf-overrides-the-final-entry
  (is (= "selected"
         (:current-leaf-id
          (model/session-from-summary
           {:pi-session-id "session-1"
            :header-json "{}"
            :last-entry-id "last"}
           {:user-uuid "user-1"
            :source-file "/session.jsonl"
            :current-leaf-id "selected"})))))
