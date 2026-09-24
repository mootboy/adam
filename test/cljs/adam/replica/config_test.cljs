(ns adam.replica.config-test
  (:require [adam.replica.config :as config]
            [cljs.test :refer [deftest is testing]]))

(deftest incomplete-configuration-disables-the-replica
  (testing "missing credentials do not prevent adam from loading"
    (is (= {:enabled? false
            :reason "ADAM_NEO4J_URI, ADAM_NEO4J_USERNAME, and ADAM_NEO4J_PASSWORD are required"}
           (config/resolve-config
            {"ADAM_NEO4J_URI" "bolt://localhost:7687"
             "ADAM_NEO4J_USERNAME" "neo4j"})))))

(deftest complete-configuration-is-normalized
  (is (= {:enabled? true
          :uri "bolt://localhost:7687"
          :username "neo4j"
          :password "secret"
          :database "neo4j"}
         (config/resolve-config
          {"ADAM_NEO4J_URI" "  bolt://localhost:7687  "
           "ADAM_NEO4J_USERNAME" " neo4j "
           "ADAM_NEO4J_PASSWORD" "secret"}))))

(deftest non-loopback-unencrypted-transport-is-rejected
  (is (= {:enabled? false
          :reason "unencrypted Neo4j transport is allowed only for loopback hosts"}
         (config/resolve-config
          {"ADAM_NEO4J_URI" "bolt://neo4j.example.com:7687"
           "ADAM_NEO4J_USERNAME" "neo4j"
           "ADAM_NEO4J_PASSWORD" "secret"}))))

(deftest invalid-uri-disables-the-replica-without-throwing
  (is (= {:enabled? false
          :reason "ADAM_NEO4J_URI must be a valid bolt or neo4j URI"}
         (config/resolve-config
          {"ADAM_NEO4J_URI" "not a URI"
           "ADAM_NEO4J_USERNAME" "neo4j"
           "ADAM_NEO4J_PASSWORD" "secret"}))))
