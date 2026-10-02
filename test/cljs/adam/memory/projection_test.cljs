(ns adam.memory.projection-test
  (:require [adam.memory.projection :as projection]
            [cljs.test :refer-macros [deftest is]]))

(deftest retained-pi-records-rebuild-embedded-memory-without-local-files
  (let [entries [{:entry-id "tool" :stream-id "main"
                  :raw-json "{\"id\":\"tool\",\"parentId\":null,\"type\":\"message\"}"}
                 {:entry-id "memory" :stream-id "main"
                  :raw-json (js/JSON.stringify
                              (clj->js {:id "memory" :parentId "tool" :type "custom"
                                        :customType "om.observations.recorded"
                                        :data {:coversUpToId "tool" :observations [{:id "aaaaaaaaaaaa" :content "retained"
                                                               :timestamp "2026-01-01T00:00:00.000Z"
                                                               :relevance "high" :tokenCount 1
                                                               :sourceEntryIds ["tool"]}]}}))}]
        request {:user-uuid "user" :source-kind "pi" :source-session-id "session"
                 :current-leaf-id "memory" :entries entries :streams []
                 :entry-file-evidence [{:stream-id "main" :entry-id "tool" :file-id "file"}]}
        result (projection/from-retained request)]
    (is (= "retained" (get-in result [:observations 0 :content])))
    (is (= ["file"] (get-in result [:observations 0 :file-ids])))
    (is (= "pi-observational-memory" (get-in result [:observations 0 :producer])))
    (is (= [] (:observations (projection/from-retained (assoc request :current-leaf-id "tool")))))
    (is (empty? (get-in (projection/from-retained (assoc request :entry-file-evidence []))
                        [:observations 0 :file-ids])))))
