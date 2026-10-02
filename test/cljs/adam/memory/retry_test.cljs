(ns adam.memory.retry-test
  (:require [adam.memory.retry :as retry]
            [cljs.test :refer [deftest is]]))

(deftest missing-source-wait-configuration-is-bounded-and-explicit
  (is (= 600000 (retry/resolve-source-wait-ms nil)))
  (is (= 1000 (retry/resolve-source-wait-ms "1000")))
  (is (= 86400000 (retry/resolve-source-wait-ms "86400000")))
  (doseq [value ["" "0" "999" "86400001" "-1" "1.5" "1e6" " 600000 " "off"]]
    (is (thrown? js/Error (retry/resolve-source-wait-ms value)))))

(deftest notification-age-uses-local-mtime-regardless-of-producer-clock
  (doseq [queued-at ["1970-01-01T00:00:00.000Z" "1970-01-01T00:00:01.500Z"
                    "2099-10-02T00:00:00.000Z"]]
    (let [notification {:queued-at queued-at :file-mtime-ms 1000}]
      (is (= 999 (retry/notification-age-ms notification 1999)))
      (is (= 1000 (retry/notification-age-ms notification 2000)))
      (is (= 0 (retry/notification-age-ms notification 999))))))

(deftest queued-at-is-only-a-fallback-when-local-mtime-is-unavailable
  (doseq [mtime [nil js/NaN js/Infinity "1000"]]
    (is (= 1000 (retry/notification-age-ms
                  {:queued-at "1970-01-01T00:00:01.000Z" :file-mtime-ms mtime} 2000))))
  (is (= 2000 (retry/notification-age-ms
                {:queued-at "1970-01-01T00:00:01.000Z" :file-mtime-ms 0} 2000))))
