(ns adam.memory.retry-test
  (:require [adam.memory.retry :as retry]
            [cljs.test :refer [deftest is]]))

(deftest missing-source-wait-configuration-is-bounded-and-explicit
  (is (= 600000 (retry/resolve-source-wait-ms nil)))
  (is (= 1000 (retry/resolve-source-wait-ms "1000")))
  (is (= 86400000 (retry/resolve-source-wait-ms "86400000")))
  (doseq [value ["" "0" "999" "86400001" "-1" "1.5" "1e6" " 600000 " "off"]]
    (is (thrown? js/Error (retry/resolve-source-wait-ms value)))))

(deftest queued-age-is-restart-stable-and-does-not-trust-a-future-wire-clock
  (let [notification {:queued-at "2026-10-02T00:00:00.000Z" :file-mtime-ms 1000}
        future-notification (assoc notification :queued-at "2099-10-02T00:00:00.000Z")]
    (is (= 999 (retry/notification-age-ms notification 1999)))
    (is (= 1000 (retry/notification-age-ms future-notification 2000)))
    (is (= 0 (retry/notification-age-ms notification 999)))))
