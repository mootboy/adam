(ns adam.replica.state-test
  (:require [adam.replica.state :as state]
            [cljs.test :refer [deftest is]]
            ["node:fs" :refer [mkdtempSync readFileSync rmSync statSync]]
            ["node:os" :refer [tmpdir]]
            ["node:path" :refer [join]]))

(def test-uuid "0199a11d-0fe1-7000-8000-000000000001")

(deftest global-state-is-created-once-with-owner-only-permissions
  (let [agent-dir (mkdtempSync (join (tmpdir) "adam-state-test-"))]
    (try
      (let [created (state/load-or-create! agent-dir (constantly test-uuid))
            loaded (state/load-or-create! agent-dir
                                          (fn [] (throw (js/Error. "must not replace state"))))
            config-path (join agent-dir "adam" "config.json")
            parsed (js->clj (js/JSON.parse (readFileSync config-path "utf8"))
                            :keywordize-keys true)]
        (is (= created loaded))
        (is (= {:version 1
                :user-uuid test-uuid
                :path config-path}
               created))
        (is (= {:version 1 :userUuid test-uuid} parsed))
        (when-not (= "win32" js/process.platform)
          (is (= 384 (bit-and 511 (.-mode (statSync config-path)))))))
      (finally
        (rmSync agent-dir #js {:recursive true :force true})))))

(deftest host-state-migrates-the-pi-identity-to-a-host-neutral-location
  (let [root (mkdtempSync (join (tmpdir) "adam-host-state-test-"))
        agent-dir (join root "pi-agent")
        config-home (join root "config")]
    (try
      (state/load-or-create! agent-dir (constantly test-uuid))
      (let [migrated (state/load-or-create-host!
                      {:config-home config-home
                       :legacy-agent-dir agent-dir
                       :uuid-fn #(throw (js/Error. "must preserve legacy identity"))})
            config-path (join config-home "adam" "config.json")]
        (is (= test-uuid (:user-uuid migrated)))
        (is (= config-path (:path migrated)))
        (is (= test-uuid
               (aget (js/JSON.parse (readFileSync config-path "utf8")) "userUuid"))))
      (finally
        (rmSync root #js {:recursive true :force true})))))

(deftest host-state-accepts-matching-canonical-and-pi-identities
  (let [root (mkdtempSync (join (tmpdir) "adam-host-state-match-test-"))
        agent-dir (join root "pi-agent")
        config-home (join root "config")]
    (try
      (state/load-or-create! agent-dir (constantly test-uuid))
      (state/load-or-create! config-home (constantly test-uuid))
      (is (= test-uuid
             (:user-uuid
              (state/load-or-create-host!
               {:config-home config-home :legacy-agent-dir agent-dir}))))
      (finally
        (rmSync root #js {:recursive true :force true})))))

(deftest host-state-refuses-conflicting-identities
  (let [root (mkdtempSync (join (tmpdir) "adam-host-state-conflict-test-"))
        agent-dir (join root "pi-agent")
        config-home (join root "config")]
    (try
      (state/load-or-create! agent-dir (constantly test-uuid))
      (state/load-or-create! config-home
                             (constantly "0199a11d-0fe1-7000-8000-000000000002"))
      (is (thrown-with-msg?
           js/Error
           #"conflicting adam user identities"
           (state/load-or-create-host!
            {:config-home config-home :legacy-agent-dir agent-dir})))
      (finally
        (rmSync root #js {:recursive true :force true})))))
