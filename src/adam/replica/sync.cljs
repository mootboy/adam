(ns adam.replica.sync)

(def default-batch-bytes (* 4 1024 1024))

(defn checkpoint-for-entry [entry]
  {:complete-through-ordinal (:ordinal entry)
   :complete-through-byte-offset (:next-byte-offset entry)
   :committed-prefix-hash (:prefix-hash entry)})

(defn- initial-checkpoint [summary]
  {:complete-through-ordinal -1
   :complete-through-byte-offset (:header-next-byte-offset summary)
   :committed-prefix-hash (:header-prefix-hash summary)})

(defn- prefix-conflict [expected actual]
  {:status :conflict
   :reason :prefix-mismatch
   :expected-hash expected
   :actual-hash actual})

(defn- range-conflict [expected actual]
  {:status :conflict
   :reason :checkpoint-out-of-range
   :expected-offset expected
   :actual-offset actual})

(defn- verify-checkpoint [summary entries checkpoint]
  (let [ordinal (:complete-through-ordinal checkpoint)]
    (cond
      (= -1 ordinal)
      (cond
        (not= (:committed-prefix-hash checkpoint)
              (:header-prefix-hash summary))
        (prefix-conflict (:committed-prefix-hash checkpoint)
                         (:header-prefix-hash summary))

        (not= (:complete-through-byte-offset checkpoint)
              (:header-next-byte-offset summary))
        (range-conflict (:complete-through-byte-offset checkpoint)
                        (:header-next-byte-offset summary))

        :else nil)

      (or (not (integer? ordinal)) (neg? ordinal) (>= ordinal (:entry-count summary)))
      (range-conflict (:complete-through-byte-offset checkpoint) (:log-bytes summary))

      :else
      (if-let [entry (some #(when (= ordinal (:ordinal %)) %) entries)]
        (cond
          (not= (:committed-prefix-hash checkpoint) (:prefix-hash entry))
          (prefix-conflict (:committed-prefix-hash checkpoint) (:prefix-hash entry))

          (not= (:complete-through-byte-offset checkpoint) (:next-byte-offset entry))
          (range-conflict (:complete-through-byte-offset checkpoint)
                          (:next-byte-offset entry))

          :else nil)
        (range-conflict (:complete-through-byte-offset checkpoint) (:log-bytes summary))))))

(defn- make-batch [entries]
  {:entries entries
   :checkpoint (checkpoint-for-entry (peek entries))})

(defn- batch-entries [entries batch-limit]
  (loop [remaining entries
         current []
         current-bytes 0
         batches []]
    (if-let [entry (first remaining)]
      (let [entry-bytes (:payload-bytes entry)]
        (if (and (seq current) (> (+ current-bytes entry-bytes) batch-limit))
          (recur remaining [] 0 (conj batches (make-batch current)))
          (recur (next remaining)
                 (conj current entry)
                 (+ current-bytes entry-bytes)
                 batches)))
      (cond-> batches
        (seq current) (conj (make-batch current))))))

(defn plan-sync
  ([summary entries checkpoint]
   (plan-sync summary entries checkpoint default-batch-bytes))
  ([summary entries checkpoint batch-limit]
   (when-not (and (integer? batch-limit) (pos? batch-limit))
     (throw (js/Error. "batch limit must be a positive integer")))
   (if (and checkpoint
            (= (:log-hash checkpoint) (:log-hash summary))
            (= (:entry-count checkpoint) (:entry-count summary)))
     {:status :unchanged :batches []}
     (let [starting-checkpoint (or checkpoint (initial-checkpoint summary))]
       (if-let [conflict (verify-checkpoint summary entries starting-checkpoint)]
         conflict
         (let [pending (filterv #(> (:ordinal %)
                                    (:complete-through-ordinal starting-checkpoint))
                                entries)
               batches (batch-entries pending batch-limit)
               final-checkpoint (if-let [last-entry (peek entries)]
                                  (checkpoint-for-entry last-entry)
                                  (initial-checkpoint summary))]
           {:status :pending
            :batches batches
            :completion (assoc final-checkpoint
                               :entry-count (:entry-count summary)
                               :log-hash (:log-hash summary))}))))))
