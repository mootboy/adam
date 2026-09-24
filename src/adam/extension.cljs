(ns adam.extension)

(defn start [pi]
  (.call
   (aget pi "registerCommand")
   pi
   "adam:status"
   #js {:description "Show adam status"
        :handler (fn [_args ctx]
                   (let [ui (aget ctx "ui")]
                     (.call (aget ui "notify") ui "adam is running" "info")))})
  nil)
