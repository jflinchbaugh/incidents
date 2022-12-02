;; incident data

^{:nextjournal.clerk/visibility {:code :hide :result :hide}}
(ns incidents
  (:require [nextjournal.clerk :as clerk]
            [xtdb.api :as xt]
            [incidents.core :refer :all]
            ))

;; all the incidents in a map
^{:nextjournal.clerk/visibility {:code :fold}}
(def incidents (with-open [node (start-xtdb! "data")]
                 (->> node
                   get-all-facts
                   (sort-by :duration-minutes)
                   reverse)))

;; all incidents in a table
^{:nextjournal.clerk/visibility {:code :fold}}
(clerk/table incidents)

^{:nextjournal.clerk/visibility {:code :fold}}
(clerk/vl
  {:data {:values incidents}
   :width 600
   :height 1600
   :mark {:type "point"
          :tooltip {:field :streets}}
   :encoding {:x {:field :incident-type
                  :type :nominal}
              :y {:field :duration-minutes
                  :type :quantitative
                  :sort "x"}}})
