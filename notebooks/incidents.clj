;; incident data

^{:nextjournal.clerk/visibility {:code :hide :result :hide}}
(ns incidents
  (:require [nextjournal.clerk :as clerk]
            [xtdb.api :as xt]
            [incidents.core :refer :all]
            [clojure.string :as str]))

;; all the incidents in a map
(def incidents (with-open [node (start-xtdb! "data")]
                 (->> node
                      get-all-facts
                      (sort-by :duration-minutes)
                      reverse)))

;; counts by intersection
(clerk/table
 (clerk/use-headers
  (cons ["Municipality" "Intersection" "Incidents"]
        (->>
         incidents
         (group-by (fn [i] (cons (:municipality i) (sort (:streets i)))))
         (map
           (fn [[[municipality & streets] v]]
             [municipality (str/join " & " streets) (count v)]))
         (sort-by last)
         reverse))))

;; counts by municipality
(clerk/table
  (clerk/use-headers
    (cons ["Municipality" "Incidents"]
      (->>
        incidents
        (group-by :municipality)
        (map
          (fn [[municipality v]]
            [municipality (count v)]))
        (sort-by last)
        reverse))))

(clerk/vl
 {:data {:values incidents}
  :mark {:type "point"
         :tooltip {:field :streets}}
  :encoding {:y {:field :incident-type
                 :type :nominal}
             :x {:field :municipality
                 :type :nominal
                 :sort "x"}}})
