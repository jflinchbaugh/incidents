;; hello clerk
(ns hello
  (:require [nextjournal.clerk :as clerk]
            [xtdb.api :as xt]
            [incidents.core :refer :all]
            ))

;; Here's a visualization of unemployment in the US.
#_(clerk/vl {:width 700 :height 400 :data {:url "https://vega.github.io/vega-datasets/data/us-10m.json"
                                         :format {:type "topojson" :feature "counties"}}
           :transform [{:lookup "id" :from {:data {:url "https://vega.github.io/vega-datasets/data/unemployment.tsv"}
                                            :key "id" :fields ["rate"]}}]
           :projection {:type "albersUsa"} :mark "geoshape" :encoding {:color {:field "rate" :type "quantitative"}}})

(def incidents (with-open [node (start-xtdb! "data")]
                 (->> node
                   get-all-facts
                   (sort-by :duration-minutes)
                   reverse)))

(clerk/table incidents)

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
