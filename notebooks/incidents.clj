^{:nextjournal.clerk/visibility {:code :hide :result :hide}}
(ns incidents
  {:nextjournal.clerk/visibility {:code :fold :result :show}}
  (:require
   [nextjournal.clerk :as clerk]
   [nextjournal.clerk.viewer :as v]
   [xtdb.api :as xt]
   [incidents.core :refer :all]
   [clojure.string :as str]))

^{:nextjournal.clerk/visibility {:code :hide :result :hide}}
^::clerk/no-cache
(def incidents (with-open [node (start-xtdb! "data")]
                 (->> node
                      get-all-facts
                      (sort-by :duration-minutes)
                      reverse)))

(clerk/plotly
  (let [fact-count (->>
                     incidents
                     (group-by (fn [i] (format-date (:start-date i))))
                     (map
                       (fn [[start-date v]]
                         [start-date (count v)]))
                     (sort-by first))]
    {:data [{:x (map first fact-count)
             :y (map second fact-count)
             :type "bar"}]
     :layout {:title "Incident Count by Date"}}))

(clerk/plotly
  (let [fact-count (->>
                     incidents
                     (group-by (fn [i] (format-date-part "HH" (:start-date i))))
                     (map
                       (fn [[start-date v]]
                         [start-date (count v)]))
                     (sort-by first))]
    {:data [{:x (map first fact-count)
             :y (map second fact-count)
             :type "bar"}]
     :layout {:title "Incident Count by Hour"}}))

(clerk/plotly
  (let [fact-count (->>
                     incidents
                     (group-by (fn [i] (format-date-part "e E" (:start-date i))))
                     (map
                       (fn [[start-date v]]
                         [start-date (count v)]))
                     (sort-by first))]
    {:data [{:x (map first fact-count)
             :y (map second fact-count)
             :type "bar"}]
     :layout {:title "Incident Count by Day"}}))

(clerk/plotly
 (let [muni-count (->>
                   incidents
                   (group-by :municipality)
                   (map
                    (fn [[municipality v]]
                      [municipality (count v)]))
                   (sort-by last)
                   reverse)]
   {:data [{:x (map first muni-count)
            :y (map second muni-count)
            :type "bar"}]
    :layout {:title "Incident Count by Municipality"}}))

(clerk/table
  {:head ["Municipality" "Incidents"]
   :rows (->>
           incidents
           (group-by :municipality)
           (map
             (fn [[municipality v]]
               [municipality (count v)]))
           (sort-by last)
           reverse)})

(clerk/plotly
  (let [muni-count (->>
                     incidents
                     (group-by (fn [i] (cons (:municipality i) (sort (:streets i)))))
                     (map
                       (fn [[[municipality & streets] v]]
                         [(str municipality ": " (str/join " & " streets)) (count v)]))
                     (sort-by last)
                     reverse
                     (take 40))]
    {:data [{:x (map first muni-count)
             :y (map second muni-count)
             :type "bar"}]
     :layout {:title "Incident Count by Intersection"}}))

(clerk/table
 {:head ["Municipality" "Intersection" "Incidents"]
  :rows (->>
         incidents
         (group-by (fn [i] (cons (:municipality i) (sort (:streets i)))))
         (map
          (fn [[[municipality & streets] v]]
            [municipality
             (clerk/html
               [:a
                {:target "_blank" :href (str (map-link municipality streets))}
                (str/join " & " streets)])
             (count v)]))
         (sort-by last)
         reverse)})
