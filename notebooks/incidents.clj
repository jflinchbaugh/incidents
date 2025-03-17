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
(def incidents (with-open [node (start-xtdb!)]
                 (->> node
                      get-all-facts
                      (sort-by :duration-minutes)
                      reverse)))

(clerk/table
 {::clerk/page-size 100}
 {:head ["Date" "Duration" "Title" "Municipality" "Intersection"]
  :rows (->> incidents
             (sort-by :start-date)
             reverse
             (map (fn [i]
                    [(format-date-time (:start-date i))
                     (or (:duration-minutes i) "-")
                     (:title i)
                     (:municipality i)
                     (clerk/html [:a
                                  {:target "_blank"
                                   :href (str (map-link (:municipality i) (:streets i)))}
                                  (str/join " & " (:streets i))])])))})

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
    :layout {:title "Incident Count by Date"}
    :config {}}))

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
    :layout {:title "Incident Count by Hour"}
    :config {}}))

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
    :layout {:title "Incident Count by Day"}
    :config {}}))

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
    :layout {:title "Incident Count by Municipality"}
    :config {}}))

(clerk/table
 {::clerk/page-size 250}
 {:head ["Municipality" "Count"]
  :rows (->>
         incidents
         (group-by :municipality)
         (map
          (fn [[municipality v]]
            [(clerk/html [:a
                          {:target "_blank" :href (str (map-link municipality nil))}
                          municipality])
             (count v)]))
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
    :layout {:title "Incident Count by Intersection"}
    :config {}}))

(clerk/table
  {::clerk/page-size 250}
  {:head ["Municipality" "Intersection" "Incidents"]
   :rows (->>
           incidents
           (group-by (fn [i] (cons (:municipality i) (sort (:streets i)))))
           (map
             (fn [[[municipality & streets] v]]
               [municipality
                (clerk/html [:a
                             {:target "_blank" :href (str (map-link municipality streets))}
                             (str/join " & " streets)])
                (count v)]))
           (sort-by last)
           reverse)})

(clerk/plotly
  (let [title-count (->>
                      incidents
                      (group-by :title)
                      (map
                        (fn [[title v]]
                          [title (count v)]))
                      (sort-by (juxt last first))
                      reverse)
        other ["Other" (->> title-count (drop 10) (map second) (reduce +))]
        title-count-show (concat (take 10 title-count) [other])
        ]
    {:data [{:labels (map first title-count-show)
             :values (map second title-count-show)
             :type "pie"
             :sort false}]
     :layout {:title "Incident Count by Title"}
     :config {}}))

(clerk/table
  {::clerk/page-size 250}
  {:head ["Title" "Incidents"]
   :rows (->>
           incidents
           (group-by :title)
           (map
             (fn [[title v]]
               [title (count v)]))
           (sort-by (juxt last first))
           reverse)})
