(ns incidents.core-test
  (:require [incidents.core :refer :all]
            [clojure.test :as t]))

(t/deftest my-test
  (t/testing "test"
    (t/is (= 1 1))))
