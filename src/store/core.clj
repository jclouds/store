(ns store.core
  (:import java.io.File))

(defn append
"takes a seq of two elements, updaing the first element with the second element, depending on their types.

concating/merging updates, not one-at-a-time-updates.

i.e. [[1 2] [3]] -> [1 2 3]

not [[12] 3] -> [1 2 3]
"
[xs]
  (let [root (first xs)]
    (cond
     (string? root) (apply str xs) 
     (map? root) (apply merge xs)
     (vector? root) (vec (apply concat xs))
     :else (apply concat xs))))

(defn try-default [default f & args]
  (try (apply f args)
    (catch java.lang.Exception _
      default)))

(defn mkdir [path] (.mkdir (File. path)))
