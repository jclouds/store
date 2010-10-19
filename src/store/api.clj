(ns store.api
  (:use store.s3)
  (:use store.core)
  (:require [clomert :as v]))

;;TODO: can get rid of all these and ust partially apply try-default in the data-domain fn below.
;;wait until generalizing with Vold.
(defn put* [bucket s3 v k]
(try-default nil
  put-clj s3 bucket (str k) v))

(defn get* [bucket s3 k]
  (try-default nil
   get-clj s3 bucket (str k)))

(defn keys* [bucket s3]
  (try-default nil
   get-keys s3 bucket))

(defn update* [bucket s3 k]
  (try-default nil
   append-clj s3 bucket (str k)))

(defn delete* [bucket s3 k]
  (try-default nil
   delete-object s3 bucket (str k)))

(defn exists?* [bucket s3 k]
  (or
   (some #(= k (.getKey %))
	 (try-default nil
		 (comp seq objects)
		 s3 bucket (str k)))
   false))

;;TODO: when feeling frisky, extract this to some simple syntactic sugar for these object-like closures over state and magic for calling fns that use their closed over state.
(defn obj [s]
  (fn [op & args]
    (let [f (s op)]
      (apply f args))))

(defn mk-store [s3 & [m]]
  (let [m (or m identity)]
    (obj {:put (fn [b v k] (put* (m b) s3 v k))
	  :keys (fn [b] (keys* (m b) s3))
	  :get (fn [b k] (get* (m b) s3 k))
	  :update (fn [b k] (update* (m b) s3 k))
	  :delete (fn [b k] (delete* (m b) s3 k))
	  :exists? (fn [b k] (exists?* (m b) s3 k))})))

;;TODO: can't compose in this way becasue macro evaluates the map at macroexpand time.  change in clomert.
#_(defn mk-store-cache [config]
  (let [factory (v/make-socket-store-client-factory
                 (v/make-client-config config))
        m (java.util.concurrent.ConcurrentHashMap.)]
    (fn [client]
      (if-let [c (get m client)]
        c
        (let [c (v/make-store-client factory client)]
          (.put m client c)
          c)))))
       
(defn mk-vstore
  [stores]
  (obj {:put (fn [bucket k v]
	       (v/do-store
		(stores (str bucket))
		(:put k v)))
	:get (fn [bucket k]
	       (v/versioned-value (v/do-store
				   (stores (str bucket))
				   (:get k))))
	:update (fn [bucket k v]
		  (v/store-apply-update
		   (stores (str bucket))
		   (fn [client]
		     (let [ver (v/store-get client k)
			   val (v/versioned-value ver)]
		       (v/store-conditional-put client
						k
						(v/versioned-set-value! ver (append
									     v
									     val)))))))
	:delete (fn [bucket k]
		  (v/do-store
		   (stores (str bucket))
		   (:delete k)))}))
;;TODO: :exists? :keys

;;(mk-vstore (mk-store-cache {:bootstrap-urls "tcp://localhost:6666"}))