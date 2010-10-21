(ns store.api
  (:use store.s3)
  (:use store.core)
  (:require [clomert :as v]))

(defn obj [s]
  (fn [op & args]
    (let [f (s op)]
      (apply f args))))

(defn mk-store [s3 & m]
  (let [m (or m identity)]
    (obj {:put (fn [b v k]
		 (try-default nil put-clj s3 (m b) (str k) v))
	  :keys (fn [b]
		  (try-default nil
			       get-keys s3 (m b)))

	  :get (fn [b k]
		 (try-default nil
			      get-clj s3 (m b) (str k)))
	  :update (fn [b k]
		    (try-default nil
				 append-clj s3 (m b) (str k)))
	  :delete (fn [b k]
		    (try-default nil
				 delete-object s3 (m b) (str k)))

	  :exists? (fn [b k]
		     (or
		      (some #(= k (.getKey %))
			    (try-default nil
					 (comp seq objects)
					 s3 (m b) (str k)))
		      false))})))

(defn mk-store-cache [config]
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
									     [v
									     val])))))))
	:delete (fn [bucket k]
		  (v/do-store
		   (stores (str bucket))
		   (:delete k)))}))
;;TODO: :exists? :keys

;;(mk-vstore (mk-store-cache {:bootstrap-urls "tcp://localhost:6666"}))