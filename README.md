# Tugas 7
      Nama : Annisa Fitri Yuliandra
      Kelas : TI 3B
      NIM : 2041720123
      
Berikut adalah penjelasan fungsi kode ml-recommender.scala :

import org.apache.spark.ml.recommendation.ALS: Mengimpor kelas ALS (Alternating Least Squares) dari pustaka Spark ML yang digunakan untuk membangun model rekomendasi.

case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long): Mendefinisikan sebuah kasus kelas Rating yang memiliki properti userId (ID pengguna), movieId (ID film), rating (nilai peringkat), dan timestamp (waktu peringkat).

def parseRating(str: String): Rating: Mendefinisikan sebuah fungsi bernama parseRating yang mengambil string sebagai argumen dan mengembalikan objek Rating. Fungsi ini memecah string menjadi empat bagian dengan pemisah "::" dan mengonversi nilai-nilai string menjadi tipe data yang sesuai.

parseRating("1::1193::5::978300760"): Melakukan pengujian pada fungsi parseRating dengan memberikan string "1::1193::5::978300760" dan mengembalikan objek Rating dengan nilai-nilai yang sesuai.

var raw = sc.textFile("/data/ml-1m/ratings.dat"): Membaca file "ratings.dat" dan menyimpannya dalam variabel raw sebagai RDD (Resilient Distributed Dataset), yaitu kumpulan data terdistribusi yang dapat diproses secara paralel.

raw.take(1): Mengambil satu baris pertama dari RDD raw untuk memeriksa apakah file berhasil dibaca dengan benar.

val ratings = raw.map(parseRating).toDF(): Menerapkan fungsi parseRating pada setiap elemen RDD raw menggunakan metode map untuk mengonversi setiap baris string menjadi objek Rating. Kemudian, mengubah RDD menjadi DataFrame menggunakan metode toDF() dan menyimpannya dalam variabel ratings.

ratings.show(5): Menampilkan lima baris pertama dari DataFrame ratings untuk memeriksa apakah data berhasil diubah dengan benar.

val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2)): Membagi DataFrame ratings menjadi dua bagian, yaitu training set dan test set dengan rasio 0,8:0,2. Training set digunakan untuk membangun model rekomendasi, sedangkan test set digunakan untuk menguji performa model.

val als = new ALS().setMaxIter(5).setRegParam(0.01).setUserCol("userId").setItemCol("movieId").setRatingCol("rating"): Membuat objek ALS dengan mengatur parameter seperti jumlah iterasi (setMaxIter), parameter regulasi (setRegParam), serta kolom-kolom yang digunakan untuk userID, movieID, dan rating.

val model = als.fit(training): Membangun model rekomendasi dengan menggunakan metode fit() pada objek ALS dan data training.

model.save("mymodel"): Menyimpan model rekomendasi yang telah dibangun ke dalam direktori "mymodel".

val predictions = model.transform(test): Menghasilkan prediksi nilai peringkat untuk data test dengan menggunakan metode transform() pada model.

predictions.map(r => r(2).asInstanceOf[Float] - r(4).asInstanceOf[Float]): Menghitung selisih antara nilai peringkat aktual (kolom 2) dan nilai peringkat prediksi (kolom 4) pada setiap baris prediksi.

.map(x => x*x): Mengkuadratkan selisih pada setiap baris prediksi.

.filter(!_.isNaN): Menghapus nilai yang bukan angka (NaN) dari hasil perhitungan kuadrat.

.reduce(_ + _): Menjumlahkan semua nilai kuadrat yang dihasilkan sebelumnya.

predictions.take(10): Mengambil sepuluh baris pertama dari DataFrame predictions untuk melihat hasil prediksi.

predictions.write.format("com.databricks.spark.csv").save("ml-predictions.csv"): Menyimpan DataFrame predictions dalam format CSV ke dalam direktori "ml-predictions.csv".
