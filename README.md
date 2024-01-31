# Improvisasi Crawling pada Peta Web Menggunakan Algoritma Terdistribusi dengan Model Koordinasi Berbasis Socket Programming

Merupakan improvisasi crawling dari mesin pencari [Telusuri](https://github.com/lazuardyk/search-engine).


## Implementasi Kode
Terdapat 3 Kode utama, Tracker, Manager, dan Client.
- Tracker: Tracker akan memiliki public IP address, dikarenakan tracker memiliki
tugas sebagai jalur informasi antara perangkat yang terhubung.
- Manager: Manager akan memiliki public IP address, dikarenakan manager memiliki
tugas untuk mengatur perangkat terhubung yang nantinya akan memiliki public IP
address.
- Client: Klien dapat memiliki public atau private IP address. Client private IP bertugas untuk melakukan crawling. Client public IP bertugas sebagai pengelola client private IP.


*note: Untuk hasil yang baik dibutuhkan setidaknya 1 tracker, 1 manager, 1 client public IP, dan 2 client private IP.

## Alur Penggunaan
1. Pastikan komputer/server sudah terinstall Python 3.6+
2. Jalankan kode tracker
3. Jalankan kode manager
4. Jalankan kode client

(detail dapat dilihat pada README di folder masing - masing)
