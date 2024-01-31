# Naskah Skripsi LaTeX

<details>
<summary>Template Information</summary>

Template Naskah Skripsi dengan typesetting LaTeX untuk JTETI Universitas Gadjah
Mada. Template ini merupakan hasil modifikasi dari versi pak Pekik Nurwantoro
(FMIPA UGM) dan mas Yohan (JTETI UGM 2008).

Diedit dan digunakan untuk keperluan SKRIPSI SARJANA ILMU KOMPUTER.

Diunggah oleh:  
Gregorius Andito Herjuno  
ILMU KOMPUTER 2013  
UNIVERSITAS NEGERI JAKARTA  
3145136218

Modifikasi lebih lanjut oleh:  
MOCHAMMAD HANIF RAMADHAN  
ILMU KOMPUTER 2019  
UNIVERSITAS NEGERI JAKARTA

</details>

## Pembuatan Dokumen

Pembuatan dokumen dapat dilakukan dengan menggunakan [tectonic](https://github.com/tectonic-typesetting/tectonic) yang lebih modern
ketimbang melakukan instalasi `TeXLive` yang memakan _storage_ cukup besar.
Referensi instalasi `tectonic` dapat dilihat pada link diatas.

Pada template ini, digunakan package `biber` untuk mengelola daftar pustaka. Pada
sistem operasi Linux, `biber` bisa didapatkan pada _repository_ distribusi
masing-masing dengan mudah. Sementara untuk Windows dan macOS, untuk menghindari
instalasi `perl` yang cukup merepotkan, dapat mengunduh file _executable binary_
yang tersedia di [Sourceforge](https://sourceforge.net/projects/biblatex-biber/files/).

Karena `tectonic` tidak dapat mendeteksi _LaTeX magic comments_ (yang
menunjukkan _root_ dokumen), maka hanya ada satu perintah yang dapat digunakan
untuk pembuatan file PDF.

```bash
tectonic template-skripsi.tex
```

## _Code Completion_

Untuk dukungan fitur _code completion_, penulis secara pribadi merekomendasikan
untuk menggunakan [TexLab](https://github.com/latex-lsp/texlab). Pengguna _Visual
Studio Code_ dapat menggunakan [extension TexLab](https://marketplace.visualstudio.com/items?itemName=efoerster.texlab).
Sementara untuk Vim/Neovim, diperlukan konfigurasi LSP secara manual.

## Konfigurasi Editor

<details>
<summary>Vim/Neovim</summary>

Untuk memberikan pengalaman penulisan yang lebih baik, disarankan untuk
membatasi jumlah karakter per baris. Pada Vim/Neovim, kalian dapat menggunakan
opsi `textwidth` dan `colorcolumn`. `textwidth` akan memberikan _linebreak_
secara otomatis jika sudah lebih dari karakter yang di set, sementara
`colorcolumn` memberikan garis vertikal sebagai penanda visual untuk batas
karakter.

Untuk konfigurasi, disarankan untuk membuat _autocommand_ khusus untuk file
`.tex`.

```vimscript
"Vimscript (Vim)
autocmd FileType latex setlocal textwidth=80 | setlocal colorcolumn=80
```

```lua
-- Lua (Neovim)
vim.api.nvim_create_autocmd("FileType", {
  pattern = {"latex"},
  callback = function()
    vim.cmd("set textwidth=80")
    vim.cmd("set colorcolumn=80")
  end
})
```

</details>

<details>
<summary>Visual Studio Code</summary>
</details>
