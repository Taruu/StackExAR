# StackExAR
**Stack Exchange archive reader**

This project is needed for convenient access to the [stackexchange.com](https://stackexchange.com/)  archive.
Using a simple API, you can index archives and retrieve data directly. The main goal of this project is to use it to train artificial intelligence.

![exmpl img](https://github.com/Taruu/StackExAR/blob/main/img/exp_img.png?raw=true)


The main goal was to be able to read the archive [stackoverflow.com](https://stackoverflow.com/).
The archive weighs 20 gigabytes and approximately more than 70 million records, since all data is recorded in a bzip2 archive this allows them to be read in stream mode

# Installation

```commandline
python3 -m venv venv
source venv/bin/activate 
pip3 install -r requirements.txt
```

Open `env_config` with any txt editor and set you configs

Example config:
```
count_workers = 32
archive_folder = "data/archives"
database_folder = "data/archives"
host = "0.0.0.0"
port = "8000"
```

# Usage

- use `/archive/list` to find all files in archive folder
- use `/archive/load` or `/archive/load/all` for preload archive files (It is worth understanding that large files require preliminary indexing)
- use `/indexing/process` or `/indexing/process/all` for index content in archive
- use `/archive/get/post` or `/archive/get/posts` for read posts


# TODO
- make faster reader for [stackoverflow.com](https://stackoverflow.com/)
- remade database worker class
- full text search?
- make a variation for static operation
