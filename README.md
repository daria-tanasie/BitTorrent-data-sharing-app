Built a distributed application based on the BitTorrent protocol using MPI in C++.
The program is using a central tracker to get the initial state and files from the clients, and when the clients need, it will tell them where to find wanted files.
The tracker is finally used to close all the uploading threads and the cleints.
Implemented a multi-threaded architecture with three threads for download, upload, and traffic efficiency for the clients.
