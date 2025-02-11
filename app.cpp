#include <mpi.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <bits/stdc++.h>
#include <string.h>
#include <iostream>
#include <fstream>
#include <unordered_map>

#define TRACKER_RANK 0
#define MAX_FILES 10
#define MAX_FILENAME 15
#define HASH_SIZE 32
#define MAX_CHUNKS 100

using namespace std;

struct client_struct {
    int id;
    vector<string> owned_files;
    unordered_map<string, vector<string>> owned_files_seg;
    vector<string> wanted_files;
    unordered_map<string, vector<string>> wanted_files_seg;
    int nr_clients = 0;
    pthread_mutex_t mutex;
};

struct tracker_struct {
    unordered_map<string, vector<string>> files;
    unordered_map<string, vector<int>> owners;
};

MPI_Status status;

void request_swarms(client_struct *client, unordered_map<string, vector<int>> &owners,
        unordered_map<string, vector<string>> &remained_seg, string req_file) {

    char file[MAX_FILENAME];
    strcpy(file, (req_file).c_str());
    file[req_file.length()] = '\0';
    MPI_Send(file, MAX_FILENAME, MPI_CHAR, 0, 2, MPI_COMM_WORLD);
    int nr_ids = 0;
    MPI_Recv(&nr_ids, 1, MPI_INT, 0, 2, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    for (int j = 0; j < nr_ids; j++) {
        int id;
        MPI_Recv(&id, 1, MPI_INT, 0, 2, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        owners.at(req_file).push_back(id);
    }

    int nr_seg = 0;
    MPI_Recv(&nr_seg, 1, MPI_INT, 0, 2, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    for (int j = 0; j < nr_seg; j++) {
        char seg_name[HASH_SIZE];
        MPI_Recv(seg_name, HASH_SIZE, MPI_CHAR, 0, 2, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        seg_name[HASH_SIZE] = '\0';
        remained_seg.at(req_file).push_back(string(seg_name));
    }
}

void request_segments(client_struct *client, unordered_map<string, vector<int>> &owners,
    string file, unordered_map<string, vector<string>> remained_seg, int &cnt_seg) {

    for (auto owner = owners.at(file).begin(); owner < owners.at(file).end(); owner++) {

        if (*owner == client->id) {
            continue;
        }

        char req[4];
        strcpy(req, "REQ");
        req[4] ='\0';
        MPI_Send(req, 4, MPI_CHAR, *owner, 4, MPI_COMM_WORLD);

        char req_file[MAX_FILENAME];
        strncpy(req_file, file.c_str(), file.length());
        req_file[file.length()] = '\0';
        MPI_Send(req_file, MAX_FILENAME, MPI_CHAR, *owner, 4, MPI_COMM_WORLD);
        MPI_Send(&cnt_seg, 1, MPI_INT, *owner, 4, MPI_COMM_WORLD);

        char resp[4];
        MPI_Recv(resp, 4, MPI_CHAR, *owner, 9, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        
        if (strcmp(resp, "OK")) {
            long unsigned int i;
            for (i = cnt_seg; i < remained_seg.at(file).size(); i++) {
                if ((int)(i) > cnt_seg + 10) {
                    break;
                }

                char filename[MAX_FILENAME];
                strncpy(filename, file.c_str(), file.length());
                filename[file.length()] = '\0';

                MPI_Send(filename, MAX_FILENAME, MPI_CHAR, *owner, 5, MPI_COMM_WORLD);
                char segment[32];
                strncpy(segment, remained_seg.at(file)[i].c_str(), 32);
                MPI_Send(&cnt_seg, 1, MPI_INT, *owner, 5, MPI_COMM_WORLD);
                MPI_Recv(segment, HASH_SIZE, MPI_CHAR, *owner, 2, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                string str_seg = string(segment);
                client->wanted_files_seg.at(file).push_back(str_seg);
            }

            char done[5];
            strcpy(done, "done");
            MPI_Send(done, MAX_FILENAME, MPI_CHAR, *owner, 5, MPI_COMM_WORLD);
            cnt_seg = i;
            break;
        }
    }
}

void update_tracker(client_struct *client, string file,
        unordered_map<string, vector<int>> &owners) {
    char filename[MAX_FILENAME];
    strcpy(filename, file.c_str());
    MPI_Send(filename, MAX_FILENAME, MPI_CHAR, 0, 3, MPI_COMM_WORLD);

    int nr_ids;
    MPI_Recv(&nr_ids, 1, MPI_INT, 0, 3, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    for (int i = 0; i < nr_ids; i++) {
        int id;
        MPI_Recv(&id, 1, MPI_INT, 0, 3, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        int ok = 1;
        for (long unsigned int j = 0; j < owners.at(file).size(); j++) {
            if (owners.at(file)[j] == id) {
                ok = 0;
            }
        }
        
        if (ok) {
            owners.at(file).push_back(id);
        }
    }
}

void write_file(client_struct *client, string file, vector<string> segments) {
    string out_file = "client" + to_string(client->id) + "_" + file;
    ofstream out(out_file);
    for (long unsigned int i = 0; i < segments.size(); i++) {
        if (i == segments.size() - 1) {
            out << segments[i];
        } else {
            out << segments[i] << endl;
        }
    }

    out.close();
}

void *download_thread_func(void *arg)
{
    client_struct *client = (client_struct*) arg;
    unordered_map<string, vector<int>> owners;
    unordered_map<string, vector<string>> remained_seg;

    for (long unsigned int i = 0; i < client->wanted_files.size(); i++) {
        owners.emplace(client->wanted_files[i], vector<int>());
        remained_seg.emplace(client->wanted_files[i], vector<string>());
    }

    char file_done[7], all_done[7];
    strcpy(file_done, "F_DONE");
    strcpy(all_done, "A_DONE");

    for (long unsigned int f = 0; f < client->wanted_files.size(); f++) {
        request_swarms(client, owners, remained_seg, client->wanted_files[f]);
        int cnt_seg = 0;
        while (cnt_seg < (int)(remained_seg.at(client->wanted_files[f]).size())) {
            request_segments(client, owners, client->wanted_files[f], remained_seg, cnt_seg);
            update_tracker(client, client->wanted_files[f], owners);
        }
        MPI_Send(file_done, 7, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
        write_file(client, client->wanted_files[f], remained_seg.at(client->wanted_files[f]));
    }

    MPI_Send(all_done, 7, MPI_CHAR, 0, 1, MPI_COMM_WORLD);

    return NULL;
}

void *upload_thread_func(void *arg)
{
    client_struct *client = (client_struct*) arg;
    int cnt_seg;
    MPI_Status local_status;
    string str_file;

    while (1) {
        MPI_Probe(MPI_ANY_SOURCE, 5, MPI_COMM_WORLD, &local_status);

        if (local_status.MPI_SOURCE != 0) {
            char filename[MAX_FILENAME];
            MPI_Recv(filename, MAX_FILENAME, MPI_CHAR, local_status.MPI_SOURCE, 5, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            
            if (!strncmp(filename, "done", 5)) {
                pthread_mutex_lock(&client->mutex);
                client->nr_clients--;
                pthread_mutex_unlock(&client->mutex);
            } else {
                str_file = string(filename);
            
                MPI_Recv(&cnt_seg, 1, MPI_INT, local_status.MPI_SOURCE, 5, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                
                if (client->owned_files_seg.find(str_file) == client->owned_files_seg.end()) {
                    char segment[32];
                    string str_seg = client->wanted_files_seg.at(str_file)[cnt_seg];
                    strcpy(segment, str_seg.c_str());
                    MPI_Send(segment, HASH_SIZE, MPI_CHAR, local_status.MPI_SOURCE, 2, MPI_COMM_WORLD);
                } else {
                    char segment[32];
                    string str_seg = client->owned_files_seg.at(str_file)[cnt_seg];
                    strcpy(segment, str_seg.c_str());
                    MPI_Send(segment, HASH_SIZE, MPI_CHAR, local_status.MPI_SOURCE, 2, MPI_COMM_WORLD);
                }
            }
            
        } else {
            char done[5];
            MPI_Recv(done, 5, MPI_CHAR, 0, 5, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            MPI_Send(done, 5, MPI_CHAR, client->id, 4, MPI_COMM_WORLD);
            break;
        }
    }
    return NULL;
}

void *traffic_thread_func(void *arg)
{
    client_struct *client = (client_struct*) arg;
    MPI_Status local_status;
    while (1) {
        MPI_Probe(MPI_ANY_SOURCE, 4, MPI_COMM_WORLD, &local_status);
        
        if (local_status.MPI_SOURCE == client->id) {
            break;
        }

        char req[4];
        MPI_Recv(req, 4, MPI_CHAR, MPI_ANY_SOURCE, 4, MPI_COMM_WORLD, &local_status);

        int id = local_status.MPI_SOURCE;
        char req_file[MAX_FILENAME];
        MPI_Recv(req_file, MAX_FILENAME, MPI_CHAR, id, 4, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        string str_file = string(req_file);
        int cnt_seg;
        MPI_Recv(&cnt_seg, 1, MPI_INT, id, 4, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        char resp[3];

        if ((client->wanted_files_seg.find(str_file) != client->wanted_files_seg.end()
            && (int)(client->wanted_files_seg.at(str_file).size()) <= cnt_seg)
            || client->owned_files_seg.find(str_file) == client->owned_files_seg.end()) {
            resp[0] = 'N';
            resp[1] = 'O';
        } else
         if (client->nr_clients >= 2) {
            resp[0] = 'N';
            resp[1] = 'O';
        } else {
            pthread_mutex_lock(&client->mutex);
            client->nr_clients++;
            pthread_mutex_unlock(&client->mutex);
            resp[0] = 'O';
            resp[1] = 'K';
        }

        resp[2] = '\0';
        MPI_Send(resp, 4, MPI_CHAR, id, 9, MPI_COMM_WORLD);
    }
    return NULL;
}

void get_data(tracker_struct &tracker, int numtasks) {

    for (int id = 1; id < numtasks; id++) {
        int nr_files = 0;
        MPI_Recv(&nr_files, 1, MPI_INT, id, 1, MPI_COMM_WORLD, &status);

        for (int i = 0; i < nr_files; i++) {
            char name[10];
            name[0] = '\0';
            MPI_Recv(name, MAX_FILENAME, MPI_CHAR, id, 1, MPI_COMM_WORLD, &status);
            string str_name = string(name);

            if (tracker.owners.find(str_name) == tracker.owners.end()) {
                tracker.owners.emplace(str_name, vector<int>());
            }

            tracker.owners.at(str_name).push_back(id);

            if (tracker.files.find(str_name) == tracker.files.end()) {
                tracker.files.emplace(str_name, vector<string>());
            }

            int nr_seg = 0;
            MPI_Recv(&nr_seg, 1, MPI_INT, id, 1, MPI_COMM_WORLD, &status);

            for (int j = 0; j < nr_seg; j++) {
                char seg_name[32];
                seg_name[0] = '\0';
                MPI_Recv(seg_name, HASH_SIZE, MPI_CHAR, id, 1, MPI_COMM_WORLD, &status);
                seg_name[32] = '\0';
                string str_seg = string(seg_name);
                if ((int)(tracker.files.at(str_name).size()) != nr_seg) {
                    tracker.files.at(str_name).push_back(str_seg);
                }
            }
        }
    }
}

void send_swarm(tracker_struct &tracker, int client_id) {
    char file[MAX_FILENAME];
    MPI_Recv(file, MAX_FILENAME, MPI_CHAR, client_id, 2, MPI_COMM_WORLD, &status);
    string str_name = string(file);
    vector<int> ids = tracker.owners.at(str_name);
    int nr_ids = ids.size();
    MPI_Send(&nr_ids, 1, MPI_INT, client_id, 2, MPI_COMM_WORLD);

    for (int i = 0; i < nr_ids; i++) {
        MPI_Send(&ids[i], 1, MPI_INT, client_id, 2, MPI_COMM_WORLD);
    }

    vector<string> segm = tracker.files.at(str_name);
    int nr_seg = segm.size();
    MPI_Send(&nr_seg, 1, MPI_INT, client_id, 2, MPI_COMM_WORLD);

    for (int i = 0; i < nr_seg; i++) {
        char seg_name[HASH_SIZE];
        seg_name[0] = '\0';
        strcpy(seg_name, (segm[i]).c_str());
        seg_name[(segm[i]).size()] = '\0';
        MPI_Send(seg_name, HASH_SIZE, MPI_CHAR, client_id, 2, MPI_COMM_WORLD);
    }
}

void update_client(tracker_struct &tracker, int client_id) {
    MPI_Status local_status;
    char file[MAX_FILENAME];
    MPI_Recv(file, MAX_FILENAME, MPI_CHAR, client_id, 3, MPI_COMM_WORLD, &local_status);
    string str_name = string(file);
    int ok = 0;
    for (long unsigned int j = 0; j < tracker.owners.at(str_name).size(); j++) {
        if (tracker.owners.at(str_name)[j] == client_id) {
            ok = 1;
        }
    }

    if (ok) {
        tracker.owners.at(str_name).push_back(client_id);
    }

    vector<int> ids = tracker.owners.at(str_name);
    int nr_ids = ids.size();
    MPI_Send(&nr_ids, 1, MPI_INT, client_id, 3, MPI_COMM_WORLD);

    for (int i = 0; i < nr_ids; i++) {
        MPI_Send(&ids[i], 1, MPI_INT, client_id, 3, MPI_COMM_WORLD);
    }
}

void close_clients(int numtasks) {
    char done[5];
    strcpy(done, "DONE");
    for (int i = 1; i < numtasks; i++) {
        MPI_Send(done, 5, MPI_CHAR, i, 5, MPI_COMM_WORLD);
    }
}

void tracker(int numtasks, int rank) {
    tracker_struct tracker;
    int clients = 1;

    get_data(tracker, numtasks);

    char conf[2];
    strncpy(conf, "OK", 2);
    conf[2] = '\0';
    for (int i = 1; i < numtasks; i++) {
        MPI_Send(conf, 2, MPI_CHAR , i, 3, MPI_COMM_WORLD);
    }

    while (clients < numtasks) {
        MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
        if (status.MPI_TAG == 2) {
            send_swarm(tracker, status.MPI_SOURCE);
        } else if (status.MPI_TAG == 3) {
            update_client(tracker, status.MPI_SOURCE);
        } else if (status.MPI_TAG == 0) {
            char file_done[7];
            MPI_Recv(file_done, 7, MPI_CHAR, status.MPI_SOURCE, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        } else if (status.MPI_TAG == 1) {
            char all_done[7];
            MPI_Recv(all_done, 7, MPI_CHAR, status.MPI_SOURCE, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            clients++;
        }
    }
    close_clients(numtasks);
}

void read_file(int rank, client_struct &client)
{
    int nr_owned = 0, nr_wanted = 0, nr_segments = 0;
    string nr = to_string(rank), nr_file, filename, nr_seg, segment;
    string in_file = "in" + nr + ".txt";
    ifstream f(in_file);
    getline(f, nr_file);
    nr_owned = stoi(nr_file);

    client.id = rank;

    for (int i = 0; i < nr_owned; i++) {
        f >> filename >> nr_segments;

        client.owned_files.push_back(filename);
        client.owned_files_seg.emplace(filename, vector<string>());
        getline(f, segment);

        for (int j = 0; j < nr_segments; j++) {
            segment = "";
            getline(f, segment);
            client.owned_files_seg.at(filename).push_back(segment);
        }
    }

    nr_file = "";
    getline(f, nr_file);
    nr_wanted = stoi(nr_file);

    for (int i = 0; i < nr_wanted; i++) {
        getline(f, filename);
        client.wanted_files.push_back(filename);
        client.wanted_files_seg.emplace(filename, vector<string>());
    }

    f.close();
}

void inform_tracker(client_struct &client) {
    int nr_files = client.owned_files.size();
    MPI_Send(&nr_files, 1, MPI_INT, 0, 1, MPI_COMM_WORLD);

    for (int i = 0; i < nr_files; i++) {
        int name_size = client.owned_files[i].length();
        char name[MAX_FILENAME];
        name[name_size] = '\0';
        strcpy(name, (client.owned_files[i]).c_str());
        MPI_Send(name, MAX_FILENAME, MPI_CHAR, 0, 1, MPI_COMM_WORLD);

        int nr_seg = (client.owned_files_seg.at(client.owned_files[i])).size();
        MPI_Send(&nr_seg, 1, MPI_INT, 0, 1, MPI_COMM_WORLD);

        for (auto itr = client.owned_files_seg.at(client.owned_files[i]).begin(); itr != client.owned_files_seg.at(client.owned_files[i]).end(); itr++) {
            char seg_name[HASH_SIZE];
            seg_name[0] = '\0';
            strcpy(seg_name, (*itr).c_str());
            seg_name[(*itr).size()] = '\0';
            MPI_Send(seg_name, HASH_SIZE, MPI_CHAR, 0, 1, MPI_COMM_WORLD);
        }
    }
}

void peer(int numtasks, int rank) {
    pthread_t download_thread;
    pthread_t upload_thread;
    pthread_t traffic_thread;

    void *status;
    int r;
    MPI_Status s;

    client_struct client;
    pthread_mutex_init(&(client.mutex), NULL);

    read_file(rank, client);
    inform_tracker(client);

    char conf[2];
    MPI_Recv(conf, 2, MPI_CHAR, 0, 3, MPI_COMM_WORLD, &s);

    r = pthread_create(&download_thread, NULL, download_thread_func, &client);
    if (r) {
        printf("Eroare la crearea thread-ului de download\n");
        exit(-1);
    }

    r = pthread_create(&upload_thread, NULL, upload_thread_func, &client);
    if (r) {
        printf("Eroare la crearea thread-ului de upload\n");
        exit(-1);
    }

    r = pthread_create(&traffic_thread, NULL, traffic_thread_func, &client);
    if (r) {
        printf("Eroare la crearea thread-ului de upload\n");
        exit(-1);
    }

    r = pthread_join(download_thread, &status);
    if (r) {
        printf("Eroare la asteptarea thread-ului de download\n");
        exit(-1);
    }

    r = pthread_join(upload_thread, &status);
    if (r) {
        printf("Eroare la asteptarea thread-ului de upload\n");
        exit(-1);
    }

    r = pthread_join(traffic_thread, &status);
    if (r) {
        printf("Eroare la asteptarea thread-ului de upload\n");
        exit(-1);
    }
    pthread_mutex_destroy(&(client.mutex));
}
 
int main (int argc, char *argv[]) {
    int numtasks, rank;
 
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    if (provided < MPI_THREAD_MULTIPLE) {
        fprintf(stderr, "MPI nu are suport pentru multi-threading\n");
        exit(-1);
    }
    MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (rank == TRACKER_RANK) {
        tracker(numtasks, rank);
    } else {
        peer(numtasks, rank);
    }

    MPI_Finalize();
}
