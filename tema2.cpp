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
    int upload_done = 0;
};

struct tracker_struct {
    unordered_map<string, vector<string>> files;
    unordered_map<string, vector<int>> owners;
};

MPI_Status status;

void request_swarms(client_struct *client, unordered_map<string, vector<int>> &owners,
        unordered_map<string, vector<string>> &remained_seg) {
    int nr_files = client->wanted_files.size();
    MPI_Send(&nr_files, 1, MPI_INT, 0, 2, MPI_COMM_WORLD);
    int i;

    for (i = 0; i < client->wanted_files.size(); i++) {
        char file[MAX_FILENAME];
        strcpy(file, (client->wanted_files[i]).c_str());
        file[client->wanted_files[i].length()] = '\0';
        MPI_Send(file, MAX_FILENAME, MPI_CHAR, 0, 2, MPI_COMM_WORLD);
        int nr_ids = 0;
        MPI_Recv(&nr_ids, 1, MPI_INT, 0, 2, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        for (int j = 0; j < nr_ids; j++) {
            int id;
            MPI_Recv(&id, 1, MPI_INT, 0, 2, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            // auto elem = find(owners.at(client->wanted_files[i]).begin(),
            //     owners.at(client->wanted_files[i]).end(), id);
            // if (elem == owners.at(client->wanted_files[i]).end()) {
            owners.at(client->wanted_files[i]).push_back(id);
            // }
        }

        int nr_seg = 0;
        MPI_Recv(&nr_seg, 1, MPI_INT, 0, 2, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        for (int j = 0; j < nr_seg; j++) {
            char seg_name[HASH_SIZE];
            MPI_Recv(seg_name, HASH_SIZE, MPI_CHAR, 0, 2, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            seg_name[HASH_SIZE] = '\0';
            remained_seg.at(client->wanted_files[i]).push_back(string(seg_name));
        }

    }
}

void request_segments(unordered_map<string, vector<int>> &owners, string file,
    unordered_map<string, vector<string>> remained_seg, int &cnt_seg) {

    char filename[MAX_FILENAME];
    strncpy(filename, file.c_str(), file.length());
    filename[file.length()] = '\0';

    for (auto owner = owners.at(file).begin(); owner < owners.at(file).end(); owner++) {
        char req[4];
        strcpy(req, "REQ");
        req[4] ='\0';
        MPI_Send(req, 4, MPI_CHAR, *owner, 4, MPI_COMM_WORLD);
        char resp[3];
        int da;
        MPI_Recv(resp, 4, MPI_CHAR, *owner, 4, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        // MPI_Recv(&da, 1, MPI_INT, *owner, 4, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        
        if (strcmp(resp, "OK")) {
            //MPI_Send(&cnt_seg, 1, MPI_INT, *owner, 5, MPI_COMM_WORLD);
            int i;
            for (i = cnt_seg; i < remained_seg.at(file).size(); i++) {
                if (i > cnt_seg + 10) {
                    break;
                }

                MPI_Send(filename, MAX_FILENAME, MPI_CHAR, *owner, 5, MPI_COMM_WORLD);
                // char segment[32];
                // strncpy(segment, remained_seg.at(file)[i].c_str(), 32);
                // segment[32] = '\0';
                // MPI_Send(segment, HASH_SIZE, MPI_CHAR, *owner, 5, MPI_COMM_WORLD);
                MPI_Recv(resp, 4, MPI_CHAR, *owner, 2, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            }
            cnt_seg = i;
            break;
        }
    }
}

void *download_thread_func(void *arg)
{
    client_struct *client = (client_struct*) arg;
    unordered_map<string, vector<int>> owners;
    unordered_map<string, vector<string>> remained_seg;

    string file = "client" + client->id;
    ofstream fout(file);

    for (int i = 0; i < client->wanted_files.size(); i++) {
        owners.emplace(client->wanted_files[i], vector<int>());
        remained_seg.emplace(client->wanted_files[i], vector<string>());
    }
    
    request_swarms(client, owners, remained_seg);

    // for (int f = 0; f < client->wanted_files.size(); f++) {
    //     for (int i = 0; i <owners.at(client->wanted_files[f]).size(); i++) {
    //         cout << client->id << " " << owners.at(client->wanted_files[f])[i] << " " << endl;
    //     }
    // }

    for (int f = 0; f < client->wanted_files.size(); f++) {
        int cnt_seg = 0;
        while (cnt_seg < remained_seg.at(client->wanted_files[f]).size()) {
            request_segments(owners, client->wanted_files[f], remained_seg, cnt_seg);
            cout << client->id << " " << cnt_seg << endl;
        }
    }

    return NULL;
}

void *upload_thread_func(void *arg)
{
    client_struct *client = (client_struct*) arg;
    int cnt_seg;
    MPI_Status local_status;
    char filename[MAX_FILENAME];
    char resp[3];
    resp[0] = 'O';
    resp[1] = 'K';
    resp[2] = '\0';
    int c = 0;

    while (1) {
        MPI_Probe(MPI_ANY_SOURCE, 5, MPI_COMM_WORLD, &local_status);

        if (local_status.MPI_SOURCE != 0) {
            //MPI_Recv(&cnt_seg, 1, MPI_INT, MPI_ANY_SOURCE, 5, MPI_COMM_WORLD, &local_status);
            MPI_Recv(filename, MAX_FILENAME, MPI_CHAR, local_status.MPI_SOURCE, 5, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            MPI_Send(resp, 4, MPI_CHAR, local_status.MPI_SOURCE, 2, MPI_COMM_WORLD);
        }
        c++;
    }

    client->upload_done = 1;
    return NULL;
}

void *traffic_thread_func(void *arg)
{
    client_struct *client = (client_struct*) arg;
    MPI_Status local_status;
    // int cnt_seg;

    while (!client->upload_done) {
        char req[4];
        int da;
        MPI_Recv(req, 4, MPI_CHAR, MPI_ANY_SOURCE, 4, MPI_COMM_WORLD, &local_status);
        int id = local_status.MPI_SOURCE;
        // MPI_Recv(&cnt_seg, 1, MPI_INT, MPI_ANY_SOURCE, 4, MPI_COMM_WORLD, &local_status);
        char resp[3];
        if (client->nr_clients < 2) {
            client->nr_clients++;
            resp[0] = 'O';
            resp[1] = 'K';
            da = 1;
        } else {
            strcpy(resp, "NO");
            da = 0;
            resp[0] = 'N';
            resp[1] = 'O';
        }
        resp[2] = '\0';

        MPI_Send(resp, 4, MPI_CHAR, id, 4, MPI_COMM_WORLD);
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

            // if (tracker.owners.at(str_name)) {
                tracker.owners.at(str_name).push_back(id);
            // }

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
                tracker.files.at(str_name).push_back(str_seg);
            }
        }
    }
}

void send_swarm(tracker_struct &tracker, int client_id) {
    char file[MAX_FILENAME];
    MPI_Recv(file, MAX_FILENAME, MPI_CHAR, client_id, 2, MPI_COMM_WORLD, &status);
    string str_name = string(file);
    vector<int> ids = tracker.owners.at(str_name);
    // tracker.owners.at(str_name).push_back(status.MPI_SOURCE);
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

void tracker(int numtasks, int rank) {
    tracker_struct tracker;
    int clients_done = 1;

    get_data(tracker, numtasks);

    char conf[2];
    strncpy(conf, "OK", 2);
    conf[2] = '\0';
    for (int i = 1; i < numtasks; i++) {
        MPI_Send(conf, 2, MPI_CHAR , i, 3, MPI_COMM_WORLD);
    }

    while (clients_done < numtasks) {
        MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
        if (status.MPI_TAG == 2) {
            int nr_files;
            MPI_Recv(&nr_files, 1, MPI_INT, clients_done, 2, MPI_COMM_WORLD , MPI_STATUS_IGNORE);
            for (int i = 0; i < nr_files; i++) {
                send_swarm(tracker, clients_done);
            }
        }
        clients_done++;
    }
}

void read_file(int rank, client_struct &client)
{
    int nr_owned = 0, nr_wanted = 0, nr_segments = 0;
    string nr = to_string(rank), nr_file, filename, nr_seg, segment;
    string in_file = "../checker/tests/test2/in" + nr + ".txt";
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
    }

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

    // for (int i = 0; i < client.owned_files.size(); i++) {
    //     cout << client.owned_files[i] << " " << endl;

    //     auto &segments = client.owned_files_seg.at(client.owned_files[i]);
    //     for (auto itr = segments.begin(); itr != segments.end(); ++itr) {
    //         cout << *itr << " " << endl; 
    //     }
    // }

    // for (int i = 0; i < client.wanted_files.size(); i++) {
    //     cout << client.wanted_files[i] << " " << endl;
    // }

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
