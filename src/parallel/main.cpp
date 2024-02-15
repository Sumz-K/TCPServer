#include <iostream>
#include <sstream>
#include <vector>
#include <map>
#include <queue>
#include <pthread.h>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <cstring>

using namespace std;

#define MAX_THREADS 5

map<string, string> datastore;
queue<int> job_queue;
pthread_mutex_t mutex_datastore = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_job_queue = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_pool = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_count=PTHREAD_MUTEX_INITIALIZER;
int actual_count=0;


struct Thread{
    pthread_t id;
    int fd;
};

struct Helper{
    struct Thread *thread_arr;
    size_t size;
};
void *handle_client(void *arg)
{
    int client_socket = *((int *)arg);
    char buffer[1024];
    int valread;
    while ((valread = read(client_socket, buffer, sizeof(buffer))) > 0)
    {
    	printf("in loop\n");
        istringstream request(buffer);
        string tok;
        vector<string> tokens;
        while (getline(request, tok, '\n'))
        {
            tokens.push_back(tok);
        }
        for (int i = 0; i < tokens.size(); i++)
        {
            string request_line = tokens[i];

            if (request_line.find("WRITE") != -1)
            {
                string request_key, request_value;
                string method = tokens[i];
                request_key = tokens[i + 1];
                request_value = tokens[i + 2];
                i += 2;

                istringstream iss(request_key);
                string key, value;
                iss >> key;

                request_value.erase(request_value.begin(), request_value.begin() + 1);
                value = request_value;

                pthread_mutex_lock(&mutex_datastore);
                datastore[key] = value;
                pthread_mutex_unlock(&mutex_datastore);

                string response = "FIN\n";
                send(client_socket, response.c_str(), response.length(), 0);
            }
            else if (request_line.find("READ") != -1)
            {
                string method = tokens[i];
                request_line = tokens[i + 1];
                i += 1;
                istringstream iss(request_line);
                string key;
                iss >> key;

                string value;
                pthread_mutex_lock(&mutex_datastore);
                if (datastore.find(key) != datastore.end())
                {
                    value = datastore[key];
                    string response = value + "\n";
                    send(client_socket, response.c_str(), response.length(), 0);
                }
                else
                {
                    string response = "NULL\n";
                    send(client_socket, response.c_str(), response.length(), 0);
                }
                pthread_mutex_unlock(&mutex_datastore);
            }
            else if (request_line.find("COUNT") != -1)
            {
                pthread_mutex_lock(&mutex_datastore);
                int count = datastore.size();
                pthread_mutex_unlock(&mutex_datastore);
                string countstr = to_string(count);
                string response = countstr + "\n";
                send(client_socket, response.c_str(), response.length(), 0);
            }
            else if (request_line.find("DELETE") != -1)
            {
                string method = tokens[i];
                request_line = tokens[i + 1];
                i += 1;
                istringstream iss(request_line);
                string key;
                iss >> key;

                string response;
                pthread_mutex_lock(&mutex_datastore);
                int erased = datastore.erase(key);
                if (erased)
                {
                    response = "FIN\n";
                    send(client_socket, response.c_str(), response.length(), 0);
                }
                else
                {
                    response = "NULL\n";
                    send(client_socket, response.c_str(), response.length(), 0);
                }
                pthread_mutex_unlock(&mutex_datastore);
            }
            else if (request_line.find("END") != -1)
            {
              	pthread_mutex_lock(&mutex_count);
              	
                actual_count--;

                string response="\n";
                send(client_socket, response.c_str(), response.length(), 0);
                close(client_socket);
                pthread_mutex_unlock(&mutex_count);
                pthread_exit(NULL);
            }
            else
            {
                string response = "\nInvalid command\n";
                send(client_socket, response.c_str(), response.length(), 0);
            }
        }
        memset(buffer, 0, sizeof(buffer));
    }
    close(client_socket);
    pthread_exit(NULL);
}

void *thread_pool_helper(void *args)
{

    struct Helper *hlp = static_cast<Helper *>(args);

    struct Thread * thread_pool=hlp->thread_arr;
    while(true){
        pthread_mutex_lock(&mutex_job_queue);
        int is_q_empty=job_queue.empty();
        pthread_mutex_unlock(&mutex_job_queue);
        
        if(!is_q_empty){
        	for (int i = 0 ; i<MAX_THREADS;i++){
        		pthread_mutex_lock(&mutex_pool);
                int threadTerminationStatus = pthread_tryjoin_np(thread_pool[i].id, NULL);
        		pthread_mutex_unlock(&mutex_pool);
        		if (threadTerminationStatus == 0){
            		pthread_mutex_lock(&mutex_job_queue);
            		int fontele = job_queue.front();
            		job_queue.pop();
	            	pthread_mutex_unlock(&mutex_job_queue);
	            	pthread_mutex_lock(&mutex_pool);
	            	thread_pool[i].fd = fontele;
                    pthread_create(&thread_pool[i].id, NULL, &handle_client, &thread_pool[i].fd);
                    pthread_mutex_unlock(&mutex_pool);
	            	}
            }
        }
        usleep(10000);      
    }
}

int main(int argc, char **argv){
    int portno;
    if (argc != 2){
        fprintf(stderr, "usage: %s <port>\n", argv[0]);
        exit(1);
    }
    portno = atoi(argv[1]);
    printf("%d\t", portno);
    
    struct sockaddr_in server, client;
    int client_len, new_socket;
    client_len = sizeof(server);
    char buffer[1500];
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    memset((char *)&server, 0, sizeof(server));
    server.sin_family = AF_INET;
    server.sin_addr.s_addr = htonl(INADDR_ANY);
    server.sin_port = htons(portno);
    bind(sock, (struct sockaddr *)&server, sizeof(server));
    printf("starting TCP server\n");
    listen(sock, 100);
    pthread_t t;
        
    int thread_number = MAX_THREADS;
    struct Thread thread_pool[thread_number];
    struct Helper threadArgs;
    threadArgs.thread_arr = thread_pool;
    threadArgs.size = thread_number;
    pthread_create(&t,NULL, &thread_pool_helper,&threadArgs);
    int returnvalue = 0;
    while (1){
        new_socket = accept(sock, (struct sockaddr *)&client, (socklen_t *)&client_len);
        pthread_mutex_lock(&mutex_count);
        int actno = actual_count++;
        pthread_mutex_unlock(&mutex_count);
        if (new_socket >= 0 && actno<thread_number){
        	pthread_mutex_lock(&mutex_pool);
        	thread_pool[actno].fd = new_socket;
            pthread_create(&thread_pool[actno].id, NULL, &handle_client, &thread_pool[actno].fd);
            pthread_mutex_unlock(&mutex_pool);
        } 
        else
        {
	        pthread_mutex_lock(&mutex_job_queue);
            printf("pushing and printing the content's of queue\n");
            job_queue.push(new_socket);
            pthread_mutex_unlock(&mutex_job_queue);
        }
    }

    close(sock);
    shutdown(sock, SHUT_RDWR);
    return 0;
}
