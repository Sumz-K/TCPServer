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

#define MAX_THREADS 40


map<string, string> datastore;
queue<int> job_queue;

namespace ns{
    pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
}
int actual_count = 0;

void *handle_client(void *arg)
{
    int client_socket = *((int *)arg);
    char buffer[1024] = {0};
    int valread;
    while ((valread = read(client_socket, buffer, sizeof(buffer))) > 0)
    {
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
                string request_key,request_value;
                string method=tokens[i];    
                request_key = tokens[i+1];
                request_value=tokens[i+2];
                i+=2;
                //cout << "Request Line: " << request_line << endl;

                istringstream iss(request_key);
                string  key,value;
                
                iss >> key;

                
                request_value.erase(request_value.begin(), request_value.begin() + 1);
                value=request_value;

                pthread_mutex_lock(&ns::mutex);
                datastore[key] = value;
                pthread_mutex_unlock(&ns::mutex);

                
    
                
                string response = "FIN\n";
                send(client_socket, response.c_str(), response.length(), 0);
            }
            else if (request_line.find("READ") != -1)
            {
                string method=tokens[i];
                request_line=tokens[i+1];
                i+=1;
                istringstream iss(request_line);
                string key;

                iss >> key;
                // cout << "Method: " << method << endl;
                // cout << "Key: " << key << endl;
                string value;
                if (datastore.find(key) != datastore.end())
                    {
                        value = datastore[key];
                        //cout << value << "\n";

                        string response = value+"\n";
                        send(client_socket, response.c_str(), response.length(), 0);
                    }
                else
                    {
                        //cout << "NULL\n";
                        string response = "NULL\n";
                        send(client_socket, response.c_str(), response.length(), 0);
                    }
            }
            else if (request_line.find("COUNT") != -1)
            {
                pthread_mutex_lock(&ns::mutex);
                int count = datastore.size();
                pthread_mutex_unlock(&ns::mutex);
                string countstr=to_string(count);
                string response=countstr+"\n" ;
                send(client_socket,response.c_str(),response.length(),0);
            }
            else if (request_line.find("DELETE") != -1)
            {
                string method=tokens[i];
                request_line=tokens[i+1];
                i+=1;

                istringstream iss(request_line);
                string key;

                iss>>key;

                

      
                string response;
                pthread_mutex_lock(&ns::mutex);
                int erased=datastore.erase(key);
                if(erased){
                    response="FIN\n" ;
                    send(client_socket,response.c_str(),response.length(),0);
                }
                else{
                    response="NULL\n" ;
                    send(client_socket,response.c_str(),response.length(),0);
                }
                pthread_mutex_unlock(&ns::mutex);
                
                
            }
            else if (request_line.find("END") != -1)
            {
               
                pthread_mutex_lock(&ns::mutex);
                actual_count--;
                
                string response="\n";
                send(client_socket, response.c_str(), response.length(), 0);
                close(client_socket);
                pthread_mutex_unlock(&ns::mutex);
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
    pthread_mutex_lock(&ns::mutex);
    actual_count--;
    pthread_mutex_unlock(&ns::mutex);
    pthread_exit(NULL);
}

void *thread_pool_helper(void *)
{
    while (true)
    {
        if (!job_queue.empty())
        {
            pthread_mutex_lock(&ns::mutex);
            if (actual_count < MAX_THREADS)
            {
                int client = job_queue.front();
                job_queue.pop();
                pthread_t thread;
                pthread_create(&thread, NULL, &handle_client, &client);
                actual_count++;
            }
            pthread_mutex_unlock(&ns::mutex);
        }
    }
}

int main(int argc, char*argv[])
{
    int portno;
    if (argc != 2) {
        fprintf(stderr, "usage: %s <port>\n", argv[0]);
        exit(1);
    }

 
    portno = atoi(argv[1]);

    int server_socket, client_socket;
    struct sockaddr_in address;

    if ((server_socket = socket(AF_INET, SOCK_STREAM, 0)) == 0)
    {
        perror("Socket creation failed");
        return -1;
    }

    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(portno);
    int reuse=1;
    setsockopt(server_socket,SOL_SOCKET,SO_REUSEADDR,&reuse,sizeof(reuse));

    bind(server_socket, (struct sockaddr *)&address, sizeof(address));
    
    if (listen(server_socket, 100) < 0)
    {
        perror("Listen failed");
        return -1;
    }

    cout << "Server listening on port " << portno << "..." << endl;

    pthread_t thread_pool_thread;
    pthread_create(&thread_pool_thread, NULL, &thread_pool_helper, NULL);

    while (true)
    {
        client_socket = accept(server_socket, NULL, NULL);
        if (client_socket < 0)
        {
            perror("Accept failed");
            return -1;
        }

        pthread_mutex_lock(&ns::mutex);
        if (actual_count < MAX_THREADS)
        {
            pthread_t thread;
            pthread_create(&thread, NULL, &handle_client, &client_socket);
            actual_count++;
        }
        else
        {
            job_queue.push(client_socket);
        }
        pthread_mutex_unlock(&ns::mutex);
    }

    close(server_socket);
    return 0;
}
