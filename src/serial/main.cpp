#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <netinet/in.h>
#include <unistd.h>
#include <sys/socket.h>
#include <unordered_map>
#include <sstream>
#include <iostream>
#include <vector>
#include <map>
using namespace std;
const int PORT = 8080;

map<string, string> datastore;

void handle_client(int client_socket)
{

    char buffer[1024] = {0};
    int valread;

    while ((valread = read(client_socket, buffer, sizeof(buffer))) > 0)
    {
        
        istringstream request(buffer);
        char delim = '\n';
        string tok;
        vector<string> tokens;
        while (getline(request, tok, delim))
        {
           
            tokens.push_back(tok);
        }

        for(auto i:tokens){
            cout<<i<<"\n";
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

                // cout << "Method: " << method << endl;
                // cout << "Key: " << key << endl;
                // cout << "Value: " << value << endl;

                datastore[key] = value;

                string response = "FIN\n";
                send(client_socket, response.c_str(), response.length(), 0);
            }
            if (request_line.find("READ") != -1)
            {   
                string method=tokens[i];
                request_line=tokens[i+1];
                i+=1;
                istringstream iss(request_line);
                string key;

                iss >> key;
                cout << "Method: " << method << endl;
                cout << "Key: " << key << endl;
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
                    string response = "NULL\n";
                    send(client_socket, response.c_str(), response.length(), 0);
                }


            }

            if(request_line.find("COUNT")!=-1){
                int count=0;
                count=datastore.size();
                
                string countstr=to_string(count);
                string response =countstr+"\n" ;

                send(client_socket,response.c_str(),response.length(),0);
            }


            if(request_line.find("DELETE")!=-1){
                string method=tokens[i];
                request_line=tokens[i+1];
                i+=1;

                istringstream iss(request_line);
                string key;

                iss>>key;

                // cout<<"Method: "<<method<<"\n";
                // cout<<"Key: "<<key<<"\n";

                int succ=datastore.erase(key);
                if(succ){
                    string response="FIN\n";
                    send(client_socket,response.c_str(),response.length(),0);
                }
                else{
                    string response="NULL\n" ;
                    send(client_socket,response.c_str(),response.length(),0);
                }

            }

            if(request_line.find("END")!=-1){
                string response="\n";
                send(client_socket,response.c_str(),response.length(),0);
                close(client_socket);
				        shutdown(client_socket, SHUT_RDWR);
                
            }


        }
        memset(buffer, 0, sizeof(buffer));
    }
}

int main(int argc,char *argv[])
{
    int portno;
    if (argc != 2) {
        fprintf(stderr, "usage: %s <port>\n", argv[0]);
        exit(1);
    }

 
    portno = atoi(argv[1]);

    int server_socket, client_socket;
    struct sockaddr_in address;

    server_socket = socket(AF_INET, SOCK_STREAM, 0);
    // {
    //     perror("Socket creation failed");
    //     return -1;
    // }

    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(portno);

    bind(server_socket, (struct sockaddr*)&address, sizeof(address));
    // if (listen(server_socket, 10) < 0)
    // {
    //     perror("Listen failed");
    //     return -1;
    // }

    cout << "Server listening on port " << portno << "..." << endl;

    while (true)
    {
        listen(server_socket, 5);
        if ((client_socket = accept(server_socket, nullptr, nullptr)) < 0)
        {
            perror("Accept failed");
            return -1;
        }

        handle_client(client_socket);
    }
    close(server_socket);
    shutdown(server_socket,SHUT_RDWR);

    return 0;
}
